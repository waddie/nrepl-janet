###
### ops.janet
###
### nREPL operation handlers and dispatch.
###
### Each handler takes the decoded request `msg` (a table with keyword keys and
### string/int/list values) and a connection context `ctx`:
###   {:sessions <table id->session>   per-connection session registry
###    :send <fn resp>                 enqueue a response dict for the writer
###    :nurse <nursery>}               connection nursery (for session workers)
###
### Quick control ops reply directly. `eval`/`load-file` are enqueued onto the
### target session's serial worker so they run one-at-a-time per session while
### the reader keeps reading (letting `interrupt` arrive mid-eval).

(import ./session :as session)
(import ./eval :as evl)

(def server-version
  "Version reported under `versions` in `describe`."
  "0.0.1")

(def supported-ops
  "Ops advertised by `describe`. Clients query this on connect, so partial
  coverage degrades gracefully instead of breaking the client."
  ["clone" "close" "describe" "eval" "load-file"
   "interrupt" "ls-sessions" "lookup" "completions" "stdin"])

(defn- env-entry
  "Resolve `sym` in `env`, walking the prototype chain (so core bindings and
  inherited session state are visible). Returns the binding's entry table."
  [env sym]
  (var e env)
  (var found nil)
  (while (and e (nil? found))
    (set found (get e sym))
    (set e (table/getproto e)))
  found)

(defn- binding-type
  "Classify a binding entry as a macro, function, or plain var for completion
  candidates."
  [entry]
  (def v (get entry :value))
  (cond
    (get entry :macro) "macro"
    (or (function? v) (cfunction? v)) "function"
    "var"))

(defn- arglists-of
  "Janet does not retain textual parameter names at runtime, but its docstrings
  conventionally lead with the signature line(s). Return that leading block as
  the `arglists` value when present."
  [entry]
  (def doc (get entry :doc))
  (when (and doc (string/has-prefix? "(" doc))
    (first (string/split "\n\n" doc))))

(defn- lookup-info
  "Build the `info` map for symbol `sym-str` in `session`, or nil if unbound."
  [session sym-str]
  (when-let [entry (env-entry (in session :env) (symbol sym-str))]
    (def info @{:name sym-str :ns (in session :ns)})
    (when-let [d (get entry :doc)] (put info :doc d))
    (when-let [a (arglists-of entry)] (put info :arglists a))
    (when-let [sm (get entry :source-map)]
      (put info :file (string (get sm 0)))
      (when (get sm 1) (put info :line (get sm 1)))
      (when (get sm 2) (put info :column (get sm 2))))
    info))

(defn- session-for
  [ctx msg]
  (get (in ctx :sessions) (get msg :session)))

(defn- unknown-session
  [ctx msg]
  ((in ctx :send) {:id (get msg :id)
                   :session (get msg :session)
                   :status ["error" "unknown-session" "done"]}))

(defn- op-clone
  "Create a new session, optionally inheriting the env of an existing one."
  [msg ctx]
  (def sessions (in ctx :sessions))
  (def parent (when-let [s (session-for ctx msg)] (in s :env)))
  (def s (session/new-session sessions parent))
  (session/start-worker s (in ctx :nurse))
  ((in ctx :send) {:id (get msg :id)
                   :new-session (in s :id)
                   :status ["done"]}))

(defn- op-close
  "Close a session: cancel any running eval, stop its worker, drop it."
  [msg ctx]
  (when-let [s (session-for ctx msg)]
    (session/close-session (in ctx :sessions) s))
  ((in ctx :send) {:id (get msg :id)
                   :session (get msg :session)
                   :status ["done"]}))

(defn- op-describe
  "Advertise supported ops and version metadata."
  [msg ctx]
  ((in ctx :send)
    {:id (get msg :id)
     :session (get msg :session)
     :ops (from-pairs (map (fn [o] [o {}]) supported-ops))
     :versions {:janet {:version-string janet/version}
                :nrepl {:version-string server-version}}
     :status ["done"]}))

(defn- op-ls-sessions
  [msg ctx]
  ((in ctx :send) {:id (get msg :id)
                   :session (get msg :session)
                   :sessions (keys (in ctx :sessions))
                   :status ["done"]}))

(defn- op-eval
  [msg ctx]
  (if-let [s (session-for ctx msg)]
    (ev/give (in s :in)
             (fn eval-job []
               (evl/evaluate s (string (get msg :code "")) (in ctx :send) (get msg :id)
                             (when (get msg :file) (string (get msg :file))))))
    (unknown-session ctx msg)))

(defn- op-load-file
  "Evaluate file contents (sent in `:file`), using `:file-name` as the source."
  [msg ctx]
  (if-let [s (session-for ctx msg)]
    (ev/give (in s :in)
             (fn load-file-job []
               (evl/evaluate s (string (get msg :file "")) (in ctx :send) (get msg :id)
                             (when (get msg :file-name) (string (get msg :file-name))))))
    (unknown-session ctx msg)))

(defn- op-interrupt
  "Cancel the session's currently running eval, if any."
  [msg ctx]
  (when-let [s (session-for ctx msg)
             ce (in s :current-eval)]
    (protect (ev/cancel (in ce :fiber) (in ce :marker))))
  ((in ctx :send) {:id (get msg :id)
                   :session (get msg :session)
                   :status ["done"]}))

(defn- op-stdin
  "Deliver client-supplied input to the session's running eval (via its `:stdin`
  channel, where a blocked `getline` is waiting). An empty `:stdin` payload
  signals end-of-input."
  [msg ctx]
  (if-let [s (session-for ctx msg)]
    (do
      (ev/give (in s :stdin) (string (get msg :stdin "")))
      ((in ctx :send) {:id (get msg :id)
                       :session (get msg :session)
                       :status ["done"]}))
    (unknown-session ctx msg)))

(defn- op-lookup
  "Return doc/arglists/source metadata for a symbol (`:sym`) in the session env."
  [msg ctx]
  (if-let [s (session-for ctx msg)]
    ((in ctx :send) {:id (get msg :id)
                     :session (get msg :session)
                     :info (or (lookup-info s (string (get msg :sym ""))) {})
                     :status ["done"]})
    (unknown-session ctx msg)))

(defn- op-completions
  "Return completion candidates for `:prefix` from the session env bindings."
  [msg ctx]
  (if-let [s (session-for ctx msg)]
    (let [env (in s :env)
          prefix (string (get msg :prefix ""))
          syms (filter (fn [sym] (string/has-prefix? prefix (string sym)))
                       (all-bindings env))
          candidates (map (fn [sym]
                            {:candidate (string sym)
                             :ns (in s :ns)
                             :type (binding-type (env-entry env sym))})
                          syms)]
      ((in ctx :send) {:id (get msg :id)
                       :session (get msg :session)
                       :completions candidates
                       :status ["done"]}))
    (unknown-session ctx msg)))

(def- handlers
  {"clone" op-clone
   "close" op-close
   "describe" op-describe
   "ls-sessions" op-ls-sessions
   "eval" op-eval
   "load-file" op-load-file
   "interrupt" op-interrupt
   "stdin" op-stdin
   "lookup" op-lookup
   "completions" op-completions})

(defn dispatch
  "Route a decoded request to its handler, or reply `unknown-op`."
  [msg ctx]
  (def op (when (get msg :op) (string (get msg :op))))
  (if-let [handler (get handlers op)]
    (handler msg ctx)
    ((in ctx :send) {:id (get msg :id)
                     :session (get msg :session)
                     :status ["error" "unknown-op" "done"]})))
