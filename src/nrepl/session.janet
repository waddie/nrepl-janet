###
### session.janet
###
### Per-connection nREPL sessions.
###
### A session is an isolated evaluation environment with a serial worker: all
### `eval`/`load-file` jobs for a session run one at a time (nREPL's required
### per-session ordering guarantee), while different sessions and the connection
### reader run concurrently. Quick control ops (clone/close/describe/interrupt/
### ls-sessions) bypass the queue and are handled directly by the reader.
###
### Shape:
###   {:id <hex string>            opaque session id sent to the client
###    :env <make-env result>      persistent compilation/runtime environment
###    :in <ev/chan of jobs>       job queue drained serially by the worker;
###                                each job is {:id <request id> :send <fn>
###                                :run <thunk>} so `close-session` can answer
###                                jobs still queued when the session closes
###    :stdin <ev/chan of strings> input delivered by `stdin` ops, read by evals
###    :current-eval <handle|nil>  {:fiber f :marker m} of a running eval, for interrupt
###    :ns <string>                reported namespace ("user" by default)
###    :last-active <number>}      monotonic timestamp of last activity, for the
###                                optional idle reaper
###
### Sessions are server-scoped: they outlive the connection that created them, so
### a client can disconnect and reconnect (with the same session id) without
### losing state. Workers spawn into the *server* nursery, not a connection's,
### and disconnect no longer closes sessions -- only an explicit `close` op,
### server shutdown, or the optional idle reaper does.

(defn gen-id
  "Generate an opaque session id (32 hex chars from the OS CSPRNG)."
  []
  (string/join (map (fn [b] (string/format "%02x" b)) (os/cryptorand 16))))

(defn new-session
  "Create and register a new session in `sessions`. If `parent-env` is given,
  the new environment inherits its bindings (via the prototype chain) so the
  clone starts from the parent's state but new defs stay isolated."
  [sessions &opt parent-env]
  (def env (if parent-env (make-env parent-env) (make-env)))
  (put env :pretty-format "%.20Q")
  (def id (gen-id))
  (def session @{:id id :env env :in (ev/chan 64) :stdin (ev/chan 64)
                 :current-eval nil :ns "user"
                 :last-active (os/clock :monotonic)})
  (put sessions id session)
  session)

(defn touch
  "Record activity on `session` so the idle reaper leaves it alone."
  [session]
  (put session :last-active (os/clock :monotonic)))

(defn start-worker
  "Spawn the session's serial worker. It drains the job queue, running each
  queued thunk to completion before taking the next, and exits when the queue
  channel is closed (on `close-session`).

  The worker is fire-and-forget (`ev/spawn`, root supervisor) rather than a
  connection nursery fiber: sessions are server-scoped and must outlive the
  connection that created them. On exit the root scheduler reaps the fiber, so a
  closed session's captured env is released. Each job's thunk is guarded so a
  bug escaping an eval-job cannot crash the worker (which would strand the
  queue)."
  [session]
  (ev/spawn
    (forever
      (def job (ev/take (in session :in)))
      (if (nil? job) (break))
      (try ((in job :run))
        ([err f] (debug/stacktrace f err "session worker "))))))

(defn close-session
  "Cancel any running eval, answer any jobs still queued behind it, close the
  worker queue (so the worker exits) and remove the session from `sessions`."
  [sessions session]
  (when-let [ce (in session :current-eval)]
    (protect (ev/cancel (in ce :fiber) (in ce :marker))))
  # Closing a channel drops its buffered values, so drain queued jobs first
  # and give each a terminal reply -- an unanswered request would leave its
  # client waiting on a `done` that never comes.
  (def q (in session :in))
  (while (pos? (ev/count q))
    (when-let [job (ev/take q)]
      ((in job :send) {:id (in job :id)
                       :session (in session :id)
                       :status ["error" "session-closed" "done"]})))
  (ev/chan-close q)
  (ev/chan-close (in session :stdin)) # unblock any eval waiting on input (EOF)
  (put sessions (in session :id) nil))

(defn reap-idle-sessions
  "Close every session idle for longer than `timeout` seconds. A session with a
  running eval (`:current-eval` set) is never reaped, however old its
  `:last-active` -- a long computation is not idle."
  [sessions timeout]
  (def now (os/clock :monotonic))
  (each id (keys sessions)
    (def s (in sessions id))
    (when (and s
               (nil? (in s :current-eval))
               (>= (- now (in s :last-active)) timeout))
      (close-session sessions s))))
