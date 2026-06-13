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
###    :in <ev/chan of thunks>     job queue drained serially by the worker
###    :current-eval <handle|nil>  {:fiber f :marker m} of a running eval, for interrupt
###    :ns <string>}               reported namespace ("user" by default)

(use spork/ev-utils)

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
  (def session @{:id id :env env :in (ev/chan 64) :current-eval nil :ns "user"})
  (put sessions id session)
  session)

(defn start-worker
  "Spawn the session's serial worker into `nurse`. It drains the job queue,
  running each queued thunk to completion before taking the next, and exits
  when the queue channel is closed."
  [session nurse]
  (spawn-nursery nurse
                 (forever
                   (def job (ev/take (in session :in)))
                   (if (nil? job) (break))
                   (job))))

(defn close-session
  "Cancel any running eval, close the worker queue (so the worker exits) and
  remove the session from `sessions`."
  [sessions session]
  (when-let [ce (in session :current-eval)]
    (protect (ev/cancel (in ce :fiber) (in ce :marker))))
  (ev/chan-close (in session :in))
  (put sessions (in session :id) nil))
