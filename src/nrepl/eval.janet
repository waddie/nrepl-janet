###
### eval.janet
###
### Code evaluation with per-request output capture and nREPL response framing.
###
### Each `eval`/`load-file` request runs `run-context` over the request's `code`
### so multi-form payloads evaluate form-by-form with correct source maps and
### line numbers. stdout/stderr produced during evaluation are captured into a
### buffer (bound inside the evaluator, the way spork/netrepl does it) and
### streamed back as `out` messages; each top-level form's value is sent as a
### `value` message; the request ends with a `status ["done"]`.
###
### The user code runs inside a child fiber recorded as the session's
### :current-eval, so an `interrupt` op can `ev/cancel` it. The cancel is sent
### with a unique per-eval marker; when that marker surfaces through
### `on-status` we know the eval was interrupted (rather than failing on a
### genuine runtime error) and reply `status ["interrupted" "done"]`.
###
### Cancellation is cooperative: `ev/cancel` only takes effect when the eval
### yields to the event loop (I/O, `ev/sleep`, channel ops). A pure CPU-bound
### loop that never yields cannot be interrupted in single-threaded Janet --
### the same caveat as canceling any fiber.

(defn- format-value
  "Render an evaluation result as a read-safe string for the `value` field."
  [v]
  (string/format "%q" v))

(defn evaluate
  "Evaluate `code` in `session`, streaming nREPL responses through `send` (a
  1-arg function taking a response dict). `id` is the originating request id.
  Optional `source` sets the source path used in error locations and source
  maps. Optional `line`/`column` are the position in `source` where `code`
  starts (the standard nREPL `eval` params) so error locations and source maps
  reflect the form's real position in the file rather than starting at line 1.
  Runs synchronously in the caller (the session worker) and returns when
  evaluation is complete, interrupted, or has errored."
  [session code send id &opt source line column]
  (default source "nrepl")
  (default line 1)
  (default column 1)
  (def env (in session :env))
  (def base {:id id :session (in session :id)})
  (def obuf @"")
  (def stdin-chan (in session :stdin))
  (def inbuf @"") # bytes delivered by `stdin` ops, not yet consumed
  (var eof false) # set once the client signals end-of-input
  (def marker @{}) # unique identity used to recognise our own interrupt cancel
  (var interrupted false)

  (defn flush-out []
    (when (pos? (length obuf))
      (send (merge base {:out (string obuf)}))
      (buffer/clear obuf)))

  (defn pull-input
    "Block until the client delivers more stdin, appending it to `inbuf` and
    returning true; return false at end-of-input. Emits a `need-input` status
    first (so the client knows to prompt) unless input is already buffered."
    []
    (if eof
      false
      (do
        (flush-out)
        (when (zero? (ev/count stdin-chan))
          (send (merge base {:status ["need-input"]})))
        (def data (ev/take stdin-chan))
        (if (or (nil? data) (empty? data))
          (do (set eof true) false)
          (do (buffer/push-string inbuf data) true)))))

  # Shadow `getline` so user code that reads a line drives the nREPL stdin flow
  # (need-input / stdin ops) rather than blocking on the server's own stdin --
  # the real getline reads a C FILE* with blocking fgetc, which would freeze the
  # single-threaded event loop and can't signal need-input.
  (defn nrepl-getline [&opt prompt buf _env]
    (default buf @"")
    (buffer/clear buf)
    (when (and prompt (not (empty? prompt)))
      (send (merge base {:out (string prompt)})))
    (var reading true)
    (while reading
      (if-let [nl (string/find "\n" inbuf)]
        (do
          (buffer/push-string buf (string/slice inbuf 0 (inc nl)))
          (def leftover (string (string/slice inbuf (inc nl))))
          (buffer/clear inbuf)
          (buffer/push-string inbuf leftover)
          (set reading false))
        (when (not (pull-input))
          (buffer/push-string buf inbuf) # flush any partial trailing line
          (buffer/clear inbuf)
          (set reading false))))
    buf)
  (put env 'getline
       @{:value nrepl-getline
         :doc (string "(getline &opt prompt buf env)\n\nRead a line of input "
                      "from the nREPL client: emits a need-input status and "
                      "waits for a stdin op. Returns the buffer, including the "
                      "trailing newline (or whatever precedes end-of-input).")})

  # The evaluator runs in run-context's inner eval fiber, whose environment IS
  # the session env. Bind :out/:err with `setdyn` (not `with-dyns`) so the
  # thunk keeps running in *this* fiber: `with-dyns` evaluates its body in a
  # fresh child fiber whose environment is a *child* of the session env, so any
  # top-level `use`/`import`/`setdyn` in user code would write to that throwaway
  # env and never persist across evals. (`def`/`var` survive regardless -- they
  # bind at compile time against the session env -- which is what made the bug
  # subtle: state mutations persisted, but module imports silently did not.)
  (def evaluator
    (fn nrepl-evaluator [thunk &]
      (setdyn :out obuf)
      (setdyn :err obuf)
      (def v (thunk))
      (flush-out)
      (send (merge base {:value (format-value v) :ns (in session :ns)}))
      v))

  (defn send-error [message trace]
    (flush-out)
    (when (and trace (pos? (length trace)))
      (send (merge base {:err (string trace)})))
    (send (merge base {:status ["eval-error"] :ex message :root-ex message})))

  (defn on-status [f res]
    (when (= :error (fiber/status f))
      # Stop evaluating further forms on the first error/interrupt (nREPL semantics).
      (put env :exit true)
      (if (= res marker)
        (set interrupted true)
        (let [trace @""]
          (with-dyns [:out trace :err trace] (debug/stacktrace f res ""))
          (send-error (if (string? res) res (string/format "%q" res)) trace)))))

  (defn on-compile-error [message errf where line col]
    (put env :exit true)
    # line/col can be nil; build the location with plain concatenation so a nil
    # never reaches string/format (which would throw -- and run-context swallows
    # errors raised inside this callback, hiding the failure entirely).
    (def loc (string (or where source) ":" (or line "?") ":" (or col "?")))
    (send-error (string message) (string loc ": compile error: " message "\n")))

  (defn on-parse-error [p where]
    (put env :exit true)
    (def message (parser/error p))
    (send-error (string message) (string (or where source) ": parse error: " message "\n")))

  # Feed the whole code string to run-context once, then signal EOF by leaving
  # the chunk buffer empty on the next call.
  (var sent false)
  (defn chunks [buf p]
    (when (not sent)
      (set sent true)
      # Offset the parser to the form's real position so source maps and error
      # locations report file lines, not lines relative to this snippet.
      (parser/where p line column)
      (buffer/push-string buf code))
    buf)

  (def super (ev/chan))
  (def runner
    (ev/go
      (fiber/new
        (fn run-eval []
          (run-context
            {:env env
             :source source
             :chunks chunks
             :evaluator evaluator
             :on-status on-status
             :on-compile-error on-compile-error
             :on-parse-error on-parse-error}))
        :tp)
      nil
      super))
  (put session :current-eval {:fiber runner :marker marker})
  (ev/take super) # wait for run-context to finish (normally, error, or interrupt)
  (put session :current-eval nil)
  (flush-out)
  (if interrupted
    (send (merge base {:status ["interrupted" "done"]}))
    (send (merge base {:status ["done"]}))))
