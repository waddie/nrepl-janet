(use spork/test)
(import ../src/nrepl/server :as server)
(import ../src/nrepl/client :as client)

(start-suite "server")

# Pick an ephemeral-ish port unlikely to clash with a real nREPL on 7888.
(def host "127.0.0.1")
(def port "17888")

(defn- find-status
  "First response in `msgs` whose status contains `token`."
  [msgs token]
  (find (fn [m]
          (when-let [st (get m :status)]
            (some (fn [s] (= token (string s))) st)))
        msgs))

(defn- value-of
  "The `:value` string from the first response that carries one."
  [msgs]
  (when-let [m (find (fn [m] (get m :value)) msgs)]
    (string (get m :value))))

(defn- out-of
  "Concatenated `:out` strings across `msgs`."
  [msgs]
  (string/join (map (fn [m] (string (get m :out ""))) msgs)))

(with [listener (server/server host port)]
  (def c (client/connect host port))

  # --- describe advertises ops + versions ------------------------------------
  (let [msgs (client/request c {:op "describe" :id "d1"})
        m (find-status msgs "done")]
    (assert m "describe completes with done")
    (assert (get m :ops) "describe advertises ops")
    (assert (get-in m [:ops :eval]) "describe advertises eval op")
    (assert (get-in m [:versions :janet]) "describe reports janet version"))

  # --- clone returns a new session id -----------------------------------------
  (def session
    (let [msgs (client/request c {:op "clone" :id "c1"})
          m (find-status msgs "done")]
      (assert (get m :new-session) "clone returns new-session")
      (string (get m :new-session))))

  # --- eval of (+ 1 2) yields value "3" then done -----------------------------
  (let [msgs (client/request c {:op "eval" :id "e1" :session session :code "(+ 1 2)"})]
    (assert (= "3" (value-of msgs)) "eval (+ 1 2) => value 3")
    (assert (find-status msgs "done") "eval completes with done"))

  # --- (print ...) produces an out message ------------------------------------
  (let [msgs (client/request c {:op "eval" :id "e2" :session session :code "(print \"hi\")"})]
    (assert (string/has-prefix? "hi" (out-of msgs)) "print produces out 'hi'")
    (assert (find-status msgs "done") "print eval completes"))

  # --- a deliberate error yields eval-error -----------------------------------
  (let [msgs (client/request c {:op "eval" :id "e3" :session session :code "(error \"boom\")"})]
    (assert (find-status msgs "eval-error") "error yields eval-error status")
    (assert (find-status msgs "done") "errored eval still completes with done"))

  # --- error locations honour the client-supplied line/column -----------------
  # nREPL clients send the position where `code` starts; without it the parser
  # would report every form on line 1 regardless of its real file position.
  (let [msgs (client/request c {:op "eval" :id "ln1" :session session
                                :file "demo.janet" :line 10
                                :code "(error \"boom\")"})
        err (string/join (map (fn [m] (string (get m :err ""))) msgs))]
    (assert (find-status msgs "eval-error") "offset eval still errors")
    (assert (string/find "line 10" err)
            "error reports the client-supplied line, not line 1"))

  # The offset tracks subsequent lines too, not just the first form.
  (let [msgs (client/request c {:op "eval" :id "ln2" :session session
                                :file "demo.janet" :line 10
                                :code "\n\n(error \"boom\")"})
        err (string/join (map (fn [m] (string (get m :err ""))) msgs))]
    (assert (string/find "line 12" err)
            "two leading newlines from line 10 puts the error on line 12"))

  # --- def persists within a session ------------------------------------------
  (client/request c {:op "eval" :id "e4" :session session :code "(def x 99)"})
  (let [msgs (client/request c {:op "eval" :id "e5" :session session :code "x"})]
    (assert (= "99" (value-of msgs)) "def persists across requests in a session"))

  # --- use/import persist within a session (regression) -----------------------
  # `use`/`import` mutate the runtime env, unlike `def`/`var` which bind at
  # compile time. The evaluator must run user code in the session env itself,
  # not a child fiber env, or the imported bindings vanish on the next eval.
  (client/request c {:op "eval" :id "u1" :session session :code "(use spork/math)"})
  (let [msgs (client/request c {:op "eval" :id "u2" :session session :code "(type sum)"})]
    (assert (= ":function" (value-of msgs))
            "a binding brought in by `use` is visible on a later eval"))

  # --- setdyn persists within a session (regression) --------------------------
  (client/request c {:op "eval" :id "u3" :session session :code "(setdyn :my-dyn 7)"})
  (let [msgs (client/request c {:op "eval" :id "u4" :session session :code "(dyn :my-dyn)"})]
    (assert (= "7" (value-of msgs)) "a dynamic binding set with setdyn persists"))

  # --- a def is NOT visible in a different session ----------------------------
  (def session2
    (string (get (find-status (client/request c {:op "clone" :id "c2"}) "done") :new-session)))
  (let [msgs (client/request c {:op "eval" :id "e6" :session session2 :code "x"})]
    (assert (find-status msgs "eval-error") "def from session 1 not visible in session 2"))

  # --- multi-form code yields a value per form --------------------------------
  (let [msgs (client/request c {:op "eval" :id "e7" :session session :code "(+ 1 1) (+ 2 2)"})
        values (map (fn [m] (string (get m :value)))
                    (filter (fn [m] (get m :value)) msgs))]
    (assert (deep= @["2" "4"] values) "two forms produce two values in order"))

  # --- ls-sessions lists the live sessions ------------------------------------
  (let [msgs (client/request c {:op "ls-sessions" :id "l1"})
        m (find-status msgs "done")]
    (assert (>= (length (get m :sessions)) 2) "ls-sessions lists active sessions"))

  # --- unknown op -------------------------------------------------------------
  (let [msgs (client/request c {:op "no-such-op" :id "u1"})]
    (assert (find-status msgs "unknown-op") "unknown op reports unknown-op"))

  # --- interrupt of a (yielding) infinite loop --------------------------------
  (do
    (client/send-msg c {:op "eval" :id "i1" :session session
                        :code "(forever (ev/sleep 0.01))"})
    (ev/sleep 0.2)
    (client/send-msg c {:op "interrupt" :id "i2" :session session})
    # Drain both the interrupt ack (which itself carries `interrupted`) and the
    # eval's own interrupted/done response; key on the eval's id so nothing of
    # this request is left in the pipe.
    (var saw-interrupted false)
    (var seen 0)
    (while (< seen 8)
      (def m (client/recv-msg c))
      (if (nil? m) (break))
      (++ seen)
      (when (and (= "i1" (string (get m :id "")))
                 (find-status [m] "interrupted"))
        (set saw-interrupted true)
        (break)))
    (assert saw-interrupted "interrupt of infinite loop yields interrupted"))

  # --- stdin: getline drives need-input then consumes a stdin op --------------
  (do
    (client/send-msg c {:op "eval" :id "si1" :session session
                        :code "(string \"got:\" (getline))"})
    # The eval blocks reading input; first drain up to the need-input status.
    (var saw-need-input false)
    (var guard 0)
    (while (< guard 8)
      (def m (client/recv-msg c))
      (++ guard)
      (if (nil? m) (break))
      (when (and (get m :status)
                 (some (fn [s] (= "need-input" (string s))) (get m :status)))
        (set saw-need-input true)
        (break)))
    (assert saw-need-input "reading input emits a need-input status")
    # Supply a line, then collect the rest of this eval's responses.
    (client/send-msg c {:op "stdin" :id "si2" :session session :stdin "hello\n"})
    (def rest @[])
    (var done false)
    (while (not done)
      (def m (client/recv-msg c))
      (if (nil? m) (break))
      (array/push rest m)
      (when (and (= "si1" (string (get m :id "")))
                 (find-status [m] "done"))
        (set done true)))
    (assert (string/find "got:hello" (or (value-of rest) ""))
            "stdin op feeds the blocked getline"))

  # --- lookup returns doc/arglists for a known symbol -------------------------
  (let [msgs (client/request c {:op "lookup" :id "k1" :session session :sym "map"})
        m (find-status msgs "done")
        info (get m :info)]
    (assert info "lookup returns an info map")
    (assert (= "map" (string (get info :name))) "lookup reports the symbol name")
    (assert (= "" (string (get info :ns ""))) "module-less symbol has a blank namespace")
    (assert (get info :doc) "lookup reports a docstring for map"))

  # --- lookup keeps a module-qualified symbol whole, with a blank ns ----------
  (let [msgs (client/request c {:op "lookup" :id "k3" :session session :sym "math/abs"})
        m (find-status msgs "done")
        info (get m :info)]
    (assert info "lookup returns an info map for a qualified symbol")
    (assert (= "math/abs" (string (get info :name))) "lookup reports the full symbol as the name")
    (assert (= "" (string (get info :ns ""))) "qualified symbol still reports a blank namespace"))

  # --- lookup of an unknown symbol yields an empty info map -------------------
  (let [msgs (client/request c {:op "lookup" :id "k2" :session session :sym "no-such-binding-xyz"})
        m (find-status msgs "done")]
    (assert (empty? (get m :info {})) "lookup of unknown symbol returns empty info"))

  # --- completions filters bindings by prefix ---------------------------------
  (let [msgs (client/request c {:op "completions" :id "p1" :session session :prefix "map"})
        m (find-status msgs "done")
        cands (get m :completions)
        names (map (fn [x] (string (get x :candidate))) cands)]
    (assert (not (empty? cands)) "completions returns candidates")
    (assert (some (fn [n] (= "map" n)) names) "completions for 'map' includes map")
    (assert (all (fn [n] (string/has-prefix? "map" n)) names)
            "every completion candidate matches the prefix")
    (assert (all (fn [x] (= "" (string (get x :ns "")))) cands)
            "module-less completions report a blank namespace"))

  # --- completions keep the module-qualified candidate resolvable -------------
  # The candidate is the full `math/abs` (what the client inserts and then echoes
  # back as lookup's `:sym`); `:ns` is blank so an "insert namespaced" client
  # does not re-prefix it into `math/math/abs`.
  (let [msgs (client/request c {:op "completions" :id "p2" :session session :prefix "math/a"})
        m (find-status msgs "done")
        cands (get m :completions)
        abs (find (fn [x] (= "math/abs" (string (get x :candidate)))) cands)]
    (assert abs "completions for 'math/a' includes math/abs as a full candidate")
    (assert (= "" (string (get abs :ns ""))) "qualified completion reports a blank namespace")
    # the candidate must round-trip through lookup and resolve to its doc
    (let [lk (client/request c {:op "lookup" :id "p2b" :session session
                                :sym (string (get abs :candidate))})
          info (get (find-status lk "done") :info)]
      (assert (get info :doc) "the completion candidate resolves to a docstring via lookup")))

  # --- session state survives disconnect + reconnect --------------------------
  # Sessions are server-scoped: dropping the connection must not lose their env.
  # Uses its own connections so closing one doesn't disturb the shared `c`.
  (do
    (def c1 (client/connect host port))
    (def persist
      (string (get (find-status (client/request c1 {:op "clone" :id "pc1"}) "done")
                   :new-session)))
    (client/request c1 {:op "eval" :id "pe1" :session persist :code "(def survivor 41)"})
    (client/close c1) # drop the whole connection

    (def c2 (client/connect host port))
    (let [msgs (client/request c2 {:op "eval" :id "pe2" :session persist
                                   :code "(+ survivor 1)"})]
      (assert (= "42" (value-of msgs))
              "a def set before disconnect is visible after reconnect"))

    # ls-sessions on the fresh connection still lists the persisted session.
    (let [msgs (client/request c2 {:op "ls-sessions" :id "pl1"})
          ids (map string (get (find-status msgs "done") :sessions))]
      (assert (some (fn [s] (= persist s)) ids)
              "ls-sessions is server-wide and lists the reconnected session"))

    # An interrupt from the new connection cleans up a yielding eval, and the
    # session stays reusable afterwards.
    (client/send-msg c2 {:op "eval" :id "pi1" :session persist
                         :code "(forever (ev/sleep 0.01))"})
    (ev/sleep 0.1)
    (client/send-msg c2 {:op "interrupt" :id "pi2" :session persist})
    # Key on the eval's id: the interrupt ack also carries `interrupted` now.
    (var saw-interrupted false)
    (var seen 0)
    (while (< seen 8)
      (def m (client/recv-msg c2))
      (if (nil? m) (break))
      (++ seen)
      (when (and (= "pi1" (string (get m :id "")))
                 (find-status [m] "interrupted"))
        (set saw-interrupted true)
        (break)))
    (assert saw-interrupted "a reconnected client can interrupt the session's eval")
    (let [msgs (client/request c2 {:op "eval" :id "pe3" :session persist :code "survivor"})]
      (assert (= "41" (value-of msgs)) "session is reusable after a cross-connection interrupt"))

    # Explicit close still removes it; a later eval reports unknown-session.
    (client/request c2 {:op "close" :id "px1" :session persist})
    (let [msgs (client/request c2 {:op "eval" :id "pe4" :session persist :code "survivor"})]
      (assert (find-status msgs "unknown-session")
              "closed session is gone; eval reports unknown-session"))
    (client/close c2))

  # --- interrupt status vocabulary ---------------------------------------------
  (let [msgs (client/request c {:op "interrupt" :id "ic1" :session session})]
    (assert (find-status msgs "session-idle")
            "interrupt with nothing running reports session-idle"))

  (let [msgs (client/request c {:op "interrupt" :id "ic2" :session "no-such-session"})]
    (assert (find-status msgs "unknown-session")
            "interrupt of an unknown session reports unknown-session"))

  # An interrupt naming a different request than the one running is rejected
  # and the eval keeps going; a matching interrupt-id cancels it.
  (do
    (client/send-msg c {:op "eval" :id "ic3" :session session
                        :code "(forever (ev/sleep 0.01))"})
    (ev/sleep 0.2)
    (client/send-msg c {:op "interrupt" :id "ic4" :session session
                        :interrupt-id "not-ic3"})
    (def ack (client/recv-msg c))
    (assert (= "ic4" (string (get ack :id))) "mismatch ack answers the interrupt request")
    (assert (find-status [ack] "interrupt-id-mismatch")
            "a non-matching interrupt-id reports interrupt-id-mismatch")
    (client/send-msg c {:op "interrupt" :id "ic5" :session session
                        :interrupt-id "ic3"})
    (var eval-interrupted false)
    (var guard 0)
    (while (< guard 8)
      (def m (client/recv-msg c))
      (if (nil? m) (break))
      (++ guard)
      (when (and (= "ic3" (string (get m :id "")))
                 (find-status [m] "interrupted"))
        (set eval-interrupted true)
        (break)))
    (assert eval-interrupted "a matching interrupt-id interrupts the eval"))

  # --- close answers jobs still queued behind a running eval --------------------
  # A closed queue drops its buffered values, so close must drain queued jobs
  # and reply session-closed, or those requests never complete.
  (do
    (def qs
      (string (get (find-status (client/request c {:op "clone" :id "qc1"}) "done")
                   :new-session)))
    (client/send-msg c {:op "eval" :id "q1" :session qs
                        :code "(forever (ev/sleep 0.01))"})
    (ev/sleep 0.2) # let the worker pick q1 up so q2 queues behind it
    (client/send-msg c {:op "eval" :id "q2" :session qs :code ":never-runs"})
    (ev/sleep 0.1)
    (client/send-msg c {:op "close" :id "qx" :session qs})
    (var q2-closed false)
    (var q1-interrupted false)
    (var guard 0)
    (while (and (< guard 12) (not (and q2-closed q1-interrupted)))
      (def m (client/recv-msg c))
      (if (nil? m) (break))
      (++ guard)
      (when (and (= "q2" (string (get m :id ""))) (find-status [m] "session-closed"))
        (set q2-closed true))
      (when (and (= "q1" (string (get m :id ""))) (find-status [m] "interrupted"))
        (set q1-interrupted true)))
    (assert q2-closed "a queued job is answered with session-closed on close")
    (assert q1-interrupted "the running eval is interrupted by close"))

  # --- close tears the session down -------------------------------------------
  (let [msgs (client/request c {:op "close" :id "x1" :session session})]
    (assert (find-status msgs "done") "close completes with done"))

  (client/close c))

# --- opt-in idle reaper ------------------------------------------------------
# A separate short-lived server so the tiny idle timeout can't affect the suite
# above. Idle sessions are reaped; a session with a running eval is not.
(with [listener (server/server host "17889" {:idle-timeout 0.2})]
  (def c (client/connect host "17889"))

  (def idle
    (string (get (find-status (client/request c {:op "clone" :id "rc1"}) "done")
                 :new-session)))
  (ev/sleep 0.5) # exceed the idle timeout with no activity
  (let [msgs (client/request c {:op "eval" :id "re1" :session idle :code "1"})]
    (assert (find-status msgs "unknown-session") "an idle session is reaped"))

  # A session with a running (yielding) eval is not idle, so it survives past
  # the timeout. Check while the eval is still in flight: ls-sessions must still
  # list it even though it has had no op activity for longer than the timeout.
  (def busy
    (string (get (find-status (client/request c {:op "clone" :id "rc2"}) "done")
                 :new-session)))
  (client/send-msg c {:op "eval" :id "re2" :session busy
                      :code "(ev/sleep 0.6) :done"})
  (ev/sleep 0.4) # exceeds the 0.2 idle timeout, but the eval is still running
  (let [msgs (client/request c {:op "ls-sessions" :id "rl1"})
        ids (map string (get (find-status msgs "done") :sessions))]
    (assert (some (fn [s] (= busy s)) ids)
            "a session with a running eval is not reaped"))
  # Tidy up: interrupt the eval and drain its responses.
  (client/send-msg c {:op "interrupt" :id "re3" :session busy})
  (var guard 0)
  (while (< guard 8)
    (def m (client/recv-msg c))
    (if (nil? m) (break))
    (++ guard)
    (when (and (= "re2" (string (get m :id ""))) (find-status [m] "done")) (break)))

  (client/close c))

(end-suite)
