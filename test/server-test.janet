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

  # --- def persists within a session ------------------------------------------
  (client/request c {:op "eval" :id "e4" :session session :code "(def x 99)"})
  (let [msgs (client/request c {:op "eval" :id "e5" :session session :code "x"})]
    (assert (= "99" (value-of msgs)) "def persists across requests in a session"))

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
    # Drain both the interrupt ack and the eval's interrupted/done responses.
    (var saw-interrupted false)
    (var seen 0)
    (while (< seen 8)
      (def m (client/recv-msg c))
      (if (nil? m) (break))
      (++ seen)
      (when (and (get m :status)
                 (some (fn [s] (= "interrupted" (string s))) (get m :status)))
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
    (assert (get info :doc) "lookup reports a docstring for map"))

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
            "every completion candidate matches the prefix"))

  # --- close tears the session down -------------------------------------------
  (let [msgs (client/request c {:op "close" :id "x1" :session session})]
    (assert (find-status msgs "done") "close completes with done"))

  (client/close c))

(end-suite)
