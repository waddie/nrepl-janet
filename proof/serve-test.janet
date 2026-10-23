(use spork/test)
(import ../src/nrepl/client :as client)

# Exercises the multiplexing client against `proof serve`, which speaks
# (a little) Clojure and imitates other servers' quirks one scenario at a time.
# Run it through serve-test.sh, or by hand against a running `proof serve`:
#
#   janet proof/serve-test.janet 7888
#
# The assertions hold for every scenario: the client must show the same output
# and values however the replies are split, batched or padded, and must not hang
# when a server lacks an op or hangs up.

(def port (get (dyn :args) 1 "7888"))

(defn- status-has?
  [res token]
  (some (fn [s] (= token s)) (get res :status)))

(defn- await-need-input
  "Wait up to a second for the eval `id` to ask for input."
  [conn id]
  (var guard 0)
  (while (and (not= id (get conn :need-input)) (< guard 100))
    (ev/sleep 0.01) (++ guard))
  (= id (get conn :need-input)))

(start-suite "proof-serve")

(def conn (client/connect-mux "127.0.0.1" port))

(def ops
  (let [r (client/describe conn)]
    (assert (status-has? r "done") "describe completes")
    (get r :ops {})))

(def session (string (get (client/clone-session conn) :new-session)))
(assert (not (empty? session)) "clone returns new-session")

# Every request gets a deadline so a client that hangs fails the suite instead.
(defmacro- within [& body] ~(ev/with-deadline 10 ,;body))

(def first-eval (within (client/eval-code conn session "(+ 1 2)")))

(if (get first-eval :closed)
  # hang-up: the server dropped the connection instead of answering.
  (do
    (assert (empty? (get first-eval :values)) "no value from a hung-up eval")
    (def [ok err] (protect (within (client/describe conn))))
    (assert (not ok) "calls after a hang-up error")
    (assert (string/find "connection closed" (string err))
            "the error is connection closed"))
  (do
    (assert (= "3" (get first-eval :value)) "eval returns the value")
    (assert (status-has? first-eval "done") "eval completes with done")

    (let [r (within (client/eval-code conn session "(println \"hi\")"))]
      (assert (= "hi\n" (get r :out)) "output arrives whole and in order"))

    # Some servers drop stderr entirely; the rest must deliver all of it.
    (let [r (within (client/eval-code conn session
                                      "(binding [*out* *err*] (println \"e\"))"))]
      (assert (index-of (get r :err) ["e\n" ""]) "error output is whole or absent"))

    # Some servers send only the last value.
    (let [r (within (client/eval-code conn session "(println \"a\") 1 (+ 1 1)"))]
      (assert (= "a\n" (get r :out)) "output of a multi-form eval")
      (assert (= "2" (get r :value)) "the last form's value wins")
      (assert (some |(deep= (get r :values) $) [@["nil" "1" "2"] @["2"]])
              "values are all forms' or just the last"))

    (let [r (within (client/eval-code conn session
                                      "(throw (ex-info \"boom\" {}))"))]
      (assert (status-has? r "eval-error") "a throw surfaces as eval-error")
      (assert (status-has? r "done") "a throw still completes")
      (assert (get r :ex) "a throw carries ex"))

    (within (client/eval-code conn session "(def x 99)"))
    (let [r (within (client/eval-code conn session "x"))]
      (assert (= "99" (get r :value)) "def persists across calls"))

    (when (get ops :interrupt)
      (let [id (client/send-async conn {:op "eval" :session session
                                        :code "(Thread/sleep 10000)"})
            result (ev/chan 1)]
        (ev/spawn (ev/give result (client/await-result conn id)))
        (ev/sleep 0.2)
        (within (client/interrupt conn session id))
        (let [r (within (ev/take result))]
          (assert (status-has? r "interrupted") "in-flight eval is interrupted")
          (assert (status-has? r "done") "interrupted eval completes"))))

    (if (get ops :stdin)
      (let [id (client/send-async conn {:op "eval" :session session
                                        :code "(str \"got \" (read-line))"})
            result (ev/chan 1)]
        (ev/spawn (ev/give result (client/await-result conn id)))
        (assert (await-need-input conn id) "need-input flags the blocked eval")
        (within (client/send-stdin conn session "hi\n"))
        (let [r (within (ev/take result))]
          (assert (= "\"got hi\"" (get r :value)) "eval resumes with the input")))
      # Without stdin, reading input returns or throws right away.
      (let [r (within (client/eval-code conn session "(read-line)"))]
        (assert (status-has? r "done") "reading input without stdin completes")
        (assert (not (status-has? r "need-input")) "no need-input without stdin")))

    (let [r (within (client/completions conn session "printl"))]
      (assert (status-has? r "done") "completions completes")
      (assert (find |(= "println" ($ :candidate)) (get r :completions))
              "completions offers clojure.core names"))
    (within (client/eval-code conn session "(def proof-var 1)"))
    (let [r (within (client/completions conn session "proof-v"))]
      (assert (find |(= "proof-var" ($ :candidate)) (get r :completions))
              "completions offers what the session defined"))

    (let [r (within (client/lookup conn session "println"))
          info (get r :info)]
      (assert (status-has? r "done") "lookup completes")
      (assert (= "println" (get info :name)) "lookup gives the name")
      (assert (= "clojure.core" (get info :ns)) "lookup gives the ns"))
    # nREPL sends an empty list rather than a dict for a symbol it can't find.
    (let [r (within (client/lookup conn session "no-such-var"))]
      (assert (status-has? r "done") "lookup of an unknown symbol completes")
      (assert (not (status-has? r "error")) "lookup of an unknown symbol isn't an error")
      (assert (empty? (get r :info)) "lookup of an unknown symbol gives no info"))

    (let [r (within (client/load-file-code conn session "(def y 7) (+ y 1)"
                                           {:file-path "/tmp/proof.clj"
                                            :file-name "proof.clj"}))]
      (assert (status-has? r "done") "load-file completes")
      (assert (= "8" (get r :value)) "load-file gives the last form's value"))
    (let [r (within (client/eval-code conn session "y"))]
      (assert (= "7" (get r :value)) "what load-file defined persists"))
    (let [r (within (client/load-file-code conn session
                                           "(throw (ex-info \"boom\" {}))"))]
      (assert (status-has? r "eval-error") "a throw in load-file surfaces as eval-error")
      (assert (status-has? r "done") "a throw in load-file still completes")
      (assert (get r :ex) "a throw in load-file carries ex")
      (assert (nil? (get r :value)) "a throw in load-file gives no value"))

    # Servers that hide close in describe still support it.
    (let [r (within (client/close-session conn session))]
      (assert (status-has? r "done") "close completes"))))

(client/close-mux conn)

(end-suite)
