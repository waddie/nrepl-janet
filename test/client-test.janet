(use spork/test)
(import ../src/nrepl/server :as server)
(import ../src/nrepl/client :as client)

# Exercises the multiplexing client: id-routed responses, merged results, state
# persistence across calls, and (the load-bearing case) an interrupt sent while
# an eval is still in flight on the same connection.

(start-suite "client-mux")

(def host "127.0.0.1")
(def port "17899")

(defn- status-has?
  [res token]
  (some (fn [s] (= token s)) (get res :status)))

(with [listener (server/server host port)]
  (def conn (client/connect-mux host port))

  # --- describe via the merged-result API -------------------------------------
  (let [r (client/describe conn)]
    (assert (get-in r [:ops :eval]) "describe advertises eval op")
    (assert (get-in r [:versions :janet]) "describe reports janet version"))

  # --- clone yields a new session id ------------------------------------------
  (def session
    (let [r (client/clone-session conn)]
      (assert (get r :new-session) "clone returns new-session")
      (string (get r :new-session))))

  # --- a merged eval result collapses value/out/status ------------------------
  (let [r (client/eval-code conn session "(+ 1 2)")]
    (assert (= "3" (string (get r :value))) "eval (+ 1 2) merges to value 3")
    (assert (status-has? r "done") "eval completes with done"))

  (let [r (client/eval-code conn session "(print \"hi\")")]
    (assert (string/has-prefix? "hi" (get r :out)) "stdout captured in merged :out"))

  (let [r (client/eval-code conn session "(error \"boom\")")]
    (assert (status-has? r "eval-error") "error surfaces as eval-error status"))

  # --- state accrues across separate calls (the whole point of the daemon) ----
  (client/eval-code conn session "(def x 99)")
  (let [r (client/eval-code conn session "x")]
    (assert (= "99" (string (get r :value))) "def persists across mux calls"))

  # --- interrupt while an eval is in flight (response demultiplexing) ----------
  # Start a yielding infinite loop, collect its result on a separate fiber, then
  # from this fiber send an interrupt for the same session. If responses are
  # routed by id, the eval's collector sees `interrupted`/`done` and unblocks.
  (let [eval-id (client/send-async conn
                                   {:op "eval" :session session
                                    :code "(forever (ev/sleep 0.01))"})
        result-chan (ev/chan 1)]
    (ev/spawn (ev/give result-chan (client/await-result conn eval-id)))
    (ev/sleep 0.2)
    (client/interrupt conn session eval-id)
    (let [eres (ev/take result-chan)]
      (assert (status-has? eres "interrupted") "in-flight eval is interrupted")
      (assert (status-has? eres "done") "interrupted eval still completes")))

  # --- connection is healthy after the interrupt ------------------------------
  (let [r (client/eval-code conn session "(+ 40 2)")]
    (assert (= "42" (string (get r :value))) "connection usable after interrupt"))

  # --- session-exists? consults ls-sessions ------------------------------------
  (assert (= true (client/session-exists? conn session))
          "session-exists? finds a cloned session")
  (assert (= false (client/session-exists? conn "bogus"))
          "session-exists? rejects an unlisted id")

  # --- stdin answers an in-flight eval blocked on need-input -------------------
  # Same shape as the interrupt test: the eval's collector runs on another
  # fiber; this fiber watches for the need-input flag and delivers input.
  (let [eval-id (client/send-async conn
                                   {:op "eval" :session session
                                    :code "(string \"got \" (getline))"})
        result-chan (ev/chan 1)]
    (ev/spawn (ev/give result-chan (client/await-result conn eval-id)))
    (var guard 0)
    (while (and (nil? (get conn :need-input)) (< guard 100))
      (ev/sleep 0.01) (++ guard))
    (assert (= eval-id (get conn :need-input))
            "need-input flags the blocked eval's id on the connection")
    (let [r (client/send-stdin conn session "hi\n")]
      (assert (status-has? r "done") "stdin op acknowledged with done"))
    (assert (nil? (get conn :need-input)) "send-stdin clears the flag")
    (let [eres (ev/take result-chan)]
      (assert (status-has? eres "need-input") "eval status records need-input")
      (assert (= "\"got hi\\n\"" (string (get eres :value)))
              "eval resumes with the delivered line")))

  # --- input buffered ahead of demand is consumed without need-input -----------
  (client/send-stdin conn session "early\n")
  (let [r (client/eval-code conn session "(getline)")]
    (assert (= "@\"early\\n\"" (string (get r :value))) "pre-supplied input is read")
    (assert (not (status-has? r "need-input")) "no need-input when input waits"))

  # --- empty stdin signals end-of-input ----------------------------------------
  (let [eval-id (client/send-async conn
                                   {:op "eval" :session session
                                    :code "(getline)"})
        result-chan (ev/chan 1)]
    (ev/spawn (ev/give result-chan (client/await-result conn eval-id)))
    (var guard 0)
    (while (and (nil? (get conn :need-input)) (< guard 100))
      (ev/sleep 0.01) (++ guard))
    (client/send-stdin conn session "")
    (let [eres (ev/take result-chan)]
      (assert (status-has? eres "done") "EOF'd eval completes")
      (assert (= "@\"\"" (string (get eres :value))) "getline returns empty at EOF")))

  (client/close-mux conn))

# --- send-async on a closed connection errors instead of hanging --------------
# Regression: once the reader fiber has seen EOF nothing services :pending, so
# without the :closed guard a call would block in await-result forever. Uses a
# raw listener because closing a real server's listener does not close already
# accepted connection streams; here we hold the server-side stream and drop it.
(with [listener (net/listen host "17900")]
  (var server-side nil)
  (ev/spawn (set server-side (net/accept listener)))
  (def dead (client/connect-mux host "17900"))
  (var guard 0)
  (while (and (nil? server-side) (< guard 100)) (ev/sleep 0.01) (++ guard))
  (assert server-side "test listener accepted the connection")
  (:close server-side) # peer drops
  (set guard 0)
  (while (and (not (get dead :closed)) (< guard 100)) (ev/sleep 0.05) (++ guard))
  (assert (get dead :closed) "reader marks the connection closed on peer EOF")
  # Deadline so a regression fails the suite as "deadline expired" instead of
  # hanging it.
  (def [ok err] (protect (ev/with-deadline 2 (client/call dead {:op "describe"}))))
  (assert (not ok) "call on a closed connection errors")
  (assert (string/find "connection closed" (string err))
          "the error is connection closed, not a hang or a write failure")
  (client/close-mux dead))

(end-suite)
