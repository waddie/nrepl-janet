###
### server.janet
###
### Connection lifecycle: per-connection reader/writer fibers, op dispatch, and
### structured shutdown via a nursery.
###
###   socket -> reader fiber -> decode bencode -> dispatch by :op/:session
###   socket <- writer fiber <- encode bencode <- out channel
###
### One reader fiber decodes complete messages and dispatches each (quick ops
### inline, eval/load-file onto the owning session's serial worker). One writer
### fiber serialises every outgoing response through a single channel, so
### bencode frames never interleave on the wire. Session workers are spawned
### into the same nursery; on disconnect the reader's cleanup cancels running
### evals and closes every queue, so all fibers drain and the nursery joins.

(use spork/ev-utils)
(import ./bencode)
(import ./session :as session)
(import ./ops :as ops)

(def default-host
  "Default interface to bind."
  "127.0.0.1")

(def default-port
  "Default nREPL port (the conventional default for nREPL clients)."
  "7888")

(defn- connection-handler
  [stream]
  (def sessions @{})
  (def out-chan (ev/chan 128))
  (def dec (bencode/decoder))
  (def nurse (nursery))

  # Tolerate sends after the connection is gone (shutdown races): the writer's
  # channel may already be closed while a worker finishes an interrupted eval.
  (defn send [resp]
    (try (ev/give out-chan resp) ([_] nil)))

  (def ctx {:sessions sessions :send send :nurse nurse})

  # Single writer: the only fiber that touches the socket for output.
  (spawn-nursery nurse
                 (while (def resp (ev/take out-chan))
                   (net/write stream (bencode/encode resp))))

  # Single reader: decode framed messages and dispatch. On EOF/close, cancel
  # running evals and close every queue so the workers and writer can exit.
  (spawn-nursery nurse
                 (defer (do
                          (each id (keys sessions)
                            (session/close-session sessions (get sessions id)))
                          (ev/chan-close out-chan))
                   (forever
                     (def chunk (net/read stream 4096))
                     (if (nil? chunk) (break)) # EOF / peer closed
                     (bencode/feed dec chunk)
                     (forever
                       (def msg (bencode/take-message dec))
                       (if (nil? msg) (break))
                       (ops/dispatch msg ctx)))))

  (join-nursery nurse)
  (protect (:close stream)))

(defn server
  "Start an nREPL server bound to `host`:`port` and return the listening
  stream. The accept loop runs on the event loop; close the returned stream
  (or use it inside `with`) to stop accepting. Defaults: 127.0.0.1:7888."
  [&opt host port]
  (default host default-host)
  (default port default-port)
  (def listener (net/listen host port))
  (ev/go (fn accept [] (net/accept-loop listener connection-handler)))
  listener)

(defn run-server
  "Start an nREPL server and block until the listener is closed. Defaults:
  127.0.0.1:7888."
  [&opt host port]
  (default host default-host)
  (default port default-port)
  (with [listener (net/listen host port)]
    (net/accept-loop listener connection-handler)))
