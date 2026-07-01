###
### server.janet
###
### Connection lifecycle: per-connection reader/writer fibers, op dispatch, and
### server-scoped sessions.
###
###   socket -> reader fiber -> decode bencode -> dispatch by :op/:session
###   socket <- writer fiber <- encode bencode <- out channel
###
### One reader fiber decodes complete messages and dispatches each (quick ops
### inline, eval/load-file onto the owning session's serial worker). One writer
### fiber serialises every outgoing response through a single channel, so
### bencode frames never interleave on the wire.
###
### Sessions live in a server-scoped registry (`sctx :sessions`) shared by every
### connection, so a client can disconnect and reconnect (same session id)
### without losing state. Session workers are fire-and-forget, not owned by the
### connection nursery; the per-connection nursery holds only that connection's
### reader and writer. On disconnect the reader just closes the outgoing channel
### so the writer drains -- running evals keep going and sessions persist.
### Sessions are removed only by an explicit `close` op, server shutdown, or the
### optional idle reaper (`:idle-timeout`, off by default).

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

(defn- make-server-ctx
  "Build the shared server context: the session registry, the idle timeout (nil
  = never reap), and a slot for the reaper fiber handle."
  [opts]
  @{:sessions @{}
    :idle-timeout (get opts :idle-timeout)
    :reaper nil})

(defn- start-reaper
  "If an idle timeout is configured, spawn a fiber that periodically reaps idle
  sessions and stash its handle in `sctx` for shutdown. No-op otherwise."
  [sctx]
  (when-let [timeout (in sctx :idle-timeout)]
    # Poll at half the timeout so a session is reaped within ~1.5x its idle
    # window; a small floor keeps a tiny timeout from spinning the loop.
    (def interval (max 0.05 (/ timeout 2)))
    (put sctx :reaper
         (ev/go
           (fn reaper []
             (forever
               # ev/cancel at shutdown surfaces here as an error; catch it to
               # exit the loop cleanly.
               (if (try (do (ev/sleep interval) false) ([_] true)) (break))
               (session/reap-idle-sessions (in sctx :sessions) timeout)))))))

(defn- shutdown
  "Server teardown: close every remaining session (so its worker exits) and stop
  the reaper. Runs when the listener closes."
  [sctx]
  (def sessions (in sctx :sessions))
  (each id (keys sessions)
    (when-let [s (in sessions id)] (session/close-session sessions s)))
  (when-let [r (in sctx :reaper)] (protect (ev/cancel r "server shutdown"))))

(defn- connection-handler
  [stream sctx]
  (def sessions (in sctx :sessions))
  (def out-chan (ev/chan 128))
  (def dec (bencode/decoder))
  (def nurse (nursery))

  # Tolerate sends after the connection is gone: a persistent session's eval may
  # still be running (or finishing) after the client that requested it drops, and
  # its output has nowhere to go once this channel is closed.
  (defn send [resp]
    (try (ev/give out-chan resp) ([_] nil)))

  (def ctx {:sessions sessions :send send})

  # Single writer: the only fiber that touches the socket for output.
  (spawn-nursery nurse
                 (while (def resp (ev/take out-chan))
                   (net/write stream (bencode/encode resp))))

  # Single reader: decode framed messages and dispatch. On EOF/close, close the
  # outgoing channel so the writer exits -- but leave sessions alone; they are
  # server-scoped and outlive this connection.
  (spawn-nursery nurse
                 (defer (ev/chan-close out-chan)
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
  (or use it inside `with`) to stop accepting. Defaults: 127.0.0.1:7888.

  `opts` may set `:idle-timeout` (seconds): sessions untouched for that long are
  reaped. Omitted or nil means sessions persist until an explicit `close` op or
  server shutdown."
  [&opt host port opts]
  (default host default-host)
  (default port default-port)
  (default opts {})
  (def sctx (make-server-ctx opts))
  (start-reaper sctx)
  (def listener (net/listen host port))
  (ev/go (fn accept []
           (defer (shutdown sctx)
             (net/accept-loop listener
                              (fn [stream] (connection-handler stream sctx))))))
  listener)

(defn run-server
  "Start an nREPL server and block until the listener is closed. Defaults:
  127.0.0.1:7888. See `server` for `opts`."
  [&opt host port opts]
  (default host default-host)
  (default port default-port)
  (default opts {})
  (def sctx (make-server-ctx opts))
  (start-reaper sctx)
  (with [listener (net/listen host port)]
    (defer (shutdown sctx)
      (net/accept-loop listener
                       (fn [stream] (connection-handler stream sctx))))))
