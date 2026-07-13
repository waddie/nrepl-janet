###
### client.janet
###
### A minimal bencode nREPL client, used mainly to drive the server from tests
### and for quick interactive poking. Not a full editor client.

(import ./bencode)

(defn connect
  "Connect to an nREPL server and return a client handle."
  [&opt host port]
  (default host "127.0.0.1")
  (default port "7888")
  @{:stream (net/connect host port)
    :dec (bencode/decoder)})

(defn send-msg
  "Encode and send a single request dict."
  [client msg]
  (net/write (in client :stream) (bencode/encode msg)))

(defn recv-msg
  "Read and decode the next response dict, blocking for more bytes as needed.
  Returns nil if the connection closes first."
  [client]
  (def dec (in client :dec))
  (var m (bencode/take-message dec))
  (while (nil? m)
    (def chunk (net/read (in client :stream) 4096))
    (if (nil? chunk) (break))
    (bencode/feed dec chunk)
    (set m (bencode/take-message dec)))
  m)

(defn- done?
  [msg]
  (when-let [st (get msg :status)]
    (some (fn [s] (= "done" (string s))) st)))

(defn recv-until-done
  "Collect responses for one request until a `done` status arrives. Returns the
  array of response dicts (including the one carrying `done`)."
  [client]
  (def msgs @[])
  (forever
    (def m (recv-msg client))
    (if (nil? m) (break))
    (array/push msgs m)
    (if (done? m) (break)))
  msgs)

(defn request
  "Send `msg` and collect responses until `done`. Returns the response array."
  [client msg]
  (send-msg client msg)
  (recv-until-done client))

(defn close
  [client]
  (protect (:close (in client :stream))))

###
### Multiplexing client
###
### The serial client above sends one request and drains responses to `done`
### before the next. That cannot support an `interrupt` arriving while an `eval`
### is still running: both requests' responses interleave on the one socket.
###
### This layer tags every request with a unique `:id` and runs a background
### reader fiber that routes each decoded response to a per-request channel keyed
### by that id (the server echoes `:id` on every response). Callers can therefore
### have an eval in flight on one fiber while another fiber sends an interrupt.
### It mirrors the demultiplexing the server already does internally.

(defn- next-id
  "Monotonic per-connection request id."
  [conn]
  (def n (+ 1 (get conn :seq 0)))
  (set (conn :seq) n)
  (string "nrepl-" n))

(defn- route-reader
  "Background fiber: decode responses and hand each to its request's channel.
  On EOF, mark the connection closed and close every pending channel so blocked
  collectors unblock."
  [conn]
  (ev/spawn
    (def stream (in conn :stream))
    (def dec (in conn :dec))
    (def pending (in conn :pending))
    (defer (do
             (set (conn :closed) true)
             (each id (keys pending)
               (protect (ev/chan-close (get pending id)))))
      (forever
        (def chunk (try (net/read stream 4096) ([_] nil)))
        (if (nil? chunk) (break)) # EOF / peer closed
        (bencode/feed dec chunk)
        (forever
          (def msg (bencode/take-message dec))
          (if (nil? msg) (break))
          (def id (get msg :id))
          # Advisory flag: a `need-input` status means the request is blocked
          # waiting for a `stdin` op (see `send-stdin`). Single slot suffices --
          # one eval runs per session at a time. Cleared by `send-stdin` and by
          # `await-result` when the flagged request completes.
          (when-let [st (and id (get msg :status))]
            (when (some (fn [s] (= "need-input" (string s))) st)
              (set (conn :need-input) id)))
          (def ch (and id (get pending id)))
          # Tolerate a race where the collector has already torn down `id`.
          (when ch (try (ev/give ch msg) ([_] nil))))))))

(defn connect-mux
  "Connect to an nREPL server and start a multiplexing client. Returns a `conn`
  whose background reader routes responses by request id. Tear down with
  `close-mux`."
  [&opt host port]
  (default host "127.0.0.1")
  (default port "7888")
  (def conn @{:stream (net/connect host (string port))
              :dec (bencode/decoder)
              :pending @{}
              :seq 0
              :closed false
              # Id of a request currently blocked on `need-input`, or nil.
              :need-input nil
              # Capacity-1 channel used as a write lock so two fibers (e.g. an
              # eval and a concurrent interrupt) can't interleave bytes mid-frame
              # on the shared socket. Seeded with one token = unlocked.
              :wlock (ev/chan 1)})
  (ev/give (in conn :wlock) :token)
  (route-reader conn)
  conn)

(defn send-async
  "Tag `msg` with a fresh `:id` (unless it already has one), register a channel
  for its responses, write it, and return the id. Pair with `await-result`.
  The socket write is serialised through the connection's write lock.
  Errors with \"connection closed\" if the reader has already seen EOF: with no
  reader, nothing would ever give to (or close) the registered channel, so a
  blocked `await-result` would hang forever."
  [conn msg]
  # Race-free without locking: neither this check-then-put nor the reader's
  # defer (set :closed, close all pending channels) has a yield point, so
  # either we error here, or our channel is in :pending when the defer runs
  # and gets closed -- the mid-request path `await-result` already handles.
  (when (in conn :closed)
    (error "connection closed"))
  (def id (or (get msg :id) (next-id conn)))
  (put (in conn :pending) id (ev/chan 64))
  (def wlock (in conn :wlock))
  (ev/take wlock)
  (defer (ev/give wlock :token)
    (try
      (net/write (in conn :stream) (bencode/encode (merge {} msg {:id id})))
      ([err]
        # The caller never learns `id`, so nobody will collect (and deregister)
        # the channel; drop it here rather than leak the :pending entry.
        (put (in conn :pending) id nil)
        (error err))))
  id)

(defn- status-done?
  [msg]
  (when-let [st (get msg :status)]
    (some (fn [s] (= "done" (string s))) st)))

(defn- merge-msg
  "Fold one response message into the accumulator. `out`/`err` concatenate,
  `value`s collect into an array, `status` tokens accumulate, and every other
  key passes through last-wins (so `new-session`, `info`, `completions`,
  `sessions`, `versions`, `ops`, `ns`, `ex`/`root-ex` all surface)."
  [acc msg]
  (eachp [k v] msg
    (case k
      :id nil
      :out (buffer/push-string (in acc :out) v)
      :err (buffer/push-string (in acc :err) v)
      :value (array/push (in acc :values) v)
      :status (each s v (array/push (in acc :status) (string s)))
      (set (acc k) v))))

(defn- finalize
  [acc]
  (set (acc :out) (string (in acc :out)))
  (set (acc :err) (string (in acc :err)))
  (when (pos? (length (in acc :values)))
    (set (acc :value) (last (in acc :values))))
  acc)

(defn await-result
  "Collect responses for request `id` until a `done` status, returning a merged
  result table. If the connection closes first, the result carries `:closed`."
  [conn id]
  (def ch (get-in conn [:pending id]))
  (unless ch (errorf "no pending request for id %s" id))
  (def acc @{:out @"" :err @"" :values @[] :status @[]})
  (forever
    (def msg (ev/take ch))
    (when (nil? msg) (set (acc :closed) true) (break)) # channel closed (EOF)
    (merge-msg acc msg)
    (when (status-done? msg) (break)))
  (put (in conn :pending) id nil)
  # A request that ends still flagged (interrupted, EOF'd, or errored while
  # awaiting input) must not leave `:need-input` pointing at a dead id.
  (when (= id (in conn :need-input))
    (set (conn :need-input) nil))
  (protect (ev/chan-close ch))
  (finalize acc))

(defn call
  "Send `msg` and block until its responses complete. Returns the merged result."
  [conn msg]
  (await-result conn (send-async conn msg)))

(defn clone-session
  "Open a new session, optionally inheriting `parent`. Returns the merged result
  (the new session id is under `:new-session`)."
  [conn &opt parent]
  (call conn (if parent {:op "clone" :session parent} {:op "clone"})))

(defn eval-code
  "Evaluate `code` in `session`. `opts` may carry `:file`/`:line`/`:column`."
  [conn session code &opt opts]
  (default opts {})
  (call conn (merge {:op "eval" :session session :code code} opts)))

(defn load-file-code
  "Send `contents` as a `load-file`, with optional `:file-path`/`:file-name`."
  [conn session contents &opt opts]
  (default opts {})
  (call conn (merge {:op "load-file" :session session :file contents} opts)))

(defn lookup
  "Look up doc/arglists/source for `sym` in `session`."
  [conn session sym]
  (call conn {:op "lookup" :session session :sym sym}))

(defn completions
  "Prefix completion for `prefix` in `session`."
  [conn session prefix]
  (call conn {:op "completions" :session session :prefix prefix}))

(defn describe
  "Ask the server to describe its supported ops and versions."
  [conn]
  (call conn {:op "describe"}))

(defn ls-sessions
  "List the server's active sessions."
  [conn]
  (call conn {:op "ls-sessions"}))

(defn session-exists?
  "Whether the server lists session `id`. Returns true or false from the
  server's `ls-sessions`, or nil when the server does not support the op
  (status carries \"unknown-op\"), leaving the caller to decide."
  [conn id]
  (def r (ls-sessions conn))
  (if (some (fn [s] (= "unknown-op" s)) (get r :status []))
    nil
    (truthy? (some (fn [s] (= (string id) (string s))) (get r :sessions [])))))

(defn interrupt
  "Interrupt the running eval `interrupt-id` in `session`. Safe to call from a
  different fiber while `eval-code` is still in flight."
  [conn session interrupt-id]
  (call conn {:op "interrupt" :session session :interrupt-id interrupt-id}))

(defn send-stdin
  "Deliver `input` to `session` via a `stdin` op. An empty `input` signals
  end-of-input. Like `interrupt`, safe to call from a different fiber while an
  `eval-code` blocked on `need-input` is still in flight; servers also buffer
  input sent ahead of demand, so this may precede the eval that reads it."
  [conn session input]
  # Clear before sending: once input is on the wire the blocked eval is no
  # longer awaiting it (it may re-flag if it asks again).
  (set (conn :need-input) nil)
  (call conn {:op "stdin" :session session :stdin input}))

(defn close-mux
  "Close the multiplexing connection. The reader fiber unblocks pending callers."
  [conn]
  (protect (:close (in conn :stream))))

(defn find-nrepl-port
  "Search `dir` and its ancestors for a `.nrepl-port` file (the convention nREPL
  tooling writes). Returns the port string, or nil if none is found."
  [&opt dir]
  (default dir (os/cwd))
  (var d (os/realpath dir))
  (var found nil)
  (forever
    (def path (string d "/.nrepl-port"))
    (when (= :file (os/stat path :mode))
      (set found (string/trim (slurp path)))
      (break))
    (def parent (os/realpath (string d "/..")))
    (if (= parent d) (break) (set d parent)))
  found)
