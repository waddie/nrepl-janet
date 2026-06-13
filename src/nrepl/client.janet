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
