###
### bencode.janet
###
### Bencode codec for the nREPL wire protocol.
###
### nREPL's default transport is raw, self-delimiting bencode written directly
### onto the socket -- there is no length framing. A single socket read may
### therefore return a partial value or several values at once, so decoding is
### incremental: bytes are fed into a holding buffer and complete top-level
### values are taken off one at a time, with any leftover bytes retained.
###
### Type mapping (bencode is dynamically typed; scalars are ints or byte strings):
###   integer    <-> i<n>e
###   byte string<-> <len>:<bytes>   (Janet strings/buffers are binary-safe)
###   list       <-> l...e
###   dict       <-> d...e           (keys MUST be emitted in sorted byte order)
###
### On decode, dict keys become keywords (`op` -> `:op`) for ergonomic dispatch;
### all other strings stay as Janet strings. The op layer coerces to int/keyword
### where a particular op needs it.
###

### ---------------------------------------------------------------------------
### Encoding
### ---------------------------------------------------------------------------

(defn- push-bytes
  "Encode a byte string: <len>:<bytes>."
  [buf bytes]
  (buffer/push-string buf (string (length bytes)) ":" bytes))

(defn- key->bytes
  "Coerce a dict key to its byte-string form for encoding and sorting."
  [k]
  (case (type k)
    :keyword (string k)
    :string k
    :symbol (string k)
    :buffer (string k)
    (errorf "bencode dict key must be a byte string, got %v" k)))

(var- encode-dict nil)

(defn- encode-value
  [buf x]
  (case (type x)
    :number
    (do
      (assert (= x (math/floor x)) (string/format "cannot bencode non-integer number %v" x))
      (buffer/push-string buf "i" (string (math/round x)) "e"))
    :string (push-bytes buf x)
    :buffer (push-bytes buf x)
    :keyword (push-bytes buf x)
    :symbol (push-bytes buf x)
    :array (do (buffer/push-byte buf (chr "l")) (each v x (encode-value buf v)) (buffer/push-byte buf (chr "e")))
    :tuple (do (buffer/push-byte buf (chr "l")) (each v x (encode-value buf v)) (buffer/push-byte buf (chr "e")))
    :table (encode-dict buf x)
    :struct (encode-dict buf x)
    (errorf "cannot bencode value of type %v: %v" (type x) x)))

(set encode-dict
     (fn encode-dict
       [buf d]
       # Collect non-nil pairs as [byte-key value], sort by byte key, then emit.
       # Sorting is a bencode hard requirement (and Janet's `<` on strings is a
       # byte-wise comparison, which is exactly the order bencode mandates).
       (def pairs (seq [[k v] :pairs d :when (not= nil v)] [(key->bytes k) v]))
       (sort-by 0 pairs)
       (buffer/push-byte buf (chr "d"))
       (each [k v] pairs
         (push-bytes buf k)
         (encode-value buf v))
       (buffer/push-byte buf (chr "e"))))

(defn encode
  "Bencode `x` into a buffer. If `buf` is given the bytes are appended to it,
  otherwise a fresh buffer is returned. Nil-valued dict entries are omitted
  (bencode has no nil). Errors on values with no bencode representation
  (nil scalar, boolean, non-integer number, function, ...)."
  [x &opt buf]
  (default buf @"")
  (encode-value buf x)
  buf)

### ---------------------------------------------------------------------------
### Decoding (incremental / streaming)
### ---------------------------------------------------------------------------
###
### The low-level decoder works on a buffer and a start position, returning a
### tuple [status value next-pos]:
###   :ok          -> a complete value was decoded; next-pos is just past it
###   :incomplete  -> not enough bytes yet; feed more and retry from the SAME pos
### Malformed input (bad byte, bad length, non-integer i...e) raises an error.

(defn- find-byte
  "Index of the first `b` at or after `pos`, or nil if not present."
  [buf pos b]
  (var i pos)
  (def n (length buf))
  (while (and (< i n) (not= (in buf i) b)) (++ i))
  (if (< i n) i nil))

(var- decode-value nil)

(defn- decode-int
  [buf pos]
  (def e (find-byte buf (inc pos) (chr "e")))
  (if (nil? e)
    [:incomplete nil pos]
    (let [s (string/slice buf (inc pos) e)
          n (scan-number s)]
      (when (or (nil? n) (not= n (math/floor n)))
        (errorf "invalid bencode integer: %v" s))
      [:ok n (inc e)])))

(defn- decode-string
  [buf pos]
  (def colon (find-byte buf pos (chr ":")))
  (if (nil? colon)
    [:incomplete nil pos]
    (let [len (scan-number (string/slice buf pos colon))]
      (when (or (nil? len) (neg? len) (not= len (math/floor len)))
        (errorf "invalid bencode string length at %d" pos))
      (def start (inc colon))
      (def end (+ start len))
      (if (> end (length buf))
        [:incomplete nil pos]
        [:ok (string/slice buf start end) end]))))

(defn- decode-list
  [buf pos]
  (def items @[])
  (var i (inc pos))
  (var result nil)
  (def n (length buf))
  (while (nil? result)
    (cond
      (>= i n) (set result [:incomplete nil pos])
      (= (in buf i) (chr "e")) (set result [:ok items (inc i)])
      (let [[st v np] (decode-value buf i)]
        (if (= st :incomplete)
          (set result [:incomplete nil pos])
          (do (array/push items v) (set i np))))))
  result)

(defn- decode-dict
  [buf pos]
  (def d @{})
  (var i (inc pos))
  (var result nil)
  (def n (length buf))
  (while (nil? result)
    (cond
      (>= i n) (set result [:incomplete nil pos])
      (= (in buf i) (chr "e")) (set result [:ok d (inc i)])
      (let [[ks kv knp] (decode-value buf i)]
        (cond
          (= ks :incomplete) (set result [:incomplete nil pos])
          (not (string? kv)) (errorf "bencode dict key must be a string at %d" i)
          (let [[vs vv vnp] (decode-value buf knp)]
            (if (= vs :incomplete)
              (set result [:incomplete nil pos])
              (do (put d (keyword kv) vv) (set i vnp))))))))
  result)

(set decode-value
     (fn decode-value [buf pos]
       (if (>= pos (length buf))
         [:incomplete nil pos]
         (let [c (in buf pos)]
           (cond
             (= c (chr "i")) (decode-int buf pos)
             (= c (chr "l")) (decode-list buf pos)
             (= c (chr "d")) (decode-dict buf pos)
             (and (>= c (chr "0")) (<= c (chr "9"))) (decode-string buf pos)
             (errorf "invalid bencode leading byte 0x%x at %d" c pos))))))

(defn decode
  "Decode a single complete bencode value from `bytes` (string or buffer).
  Returns the value. Raises if the input is incomplete. Trailing bytes after
  the first value are ignored; use a `decoder` for stream framing."
  [bytes]
  (def [status value _] (decode-value bytes 0))
  (assert (= status :ok) "incomplete bencode value")
  value)

### Streaming decoder: a stateful holding buffer over raw socket chunks.

(defn decoder
  "Create a streaming bencode decoder. Feed raw chunks with `feed` and pull
  complete top-level values with `take-message`."
  []
  @{:buf @"" :pos 0})

(defn feed
  "Append a raw chunk of bytes to the decoder's holding buffer."
  [dec bytes]
  (buffer/push-string (in dec :buf) bytes)
  dec)

(def- compact-threshold
  "Once consumed bytes exceed this, drop them so the holding buffer can't grow
  without bound across many messages on a long-lived connection."
  4096)

(defn take-message
  "Try to decode one complete top-level value from the decoder's buffer.
  Returns the value, or nil if more bytes are needed. Raises on malformed
  input. Drain in a loop after each `feed` until it returns nil."
  [dec]
  (def buf (in dec :buf))
  (def [status value newpos] (decode-value buf (in dec :pos)))
  (cond
    (not= status :ok) nil
    (do
      (set (dec :pos) newpos)
      (when (>= (in dec :pos) compact-threshold)
        (def leftover (buffer/slice buf (in dec :pos)))
        (buffer/clear buf)
        (buffer/push-string buf leftover)
        (set (dec :pos) 0))
      value)))
