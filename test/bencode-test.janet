(use spork/test)
(import ../src/nrepl/bencode :as b)

(start-suite "bencode")

# --- encoding scalars --------------------------------------------------------

(assert (= "i42e" (string (b/encode 42))) "encode positive int")
(assert (= "i0e" (string (b/encode 0))) "encode zero")
(assert (= "i-7e" (string (b/encode -7))) "encode negative int")
(assert (= "4:spam" (string (b/encode "spam"))) "encode string")
(assert (= "0:" (string (b/encode ""))) "encode empty string")
(assert (= "3:foo" (string (b/encode :foo))) "encode keyword as byte string")
(assert (= "3:bar" (string (b/encode 'bar))) "encode symbol as byte string")
(assert (= "5:hello" (string (b/encode @"hello"))) "encode buffer")

# --- encoding containers -----------------------------------------------------

(assert (= "l4:spam4:eggse" (string (b/encode ["spam" "eggs"]))) "encode list")
(assert (= "le" (string (b/encode []))) "encode empty list")
(assert (= "d3:cow3:moo4:spam4:eggse" (string (b/encode {"cow" "moo" "spam" "eggs"})))
        "encode dict")
(assert (= "de" (string (b/encode {}))) "encode empty dict")

# Dict keys MUST be emitted in sorted byte order regardless of insertion order.
(assert (= "d1:ai1e1:bi2e1:ci3ee" (string (b/encode @{:c 3 :a 1 :b 2})))
        "dict keys sorted as byte strings")
# Byte order, not codepoint-aware collation: "Z" (0x5A) sorts before "a" (0x61).
(assert (= "d1:Zi1e1:ai2ee" (string (b/encode @{:a 2 :Z 1})))
        "dict key sort is raw byte order")

# Nil-valued entries are omitted (bencode has no nil).
(assert (= "d1:ai1ee" (string (b/encode @{:a 1 :b nil})))
        "nil dict values omitted")

# --- binary safety -----------------------------------------------------------

(let [raw (string/from-bytes 0 1 2 255 0 10)]
  (assert (= raw (b/decode (b/encode raw))) "binary byte string round-trips"))

# --- decoding ----------------------------------------------------------------

(assert (= 42 (b/decode "i42e")) "decode int")
(assert (= -7 (b/decode "i-7e")) "decode negative int")
(assert (= "spam" (b/decode "4:spam")) "decode string")
(assert (deep= @["spam" "eggs"] (b/decode "l4:spam4:eggse")) "decode list")
(assert (deep= @{:cow "moo" :spam "eggs"} (b/decode "d3:cow3:moo4:spam4:eggse"))
        "decode dict with keyword keys")

# A realistic nREPL request message.
(let [msg (b/decode "d2:op4:eval4:code7:(+ 1 2)2:id2:42e")]
  (assert (= "eval" (msg :op)) "decoded :op")
  (assert (= "(+ 1 2)" (msg :code)) "decoded :code")
  (assert (= "42" (msg :id)) "decoded :id"))

# --- round-trip over nested structure ---------------------------------------

(let [orig @{:op "describe"
             :id "7"
             :status @["done"]
             :ops @{:eval @{} :clone @{}}
             :versions @{:janet "1.41.2"}}
      round (b/decode (b/encode orig))]
  (assert (deep= orig round) "nested structure round-trips"))

# --- streaming: a value split across several read chunks ---------------------

(let [full "d2:op4:eval2:id2:42e"
      dc (b/decoder)]
  # Feed one byte at a time; only the final byte should complete the message.
  (var got nil)
  (for i 0 (length full)
    (b/feed dc (string/slice full i (inc i)))
    (set got (or got (b/take-message dc))))
  (assert (deep= @{:op "eval" :id "42"} got) "message split across chunks decodes"))

# --- streaming: several messages in one chunk -------------------------------

(let [dec (b/decoder)]
  (b/feed dec "i1ei2ei3e")
  (assert (= 1 (b/take-message dec)) "first of three")
  (assert (= 2 (b/take-message dec)) "second of three")
  (assert (= 3 (b/take-message dec)) "third of three")
  (assert (= nil (b/take-message dec)) "no fourth message"))

# --- streaming: a trailing partial value is retained ------------------------

(let [dec (b/decoder)]
  (b/feed dec "i1e2:ab") # one whole int, then a complete string
  (assert (= 1 (b/take-message dec)) "whole value before partial")
  (assert (= "ab" (b/take-message dec)) "complete string")
  (b/feed dec "5:hel") # length says 5 but only 3 bytes present
  (assert (= nil (b/take-message dec)) "partial string not yet decodable")
  (b/feed dec "lo")
  (assert (= "hello" (b/take-message dec)) "partial string completes after feed"))

# --- malformed input raises --------------------------------------------------

(assert-error "invalid leading byte" (b/decode "x"))
(assert-error "non-integer i...e" (b/decode "i4.5e"))

(end-suite)
