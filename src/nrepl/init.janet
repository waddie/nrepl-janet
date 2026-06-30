###
### nrepl
###
### An nREPL server library for the Janet programming language. Speaks the
### standard bencode-over-socket nREPL wire protocol, so Janet can plug into the
### existing ecosystem of nREPL clients and editor integrations.
###
### Quick start:
###   (import nrepl)
###   (nrepl/run-server "127.0.0.1" "7888")   # blocks
### or, non-blocking:
###   (def listener (nrepl/server))            # returns the listening stream
###   ...
###   (:close listener)

(import ./server)
(import ./client)
(import ./bencode)

# Server API
(def server
  "Start an nREPL server and return the listening stream. See `server/server`."
  server/server)
(def run-server
  "Start an nREPL server and block until closed. See `server/run-server`."
  server/run-server)
(def default-host server/default-host)
(def default-port server/default-port)

# Serial client API (mainly for tests / quick poking)
(def connect client/connect)
(def request client/request)

# Multiplexing client API (for building real tools: id-routed responses,
# merged results, interrupt-while-eval). See `client.janet`.
(def connect-mux client/connect-mux)
(def call client/call)
(def send-async client/send-async)
(def await-result client/await-result)
(def clone-session client/clone-session)
(def eval-code client/eval-code)
(def load-file-code client/load-file-code)
(def lookup client/lookup)
(def completions client/completions)
(def describe client/describe)
(def ls-sessions client/ls-sessions)
(def interrupt client/interrupt)
(def close-mux client/close-mux)
(def find-nrepl-port client/find-nrepl-port)

# Codec (exposed for tooling and tests)
(def encode bencode/encode)
(def decode bencode/decode)
