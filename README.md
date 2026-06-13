# nrepl-janet

An [nREPL](https://nrepl.org) server library for the
[Janet](https://janet-lang.org) programming language.

nREPL is a language-agnostic, message-oriented network REPL protocol from the
Clojure community. A conforming nREPL server lets Janet plug into the existing
ecosystem of nREPL clients and editor integrations. This library speaks the
standard bencode-over-socket nREPL wire protocol.

## Status

Working server covering the operations editors depend on:

| Op            | Notes                                                         |
| ------------- | ------------------------------------------------------------- |
| `clone`       | new session, optionally inheriting another session’s env      |
| `close`       | cancel any running eval and tear the session down             |
| `describe`    | advertises supported ops + `versions`                         |
| `eval`        | form-by-form, streaming `out`/`err`, one `value` per form     |
| `load-file`   | evaluate file contents with a source name for error locations |
| `interrupt`   | `ev/cancel` the running eval (cooperative — see caveat)       |
| `ls-sessions` | list active sessions                                          |
| `lookup`      | doc / arglists / source location for a symbol                 |
| `completions` | prefix completion over the session’s bindings                 |
| `stdin`       | feed input to a blocked read (see stdin caveat)               |

**Interrupt caveat:** cancellation is cooperative. `ev/cancel` only takes effect
when the evaluation yields to the event loop (I/O, `ev/sleep`, channel ops).

**Stdin caveat:** input is read through `getline`, which is shadowed during
evaluation to emit `need-input` and wait for a `stdin` op. Janet’s real
`getline` reads the server process’s blocking C `stdin`, which can’t signal
`need-input` and would stall the single-threaded event loop — so code that reads
input via other means (e.g. `(file/read stdin :line)`) is not redirected. An
empty `stdin` payload signals end-of-input.

## Installation

Either packaging system works; both read the package name and dependencies from
the same `project.janet`.

```sh
jpm install            # via the Janet Project Manager
janet --install .      # via Janet’s built-in bundle system
```

`spork` must already be on the Janet syspath before installing. It’s both a
runtime dependency and (for `janet --install`) a build-time one. Install it the
same way first if needed: `janet --install https://github.com/janet-lang/spork.git`.

## Usage

```janet
(import nrepl)

# Block until the listener is closed:
(nrepl/run-server "127.0.0.1" "7888")

# Or run non-blocking and keep the returned listener to stop later:
(def listener (nrepl/server "127.0.0.1" "7888"))
# ... later ...
(:close listener)
```

Defaults are `127.0.0.1:7888`.

### Starting from the shell (jack-in)

Once installed, a one-liner that blocks on the listener, suitable for editor
jack-in, with the host/port baked into the launched command:

```sh
janet -e '(import nrepl)(nrepl/run-server "127.0.0.1" "7888")'
```

### Talking to it from Janet

A minimal client ships for testing and quick poking:

```janet
(import nrepl)
(def c (nrepl/connect "127.0.0.1" "7888"))

(def session
  (-> (nrepl/request c {:op "clone" :id "1"})
      (first)
      (get :new-session)))

(nrepl/request c {:op "eval" :id "2" :session session :code "(+ 1 2)"})
# => @[@{:value "3" :ns "user" ...} @{:status @["done"] ...}]
```

## License

Copyright © 2026 Tom Waddington

Distributed under the MIT License. See LICENSE file for details.
