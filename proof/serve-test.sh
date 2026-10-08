#!/bin/sh
# Runs proof/serve-test.janet against `proof serve` once per scenario, then lets
# proof check the requests the client sent. Needs proof on the PATH:
#
#   go install github.com/nrepl/proof/cmd/proof@latest
#   proof/serve-test.sh
set -u
cd "$(dirname "$0")/.."

port=${PORT:-17920}
failed=""

for args in "" split-output empty-messages last-value no-err error-with-done \
  no-op-echo no-close-op no-interrupt no-stdin read-line-throws \
  string-versions no-session-closed shared-state socket-sessions any-session \
  ns-fallback ns-error eof-error unsorted-keys byte-writes batched-writes \
  hang-up -like=babashka -like=clojure-clr -like=basilisp -like=jank \
  -like=dialtone -like=repartee; do
  echo "--- proof serve ${args:-(nREPL)}"
  # $args is unquoted on purpose: the empty run passes no arguments.
  proof serve -listen 127.0.0.1:$port $args &
  serve=$!
  until nc -z 127.0.0.1 $port 2>/dev/null; do
    kill -0 $serve 2>/dev/null || exit 2
    sleep 0.2
  done
  janet proof/serve-test.janet $port || failed="$failed ${args:-(nREPL)}"
  kill -INT $serve
  wait $serve || failed="$failed ${args:-(nREPL)}(proof)"
done

if [ -n "$failed" ]; then
  echo "failed:$failed"
  exit 1
fi
