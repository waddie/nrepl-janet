(declare-project
  :name "nrepl"
  :description "An nREPL server library for the Janet programming language."
  :author "Tom Waddington"
  :license "MIT"
  :dependencies ["https://github.com/janet-lang/spork.git"]
  :version "0.4.0"
  :url "https://github.com/waddie/nrepl-janet"
  :repo "git+https://github.com/waddie/nrepl-janet")

(declare-source
  :prefix "nrepl"
  :source @["src/nrepl/init.janet"
            "src/nrepl/bencode.janet"
            "src/nrepl/session.janet"
            "src/nrepl/eval.janet"
            "src/nrepl/ops.janet"
            "src/nrepl/server.janet"
            "src/nrepl/client.janet"])
