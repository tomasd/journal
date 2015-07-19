(ns user
  (:use [btspn.journal.core])
  (:require
    [clojure.core.async :refer [go-loop <! chan]]
    [taoensso.timbre :refer [debug]]
    ))

(defn run []
  (let [writer  (memory-writer)
        [journal journal-reg] (start-journal writer)
        stream  (listen-to-journal journal-reg 100 writer)
        entries (persistent-poll-journal stream  journal "1" writer 3 10)]

    (write-journal-entry journal "e" 1 "aaa")
    (write-journal-entry journal "e" 1 "bbb")
    (write-journal-entry journal "e" 2 "xxx")
    (write-journal-entry journal "e" 2 "yyy")
    (write-journal-entry journal "e" 1 "ccc")
    (go-loop
      []
      (let [entry (<! entries)]
        (debug entry)
        (recur))
      )
    journal
    ))