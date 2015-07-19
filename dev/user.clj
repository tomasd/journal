(ns user
  (:use [btspn.journal.core])
  (:require
    [clojure.core.async :refer [go-loop <! chan]]
    [taoensso.timbre :refer [debug]]
    [jdbc.pool.hikari :as pool]
    [btspn.journal.db :as db]
    [clojure.java.jdbc :as jdbc]))

(defn run []
  (let [writer  (memory-writer)
        [journal journal-reg] (start-journal writer)
        stream  (listen-to-journal journal-reg 100 writer)
        entries (persistent-poll-journal stream journal "1" writer 3 10)]

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



(defn- db-spec []
  (pool/make-datasource-spec {:database-name "test"
                              :username      "postgres"
                              :password      "postgres"
                              :server-name   "localhost"
                              :port-number   5433
                              :adapter       "postgresql"}))

(defn create-tables []
  (let [db-spec (db-spec)]
    (try
      (jdbc/execute!
        db-spec
        ["create table if not exists journal (
       journal_seq int primary key,
       entity varchar,
       id varchar,
       payload varchar
        )"])
      (finally
        (.close db-spec)))))

(defn run-db []
  (let [db-spec (db-spec)
        writer  (db/db-writer db-spec "journal")
        [journal journal-reg] (start-journal writer)
        stream  (listen-to-journal journal-reg 100 writer)
        entries (persistent-poll-journal stream journal "1" writer 3 10)]
    #_(try)
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
    writer
    #_(finally
      (.close db-spec))))