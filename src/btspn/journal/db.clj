(ns btspn.journal.db
  (:require
    [btspn.journal.core :as journal]
    [clojure.java.jdbc :as jdbc]
    [clojure.edn :as edn]))

(deftype DbJournal [db-spec table]
  journal/JournalWriter
  (-write-entry [this entry]
    (jdbc/insert! db-spec table {:journal_seq (journal/journal-seq entry)
                                 :entity      (journal/entity entry)
                                 :id          (journal/id entry)
                                 :payload     (pr-str (journal/payload entry))}))
  (-last-seq [this]
    ((jdbc/query db-spec [(str "select max(journal_seq) as max from " table)])
          first
          :max
          ) 0))
  journal/JournalReader
  (-read-sequence [this from to]
    (->> (jdbc/query db-spec [(str "select journal_seq, entity, id, payload from " table " where journal_seq between ? and ?") from to])
         (map (fn [[seq entity id payload]]
                (journal/commit seq entity id (edn/read-string payload))))))
  (-read-entity [this entity id]
    (->> (jdbc/query db-spec [(str "select journal_seq, entity, id, payload from " table " where entity = ? and id = ?") entity id])
         (map #(apply journal/commit %)))))

(defn db-writer [db-spec table]
  (DbJournal. db-spec table))