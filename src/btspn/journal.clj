(ns btspn.journal
  (:require
    [clojure.core.async :refer [go go-loop alts! chan put! >!! >! <! <!! pipe onto-chan close!]]
    [taoensso.timbre :refer [info trace debug]]
    [clojure.core.match :refer [match]])
  )

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))


(defn journal-seq [entry]
  (get entry 0))
(defn entity [entry]
  (get entry 1))
(defn id [entry]
  (get entry 2))
(defn payload [entry]
  (get entry 3))
(defn commit [seq entity id payload]
  [seq entity id payload])

(defprotocol JournalWriter
  (-write-entry [this entry])
  (-last-seq [this]))
(defn write-entry [writer entry]
  (-write-entry writer entry))
(defn last-seq [writer]
  (-last-seq writer))

(defprotocol JournalReader
  (-read-sequence [this from to])
  (-read-entity [this entity id]))
(defn read-entity [reader entity id]
  (-read-entity reader entity id))
(defn read-sequence [reader from to]
  (-read-sequence reader from to))

(defprotocol JournalStream
  (-publish-entry [this entry]))
(defn publish-entry-to-stream [stream entry]
  (-publish-entry stream entry))

(deftype MemoryWriter [entries]
  JournalWriter
  (-write-entry [this entry]
    (swap! entries conj entry))
  (-last-seq [this]
    (let [entry (last @entries)]
      (if entry (journal-seq entry) 0)))
  JournalReader
  (-read-sequence [this from to]
    (let [v    @entries
          from (max (- from 1) 0)
          to   (max from (min to (count v)))]
      (subvec v from to)))
  (-read-entity [this -entity -id]
    (filter #(and (= -entity (entity %)) (= -id (id %))) @entries)))
(defn memory-writer []
  (MemoryWriter. (atom [])))

(defn write-journal-entry [journal entity id payload]
  (go
    (let [resp (chan)]
      (>! journal [resp [entity id payload]])
      (<! resp))))

(deftype AsyncStream [channel]
  JournalStream
  (-publish-entry [this journal-entry]
    (put! channel journal-entry)))

(defrecord BufferedStream [buffer-size buffer]
  JournalStream
  (-publish-entry [this entry]
    (swap! buffer
           #(let [buffer (conj % entry)]
             (subvec buffer (- (count buffer) buffer-size))))))


(defn- retain-items [buffer buffer-size]
  (subvec buffer (max (- (count buffer) buffer-size) 0)))

(defn- serve-request [from to buffer reader queued resp]
  (let [f (if-let [entry (first buffer)]
            (journal-seq entry) 0)
        l (if-let [entry (last buffer)]
            (journal-seq entry)
            0)]
    (cond
      (< to f)
      (do (put! resp
                (read-sequence reader from to))
          queued)

      (<= f from l)
      (do (put! resp
                (filter #(<= from (journal-seq %) to) buffer))
          queued)

      (< l from)
      (conj queued [resp from to]))))

(defn- deque-requests [entry queued buffer reader]
  (let [candidates (filter (fn [[_ from to]]
                             (<= from (journal-seq entry) to))
                           queued)]
    (reduce (fn [queued [resp from to]]
              (serve-request from to buffer reader queued resp))
            (remove (set candidates) queued)
            candidates
            )))

(defn listen-to-journal [journal-reg buffer-size reader]
  (let [events-ch (chan)
        req-ch    (chan)
        stream    (AsyncStream. events-ch)]
    (>!! journal-reg [:register stream])
    (go-loop [buffer []
              queued []]
      (match
        (alts! [events-ch req-ch])

        [entry events-ch]
        (let [buffer (-> (conj buffer entry)
                         (retain-items buffer-size))]
          (recur buffer
                 (deque-requests entry queued buffer reader)))

        [[:find from to resp] req-ch]
        (do
          (recur buffer
                 (serve-request from to buffer reader queued resp)))

        [[:close] req-ch] (close! req-ch)))
    req-ch))

(defn poll-journal [journal-listener start-seq page-size]
  (let [response (chan)]
    (go-loop
      [last-seq start-seq]
      (let [resp (chan)
            from (inc last-seq)]
        (>! journal-listener [:find from (+ from page-size) resp])
        (let [entries  (<! resp)
              last-seq (if-let [entry (last entries)]
                         (journal-seq entry)
                         last-seq)]
          (onto-chan response entries false)
          (recur last-seq))))
    response))

(defn persistent-poll-journal [journal-listener journal id reader checkpoint-each page-size]
  (let [start-seq (if-let [current (last (read-entity reader "persistent-journal" id))]
                    current 0)
        response  (chan)
        entries   (poll-journal journal-listener start-seq page-size)]
    (go-loop
      [count 0]
      (let [entry (<! entries)]
        (>! response entry)
        (if (= (mod count checkpoint-each)
               (- checkpoint-each 1))
          (let [seq (journal-seq entry)]
            (debug (str "Checkpointing " id " on " seq))
            (write-journal-entry journal "persistent-journal" id seq)))
        (recur (inc count))))
    response))


(defn start-journal
  ([writer]
   (let [in  (chan)
         reg (chan)]
     (start-journal in reg writer)
     [in reg]))
  ([in reg writer]
   (info "Starting journal")
   (go-loop
     [journal-seq (last-seq writer)
      streams #{}]
     (match
       (alts! [in reg])
       [[out v] in] (let [journal-seq (inc journal-seq)
                          entry       (apply commit journal-seq v)]

                      (trace (str "Write entry " v))
                      (write-entry writer entry)
                      (>! out journal-seq)
                      (dorun (map (fn [stream]
                                    (publish-entry-to-stream stream entry))
                                  streams))
                      (recur journal-seq streams))
       [[:register v] reg] (do
                             (debug (str "Registering " v))
                             (recur journal-seq (conj streams v)))
       [[:unregister v] reg] (do
                               (debug (str "Unregistering " v))
                               (recur journal-seq (remove #{v} streams)))))
    ))
