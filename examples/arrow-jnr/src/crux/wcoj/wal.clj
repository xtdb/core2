(ns crux.wcoj.wal
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [crux.wcoj :as wcoj])
  (:import [java.io File FileOutputStream]
           java.nio.charset.StandardCharsets
           java.lang.AutoCloseable))

(defprotocol WAL
  (write [this record])
  (read-all [this])
  (delete [this]))

(defrecord WALRelation [wal relation]
  wcoj/Relation
  (table-scan [this db]
    (wcoj/table-scan relation db))

  (table-filter [this db var-bindings]
    (wcoj/table-filter relation db var-bindings))

  (insert [this value]
    (-> this
        (update :wal write [::insert value])
        (update :relation wcoj/insert value)))

  (delete [this value]
    (-> this
        (update :wal write [::delete value])
        (update :relation wcoj/delete value)))

  (truncate [this]
    (-> this
        (update :wal delete)
        (update :relation wcoj/truncate)))

  (cardinality [this]
    (wcoj/cardinality relation))

  AutoCloseable
  (close [this]
    (wcoj/try-close wal)))

(deftype EDNFileWAL [^:volatile-mutable ^FileOutputStream out ^File f sync?]
  WAL
  (write [this record]
    (when-not out
      (set! out (FileOutputStream. f true)))
    (.write out (.getBytes (prn-str record) StandardCharsets/UTF_8))
    (when sync?
      (.sync (.getFD out)))
    this)

  (read-all [this]
    (when (.exists f)
      (with-open [reader (io/reader f)]
        (mapv edn/read-string (line-seq reader)))))

  (delete [this]
    (wcoj/try-close this)
    (.delete f)
    this)

  AutoCloseable
  (close [this]
    (wcoj/try-close out)
    (set! out nil)))

(defn new-edn-file-wal
  (^crux.wcoj.wal.EDNFileWAL [f]
   (new-edn-file-wal f false))
  (^crux.wcoj.wal.EDNFileWAL [f sync?]
   (let [f (io/file f)]
     (->EDNFileWAL nil f sync?))))

(defn- replay-wal [wal relation]
  (reduce (fn [relation [op value]]
            (case op
              ::insert (wcoj/insert relation value)
              ::delete (wcoj/delete relation value)))
          relation
          (read-all wal)))

(defn new-wal-relation ^crux.wcoj.wal.WALRelation [wal relation-or-name]
  (let [relation (if (string? relation-or-name)
                   (wcoj/*tuple-relation-factory* relation-or-name)
                   relation-or-name)]
    (->WALRelation wal (replay-wal wal relation))))
