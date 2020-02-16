(ns crux.wcoj.wal
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [crux.wcoj :as wcoj])
  (:import [java.io File RandomAccessFile]
           java.nio.charset.StandardCharsets
           [java.lang.ref Reference SoftReference]
           java.lang.AutoCloseable))

(defprotocol WAL
  (write-record [this record])
  (read-records [this offset])
  (delete [this]))

(defrecord WALRelationAndNextOffset [^SoftReference relation ^long next-offset])

(defn- new-wal-relation-and-next-offset ^crux.wcoj.wal.WALRelationAndNextOffset [relation next-offset]
  (->WALRelationAndNextOffset (SoftReference. relation) next-offset))

(defn- replay-wal ^crux.wcoj.wal.WALRelationAndNextOffset [wal ^WALRelationAndNextOffset relation-and-next-offset]
  (let [relation (.get ^Reference (.relation relation-and-next-offset))
        next-offset (if relation
                      (.next-offset relation-and-next-offset)
                      0)
        [relation next-offset] (reduce (fn [[relation] {:keys [record next-offset]}]
                                         (let [[op value] record]
                                           [(case op
                                              :insert (wcoj/insert relation value)
                                              :delete (wcoj/delete relation value))
                                            next-offset]))
                                       [(or relation (wcoj/*tuple-relation-factory* ""))
                                        next-offset]
                                       (read-records wal next-offset))]
    (new-wal-relation-and-next-offset relation next-offset)))

(deftype WALRelation [^:volatile-mutable ^WALRelationAndNextOffset relation-and-next-offset wal]
  wcoj/Relation
  (table-scan [this db]
    (let [new-relation-and-next-offset (replay-wal wal relation-and-next-offset)]
      (set! (.-relation-and-next-offset this) new-relation-and-next-offset)
      (wcoj/table-scan (.get ^Reference (.relation new-relation-and-next-offset)) db)))

  (table-filter [this db var-bindings]
    (let [new-relation-and-next-offset (replay-wal wal relation-and-next-offset)]
      (set! (.-relation-and-next-offset this) new-relation-and-next-offset)
      (wcoj/table-filter (.get ^Reference (.relation new-relation-and-next-offset)) db var-bindings)))

  (insert [this value]
    (write-record wal [:insert value])
    this)

  (delete [this value]
    (write-record wal [:delete value])
    this)

  (truncate [this]
    (set! (.-relation-and-next-offset this)
          (new-wal-relation-and-next-offset (some-> (.get ^Reference (.relation relation-and-next-offset)) (wcoj/truncate)) 0))
    (delete wal)
    this)

  (cardinality [this]
    (let [new-relation-and-next-offset (replay-wal wal relation-and-next-offset)]
      (set! (.-relation-and-next-offset this) new-relation-and-next-offset)
      (wcoj/cardinality (.get ^Reference (.relation new-relation-and-next-offset)))))

  AutoCloseable
  (close [this]
    (set! (.-relation-and-next-offset this) nil)
    (wcoj/try-close wal)))

(deftype EDNFileWAL [^:volatile-mutable ^RandomAccessFile write-raf ^File f sync?]
  WAL
  (write-record [this record]
    (when-not write-raf
      (set! (.-write-raf this) (doto (RandomAccessFile. f (if sync? "rwd" "rw"))
                                 (.seek (.length f)))))
    (.write write-raf (.getBytes (prn-str record) StandardCharsets/UTF_8))
    {:record record
     :next-offset (.getFilePointer write-raf)})

  (read-records [this offset]
    (when (and (.exists f) (> (.length f) offset))
      (with-open [read-raf (doto (RandomAccessFile. f "r")
                             (.seek offset))]
        (->> (repeatedly #(when-let [l (.readLine read-raf)]
                            {:record (read-string l)
                             :next-offset (.getFilePointer read-raf)}))
             (take-while some?)
             (vec)))))

  (delete [this]
    (wcoj/try-close this)
    (.delete f)
    nil)

  AutoCloseable
  (close [this]
    (wcoj/try-close write-raf)
    (set! (.-write-raf this) nil)))

(defn new-edn-file-wal
  (^crux.wcoj.wal.EDNFileWAL [f]
   (new-edn-file-wal f false))
  (^crux.wcoj.wal.EDNFileWAL [f sync?]
   (let [f (io/file f)]
     (->EDNFileWAL nil f sync?))))

(defn new-wal-relation ^crux.wcoj.wal.WALRelation [wal relation-or-name]
  (let [relation (if (string? relation-or-name)
                   (wcoj/*tuple-relation-factory* relation-or-name)
                   relation-or-name)]
    (->WALRelation (new-wal-relation-and-next-offset relation 0) wal)))
