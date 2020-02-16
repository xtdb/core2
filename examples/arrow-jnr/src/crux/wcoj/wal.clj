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

(defrecord WALRecord [record next-offset])

(defrecord WALRelationAndNextOffset [relation ^long next-offset])

(defn- new-wal-relation-and-next-offset ^crux.wcoj.wal.WALRelationAndNextOffset [relation next-offset]
  (->WALRelationAndNextOffset relation next-offset))

(defn- replay-wal ^crux.wcoj.wal.WALRelationAndNextOffset [wal ^WALRelationAndNextOffset relation-and-next-offset]
  (let [relation-and-next-offset (or relation-and-next-offset
                                     (new-wal-relation-and-next-offset (wcoj/*tuple-relation-factory* "") 0))]
    (reduce (fn [relation-and-next-offset ^WALRecord wal-record]
              (let [[op value] (.record wal-record)]
                (-> relation-and-next-offset
                    (update :relation (case op
                                        :insert wcoj/insert
                                        :delete wcoj/delete) value)
                    (assoc :next-offset (.next-offset wal-record)))))
            relation-and-next-offset
            (read-records wal (.next-offset relation-and-next-offset)))))

(deftype WALRelation [^:volatile-mutable ^Reference relation-and-next-offset wal]
  wcoj/Relation
  (table-scan [this db]
    (let [relation-and-next-offset (.get relation-and-next-offset)
          new-relation-and-next-offset (replay-wal wal relation-and-next-offset)]
      (when-not (identical? new-wal-relation-and-next-offset relation-and-next-offset)
        (set! (.-relation-and-next-offset this) (SoftReference. new-relation-and-next-offset)))
      (wcoj/table-scan (.relation new-relation-and-next-offset) db)))

  (table-filter [this db var-bindings]
    (let [relation-and-next-offset (.get relation-and-next-offset)
          new-relation-and-next-offset (replay-wal wal relation-and-next-offset)]
      (when-not (identical? new-wal-relation-and-next-offset relation-and-next-offset)
        (set! (.-relation-and-next-offset this) (SoftReference. new-relation-and-next-offset)))
      (wcoj/table-filter (.relation new-relation-and-next-offset) db var-bindings)))

  (insert [this value]
    (write-record wal [:insert value])
    this)

  (delete [this value]
    (write-record wal [:delete value])
    this)

  (truncate [this]
    (set! (.-relation-and-next-offset this) (SoftReference. nil))
    (delete wal)
    this)

  (cardinality [this]
    (let [relation-and-next-offset (.get relation-and-next-offset)
          new-relation-and-next-offset (replay-wal wal relation-and-next-offset)]
      (when-not (identical? new-wal-relation-and-next-offset relation-and-next-offset)
        (set! (.-relation-and-next-offset this) (SoftReference. new-relation-and-next-offset)))
      (wcoj/cardinality (.relation new-relation-and-next-offset))))

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
    (->WALRecord record (.getFilePointer write-raf)))

  (read-records [this offset]
    (when (and (.exists f) (> (.length f) offset))
      (with-open [read-raf (doto (RandomAccessFile. f "r")
                             (.seek offset))]
        (->> (repeatedly #(when-let [l (.readLine read-raf)]
                            (->WALRecord (read-string l) (.getFilePointer read-raf))))
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
    (->WALRelation (SoftReference. nil) wal)))
