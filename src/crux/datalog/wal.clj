(ns crux.datalog.wal
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [crux.datalog :as d])
  (:import [java.io File RandomAccessFile]
           java.nio.charset.StandardCharsets
           [java.lang.ref Reference SoftReference]
           java.lang.AutoCloseable))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol WAL
  (write-record [this record])
  (read-records [this offset])
  (delete [this]))

(defprotocol WALDirectory
  (list-wals [this])
  (get-wal-relation [this k]))

(defrecord WALRecord [record next-offset])

(defrecord WALRelationAndNextOffset [relation ^long next-offset])

(defn- new-wal-relation-and-next-offset ^crux.datalog.wal.WALRelationAndNextOffset [relation next-offset]
  (->WALRelationAndNextOffset relation next-offset))

(defn- replay-wal ^crux.datalog.wal.WALRelationAndNextOffset [wal ^WALRelationAndNextOffset relation-and-next-offset relation-name tuple-relation-factory]
  (let [relation-and-next-offset (or relation-and-next-offset
                                     (new-wal-relation-and-next-offset (tuple-relation-factory relation-name) 0))]
    (reduce (fn [relation-and-next-offset ^WALRecord wal-record]
              (let [[op value] (.record wal-record)]
                (-> relation-and-next-offset
                    (update :relation (case op
                                        :insert d/insert
                                        :delete d/delete) value)
                    (assoc :next-offset (.next-offset wal-record)))))
            relation-and-next-offset
            (read-records wal (.next-offset relation-and-next-offset)))))

(deftype WALRelation [name tuple-relation-factory ^:volatile-mutable ^Reference relation-and-next-offset wal]
  d/Relation
  (table-scan [this db]
    (let [relation-and-next-offset (.get relation-and-next-offset)
          new-relation-and-next-offset (replay-wal wal relation-and-next-offset name tuple-relation-factory)]
      (when-not (identical? new-wal-relation-and-next-offset relation-and-next-offset)
        (set! (.-relation-and-next-offset this) (SoftReference. new-relation-and-next-offset)))
      (d/table-scan (.relation new-relation-and-next-offset) db)))

  (table-filter [this db var-bindings]
    (let [relation-and-next-offset (.get relation-and-next-offset)
          new-relation-and-next-offset (replay-wal wal relation-and-next-offset name tuple-relation-factory)]
      (when-not (identical? new-wal-relation-and-next-offset relation-and-next-offset)
        (set! (.-relation-and-next-offset this) (SoftReference. new-relation-and-next-offset)))
      (d/table-filter (.relation new-relation-and-next-offset) db var-bindings)))

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
          new-relation-and-next-offset (replay-wal wal relation-and-next-offset name tuple-relation-factory)]
      (when-not (identical? new-wal-relation-and-next-offset relation-and-next-offset)
        (set! (.-relation-and-next-offset this) (SoftReference. new-relation-and-next-offset)))
      (d/cardinality (.relation new-relation-and-next-offset))))

  (relation-name [this]
    name)

  AutoCloseable
  (close [this]
    (set! (.-relation-and-next-offset this) nil)
    (d/try-close wal)))

(deftype EDNFileWAL [^:volatile-mutable ^RandomAccessFile write-raf ^File f sync?]
  WAL
  (write-record [this record]
    (when-not write-raf
      (io/make-parents f)
      (set! (.-write-raf this) (doto (RandomAccessFile. f (if sync? "rwd" "rw"))
                                 (.seek (.length f)))))
    (.write write-raf (.getBytes (prn-str record) StandardCharsets/UTF_8))
    (->WALRecord record (.getFilePointer write-raf)))

  (read-records [this offset]
    (when (and (.exists f) (> (.length f) ^long offset))
      (with-open [read-raf (doto (RandomAccessFile. f "r")
                             (.seek offset))]
        (->> (repeatedly #(when-let [l (.readLine read-raf)]
                            (->WALRecord (read-string l) (.getFilePointer read-raf))))
             (take-while some?)
             (vec)))))

  (delete [this]
    (d/try-close this)
    (.delete f)
    nil)

  AutoCloseable
  (close [this]
    (d/try-close write-raf)
    (set! (.-write-raf this) nil)))

(defn new-edn-file-wal
  (^crux.datalog.wal.EDNFileWAL [f]
   (new-edn-file-wal f false))
  (^crux.datalog.wal.EDNFileWAL [f sync?]
   (let [f (io/file f)]
     (->EDNFileWAL nil f sync?))))

(defn new-wal-relation ^crux.datalog.wal.WALRelation [relation-name wal tuple-relation-factory]
  (->WALRelation relation-name tuple-relation-factory (SoftReference. nil) wal))

(defrecord LocalDirectoryWALDirectory [^File dir wal-factory tuple-relation-factory suffix]
  WALDirectory
  (list-wals [this]
    (let [dir-path (.toPath dir)]
      (for [^File f (file-seq dir)
            :when (.isFile f)]
        (str (.relativize dir-path (.toPath f))))))

  (get-wal-relation [this k]
    (new-wal-relation k (wal-factory (io/file dir (str k suffix))) tuple-relation-factory)))

(defn new-local-directory-wal-directory
  ([dir]
   (new-local-directory-wal-directory dir new-edn-file-wal d/new-sorted-set-relation ""))
  ([dir wal-factory tuple-relation-factory suffix]
   (->LocalDirectoryWALDirectory dir wal-factory tuple-relation-factory suffix)))
