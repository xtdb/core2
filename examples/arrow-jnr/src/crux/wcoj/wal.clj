(ns crux.wcoj.wal
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [crux.wcoj :as wcoj])
  (:import [java.io File RandomAccessFile]
           java.nio.charset.StandardCharsets
           java.lang.ref.SoftReference
           java.lang.AutoCloseable))

(defprotocol WAL
  (write [this record])
  (read-all [this offset])
  (delete [this]))

(defn- replay-wal [wal offset relation]
  (reduce (fn [[_ relation] {:keys [record next-offset]}]
            (let [[op value] record]
              [next-offset
               (case op
                 :insert (wcoj/insert relation value)
                 :delete (wcoj/delete relation value))]))
          [offset (or relation (wcoj/*tuple-relation-factory* ""))]
          (read-all wal (if relation
                          offset
                          0))))

(deftype WALRelation [^:volatile-mutable ^long next-offset ^:volatile-mutable ^SoftReference relation wal]
  wcoj/Relation
  (table-scan [this db]
    (let [[next-offset relation] (replay-wal wal next-offset (.get relation))]
      (set! (.-relation this) (SoftReference. relation))
      (set! (.-next-offset this) next-offset)
      (wcoj/table-scan relation db)))

  (table-filter [this db var-bindings]
    (let [[next-offset new-relation] (replay-wal wal next-offset (.get relation))]
      (set! (.-relation this) (SoftReference. relation))
      (set! (.-next-offset this) next-offset)
      (wcoj/table-filter relation db var-bindings)))

  (insert [this value]
    (update this :wal write [:insert value]))

  (delete [this value]
    (update this :wal write [:delete value]))

  (truncate [this]
    (set! (.-relation this) (SoftReference. (some-> relation (wcoj/truncate))))
    (set! (.-next-offset this) 0)
    (update :wal delete))

  (cardinality [this]
    (let [[next-offset relation] (replay-wal wal next-offset (.get relation))]
      (set! (.-relation this) (SoftReference. relation))
      (set! (.-next-offset this) next-offset)
      (wcoj/cardinality relation)))

  AutoCloseable
  (close [this]
    (set! (.-relation this) nil)
    (set! (.-next-offset this) -1)
    (wcoj/try-close wal)))

(deftype EDNFileWAL [^:volatile-mutable ^RandomAccessFile write-raf ^File f sync?]
  WAL
  (write [this record]
    (when-not write-raf
      (set! (.-write-raf this) (RandomAccessFile. f (if sync? "rwd" "rw"))))
    (.write write-raf (.getBytes (prn-str record) StandardCharsets/UTF_8))
    this)

  (read-all [this offset]
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
    this)

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
    (->WALRelation 0 (SoftReference. relation) wal)))
