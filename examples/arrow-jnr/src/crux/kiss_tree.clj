(ns crux.kiss-tree
  (:import [java.nio.channels FileChannel FileChannel$MapMode]
           [java.nio.file OpenOption Path StandardOpenOption]
           [java.nio MappedByteBuffer]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector ValueVector]))

(def ^:private ^BufferAllocator
  allocator (RootAllocator. Long/MAX_VALUE))

(defrecord KissTree [vectors ^MappedByteBuffer top-level])

(defn new-kiss-tree ^crux.kiss_tree.KissTree []
  (with-open [f (FileChannel/open
                 (Path/of (str "/dev/shm/" (gensym "kiss_tree_")) (make-array String 0))
                 (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/READ
                                         StandardOpenOption/WRITE StandardOpenOption/DELETE_ON_CLOSE]))]
    (->KissTree (atom [])
                (.map f FileChannel$MapMode/PRIVATE 0 (* 4096 65536)))))

(defn- top-level-offset ^long [^long k]
  (let [l1-offset (unsigned-bit-shift-right k 16)
        l2-offset (bit-and (dec 1024) (unsigned-bit-shift-right k 6))]
     (+ (* 4096 l1-offset) (* l2-offset Integer/BYTES))))

(defn- vector-offset ^long [^long k]
  (bit-and (dec 64) k))

(defn insert-kv [^KissTree t ^long k v]
  (let [offset (top-level-offset k)
        idx (.getInt ^MappedByteBuffer (.top-level t) offset)]
    (if (zero? idx)
      (let [size (count (swap! (.vectors t) conj (doto (BigIntVector. "" allocator)
                                                   (.setInitialCapacity 64)
                                                   (.setSafe (vector-offset k) (long v)))))]
          (.putInt ^MappedByteBuffer (.top-level t)
                   offset
                   size))
      (.setSafe ^BigIntVector (get (.vectors t) (dec idx)) (vector-offset k) (long v)))
    t))

(defn get-kv [^KissTree t ^long k]
  (let [offset (top-level-offset k)
        l3 (vector-offset k)
        idx (.getInt ^MappedByteBuffer (.top-level t) offset)]
    (when (pos? idx)
      (.getObject ^ValueVector (get @(.vectors t) (dec idx)) l3))))
