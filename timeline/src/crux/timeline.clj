(ns crux.timeline
  (:require [clojure.java.io :as io])
  (:import [java.util Arrays Comparator List Map]
           java.io.RandomAccessFile
           [java.nio ByteBuffer ByteOrder MappedByteBuffer]
           java.nio.channels.FileChannel$MapMode
           java.nio.charset.StandardCharsets
           [org.roaringbitmap FastRankRoaringBitmap RoaringBitmap]
           [com.google.flatbuffers FlexBuffers FlexBuffers$Blob FlexBuffers$Key FlexBuffers$Map FlexBuffers$Reference FlexBuffersBuilder]))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol ClojureToFlex
  (clj->flex ^com.google.flatbuffers.FlexBuffersBuilder [this ^FlexBuffersBuilder builder ^String k]))

(defn clj->flex-key ^String [x]
  (if (keyword? x)
    (subs (str x) 1)
    (str x)))

(extend-protocol ClojureToFlex
  (class (byte-array 0))
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putBlob k this)))

  Boolean
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putBoolean k this)))

  Byte
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putInt ^String k ^int this)))

  Short
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putInt ^String k ^int this)))

  Integer
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putInt ^String k ^int this)))

  Long
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putInt ^String k ^long this)))

  Float
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putFloat ^String k ^float this)))

  Double
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putFloat ^String k ^double this)))

  String
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (doto builder
      (.putString ^String k this)))

  List
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (let [[type :as types] (distinct (map class this))
          typed? (and (= 1 (count types))
                      (or (.isAssignableFrom Number type)
                          (= Boolean type)))
          vector-ref (.startVector builder)]
      (doseq [x this]
        (clj->flex x builder nil))
      (.endVector builder k vector-ref typed? false)
      builder))

  Map
  (clj->flex [this ^FlexBuffersBuilder builder k]
    (let [map-ref (.startMap builder)]
      (doseq [[k v] this
              :let [k (clj->flex-key k)]]
        (clj->flex v builder k))
      (.endMap builder k map-ref))
    builder))

(defn clj->flexbuffer ^java.nio.ByteBuffer [x]
  (.finish (clj->flex x (FlexBuffersBuilder.) nil)))

(defn write-size-prefixed-buffer ^java.nio.ByteBuffer [^ByteBuffer out ^ByteBuffer b]
  (.putInt out (.remaining b))
  (.put out b))

(defn read-size-prefixed-buffer ^java.nio.ByteBuffer [^ByteBuffer in ^long position]
  (when (<= (+ position Integer/BYTES) (.capacity in))
    (let [size (.getInt in position)]
      (when (pos? size)
        (.slice in (+ position Integer/BYTES) size)))))

(defn flex-key->clj [^FlexBuffers$Key k]
  (keyword (str k)))

(defn flex-key-idx->clj [^FlexBuffers$Map m ^long key-idx]
  (flex-key->clj (.get (.keys m) key-idx)))

(defn flex-key->idx ^long [^FlexBuffers$Map m k]
  (let [k (clj->flex-key k)
        kv (.keys m)]
    (loop [n 0]
      (cond
        (= n (.size kv))
        -1
        (= k (str (.get kv n)))
        n
        :else
        (recur (inc n))))))

(defn flex->clj [^FlexBuffers$Reference ref]
  (cond
    (.isNull ref) nil
    (.isBoolean ref) (.asBoolean ref)
    (.isInt ref) (.asLong ref)
    (.isFloat ref) (.asFloat ref)
    (.isString ref) (.asString ref)
    (.isBlob ref) (.getBytes (.asBlob ref))
    (.isMap ref) (let [m (.asMap ref)
                       kv (.keys m)
                       vv (.values m)]
                   (loop [n 0
                          acc (transient {})]
                     (if (< n (.size kv))
                       (recur (inc n) (assoc! acc
                                              (flex-key->clj (.get kv n))
                                              (flex->clj (.get vv n))))
                       (persistent! acc))))
    (or (.isTypedVector ref)
        (.isVector ref)) (let [v (.asVector ref)]
                           (loop [n 0
                                  acc (transient [])]
                             (if (< n (.size v))
                               (recur (inc n) (conj! acc (flex->clj (.get v n))))
                               (persistent! acc))))
    :else
    (throw (IllegalArgumentException. (str "Unknown type: " (.getType ref))))))

(defn flex-root ^com.google.flatbuffers.FlexBuffers$Reference [^ByteBuffer b]
  (FlexBuffers/getRoot b))

(defn flexbuffer->clj [^ByteBuffer b]
  (flex->clj (flex-root b)))

(defn get-flex ^com.google.flatbuffers.FlexBuffers$Reference [^FlexBuffers$Reference ref k]
  (cond
    (.isMap ref) (.get (.asMap ref) (clj->flex-key k))
    (or (.isVector ref)
        (.isTypedVector ref)) (.get (.asVector ref) k)))

(defn round-up-to-next-multiple ^long [^long n ^long m]
  (bit-and (+ n (dec m)) (bit-not (dec m))))

(defn mmap-file ^java.nio.MappedByteBuffer [f size]
  (with-open [raf (doto (RandomAccessFile. (doto (io/file f)
                                             (io/make-parents)) "rw")
                    (.setLength size))
              in (.getChannel raf)]
    (.map in FileChannel$MapMode/READ_WRITE 0 (.size in))))

(defn realloc
  (^java.nio.ByteBuffer [^ByteBuffer b]
   (realloc b (* 2 (.capacity b))))
  (^java.nio.ByteBuffer [^ByteBuffer b ^long new-capacity]
   (let [new-buffer (if (.isDirect b)
                      (ByteBuffer/allocateDirect new-capacity)
                      (ByteBuffer/allocate new-capacity))]
     (.put new-buffer (.flip b)))))

(defn ensure-remaining-size ^java.nio.ByteBuffer [^ByteBuffer b ^long size]
  (if (>= (.remaining b) size)
    b
    (realloc b)))

(def ^:const column-width (* Long/BYTES 2))

(def ^:const column-type-nil 0)
(def ^:const column-type-boolean 1)
(def ^:const column-type-long 2)
(def ^:const column-type-double 3)
(def ^:const column-type-string 4)
(def ^:const column-type-bytes 5)

(def ^:const column-type-bit-pos 0)
(def ^:const column-type-bits 4)
(def ^:const column-size-bit-pos column-type-bits)
(def ^:const column-size-bits 4)
(def ^:const column-key-idx-bit-pos (+ column-size-bits column-size-bit-pos))
(def ^:const column-key-idx-bits 8)
(def ^:const column-tuple-id-bit-pos (+ column-key-idx-bits column-key-idx-bit-pos))
(def ^:const column-tuple-id-bits (- Long/SIZE column-tuple-id-bit-pos))

(def ^:const column-varlen-size 0xf)

(def ^:const fbt-type->column-type
  {FlexBuffers/FBT_NULL
   column-type-nil
   FlexBuffers/FBT_BOOL
   column-type-boolean
   FlexBuffers/FBT_INT
   column-type-long
   FlexBuffers/FBT_FLOAT
   column-type-double
   FlexBuffers/FBT_STRING
   column-type-string
   FlexBuffers/FBT_BLOB
   column-type-bytes})

(def ^:const column-type->kw
  {column-type-nil :column.type/nil
   column-type-boolean :column.type/boolean
   column-type-long :column.type/long
   column-type-double :column.type/double
   column-type-string :column.type/string
   column-type-bytes :column.type/bytes})

(defn ->column-id ^long [^long tuple-id ^long key-idx ^long size ^long type]
  (bit-or (bit-shift-left tuple-id column-tuple-id-bit-pos)
          (bit-shift-left key-idx column-key-idx-bit-pos)
          (bit-shift-left size column-size-bit-pos)
          (bit-shift-left type column-type-bit-pos)))

(defn column-id->tuple-id ^long [^long id]
  (bit-and (bit-shift-right id column-tuple-id-bit-pos)
           (dec (bit-shift-left 1 column-tuple-id-bits))))

(defn column-id->key-idx ^long [^long id]
  (bit-and (bit-shift-right id column-key-idx-bit-pos)
           (dec (bit-shift-left 1 column-key-idx-bits))))

(defn column-id->size ^long [^long id]
  (bit-and (bit-shift-right id column-size-bit-pos)
           (dec (bit-shift-left 1 column-size-bits))))

(defn column-id->type ^long [^long id]
  (bit-and (bit-shift-right id column-type-bit-pos)
           (dec (bit-shift-left 1 column-type-bits))))

(defn column-id->map [^long id]
  {:column/tuple-id (column-id->tuple-id id)
   :column/key-idx (column-id->key-idx id)
   :column/size (column-id->size id)
   :column/type (column-id->type id)})

(defn long->bytes ^bytes [^long size ^long x]
  (Arrays/copyOf (.array (.putLong (ByteBuffer/allocate Long/BYTES) x)) size))

(defn bytes->long ^long [^bytes x]
  (let [x (if (< (alength x) Long/BYTES)
            (Arrays/copyOf x Long/BYTES)
            x)]
    (.getLong (ByteBuffer/wrap x) 0)))

(defn eight-bytes->clj [^long type ^long size ^long x]
  (cond
    (= column-type-nil type) nil
    (= column-type-boolean type) (= 1 x)
    (= column-type-long type) x
    (= column-type-double type) (Double/longBitsToDouble x)
    (= column-type-string type) (String. ^bytes (long->bytes size x) StandardCharsets/UTF_8)
    (= column-type-bytes type) (long->bytes size x)
    :else
    (throw (IllegalArgumentException. "Unknown type: " type))))

(defn clj->eight-bytes ^long [x]
  (cond
    (nil? x) 0
    (boolean? x) (if x 1 0)
    (int? x) x
    (float? x) (Double/doubleToLongBits x)
    (string? x) (bytes->long (.getBytes ^String x StandardCharsets/UTF_8))
    (bytes? x) (bytes->long x)
    :else
    (throw (IllegalArgumentException. "Unknown type: " (.getName (class x))))))

(defn blob->eight-bytes ^long [^FlexBuffers$Blob b]
  (let [limit (min (.size b) Long/BYTES)]
    (loop [idx 0
           bit-idx (- Long/SIZE Byte/SIZE)
           acc 0]
      (if (= limit idx)
        acc
        (recur (inc idx)
               (- bit-idx Byte/SIZE)
               (bit-or acc (bit-shift-left (.get b idx) bit-idx)))))))

(defn flex->eight-bytes ^long [^FlexBuffers$Reference x]
  (cond
    (.isNull x) 0
    (.isBoolean x) (if (.asBoolean x) 1 0)
    (.isInt x) (.asLong x)
    (.isFloat x) (Double/doubleToLongBits (.asFloat x))
    (or (.isBlob x)
        (.isString x)) (blob->eight-bytes (.asBlob x))
    :else
    (throw (IllegalArgumentException. "Unknown type: " (.getType x)))))

(defn put-column ^java.nio.ByteBuffer [^ByteBuffer column ^long column-id ^long eight-bytes]
  (-> ^java.nio.ByteBuffer (ensure-remaining-size column column-width)
      (.putLong column-id)
      (.putLong eight-bytes)))

(defn swap-column ^java.nio.ByteBuffer [^ByteBuffer column ^long a-idx ^long b-idx]
  (if (= a-idx b-idx)
    column
    (let [a-idx (* a-idx column-width)
          b-idx (* b-idx column-width)
          a-column-id (.getLong column a-idx)
          a-eight-bytes (.getLong column (+ a-idx Long/BYTES))]
      (-> column
          (.putLong a-idx (.getLong column b-idx))
          (.putLong (+ a-idx Long/BYTES) (.getLong column (+ b-idx Long/BYTES)))
          (.putLong b-idx a-column-id)
          (.putLong (+ b-idx Long/BYTES) a-eight-bytes)))))

(defn project-column [^ByteBuffer column k tuple-id buffer]
  (let [root (flex-root buffer)
        flex-v (get-flex root k)
        type (get fbt-type->column-type (.getType flex-v))]
    (if (or (.isBlob flex-v) (.isString flex-v))
      (let [b (.asBlob flex-v)]
        (put-column column
                    (->column-id tuple-id
                                 (flex-key->idx (.asMap root) k)
                                 (if (<= (.size b) Long/BYTES)
                                   (.size b)
                                   column-varlen-size)
                                 type)
                    (blob->eight-bytes b)))
      (put-column column
                  (->column-id tuple-id
                               (flex-key->idx (.asMap root) k)
                               Long/BYTES
                               type)
                  (flex->eight-bytes flex-v)))))

(defn ->project-column
  (^java.nio.ByteBuffer [k ^ByteBuffer in]
   (->project-column k in (ByteBuffer/allocateDirect 4096) 0))
  (^java.nio.ByteBuffer [k ^ByteBuffer in ^ByteBuffer out ^long start-position]
   (loop [column out
          position start-position]
     (if-let [b (read-size-prefixed-buffer in position)]
       (recur (project-column column k position b)
              (+ position Integer/BYTES (.capacity ^ByteBuffer b)))
       column))))

(defn column-size ^long [^ByteBuffer column]
  (quot (.position column) column-width))

(defn column-capacity ^long [^ByteBuffer column]
  (quot (.capacity column) column-width))

(defn buffer-tuple-lookup ^com.google.flatbuffers.FlexBuffers$Reference [^ByteBuffer tuple-buffer ^long tuple-id]
  (flex-root (read-size-prefixed-buffer tuple-buffer tuple-id)))

(defn column->flex ^com.google.flatbuffers.FlexBuffers$Reference [tuple-lookup-fn ^long column-id]
  (let [tuple-id (column-id->tuple-id column-id)
        key-idx (column-id->key-idx column-id)
        root ^FlexBuffers$Reference (tuple-lookup-fn tuple-id)]
    (.get (.asMap root) key-idx)))

(defn get-column [tuple-lookup-fn ^ByteBuffer column ^long idx]
  (let [idx (* column-width idx)
        column-id (.getLong column idx)
        type (column-id->type column-id)
        size (column-id->size column-id)]
    (if (and (= column-varlen-size size)
             (or (= column-type-string type)
                 (= column-type-bytes type)))
      (flex->clj (column->flex tuple-lookup-fn column-id))
      (eight-bytes->clj type size (.getLong column (+ idx Long/BYTES))))))

(defn get-column->map [tuple-lookup-fn ^ByteBuffer column ^long idx]
  (let [{:column/keys [tuple-id key-idx] :as m} (column-id->map (.getLong column (* column-width idx)))
        t ^FlexBuffers$Reference (tuple-lookup-fn tuple-id)]
    (-> m
        (assoc :column/value (get-column tuple-lookup-fn column idx)
               :column/key (flex-key-idx->clj (.asMap t) key-idx)
               :column/tuple (flex->clj t))
        (update :column/type column-type->kw))))

(defn column->clj [tuple-lookup-fn column]
  (for [idx (range (column-size column))]
    (get-column tuple-lookup-fn column idx)))

(defn column->maps [tuple-lookup-fn column]
  (for [idx (range (column-size column))]
    (get-column->map tuple-lookup-fn column idx)))

(definterface ILiteralColumnComparator
  (^long compareAt [^clojure.lang.IFn tupleLookupFn ^java.nio.ByteBuffer column ^long idx]))

(definterface IColumnComparator
  (^long compareAt [^clojure.lang.IFn tupleLookupFn
                    ^java.nio.ByteBuffer columnA
                    ^long idxA
                    ^java.nio.ByteBuffer columnB
                    ^long idxB]))

(def ^crux.timeline.ILiteralColumnComparator nil-column-comparator
  (reify ILiteralColumnComparator
    (compareAt [_ _ column idx]
      (let [idx (* column-width idx)
            column-id (.getLong column idx)
            type (column-id->type column-id)]
        (Long/compare type column-type-nil)))))

(defn boolean-column-comparator ^crux.timeline.ILiteralColumnComparator [^Boolean x]
  (let [x (if x 1 0)]
    (reify ILiteralColumnComparator
      (compareAt [_ _ column idx]
        (let [idx (* column-width idx)
              column-id (.getLong column idx)
              type (column-id->type column-id)]
          (if (not= column-type-boolean type)
            (Long/compare type column-type-boolean)
            (Long/compare (.getLong column (+ idx Long/BYTES)) x)))))))

(defn long-column-comparator ^crux.timeline.ILiteralColumnComparator [^long x]
  (reify ILiteralColumnComparator
    (compareAt [_ _ column idx]
      (let [idx (* column-width idx)
            column-id (.getLong column idx)
            type (column-id->type column-id)]
        (if (not= column-type-long type)
          (Long/compare type column-type-long)
          (Long/compare (.getLong column (+ idx Long/BYTES)) x))))))

(defn double-column-comparator-column ^crux.timeline.ILiteralColumnComparator [^double x]
  (reify ILiteralColumnComparator
    (compareAt [_ _ column idx]
      (let [idx (* column-width idx)
            column-id (.getLong column idx)
            type (column-id->type column-id)]
        (if (not= column-type-double type)
          (Long/compare type column-type-double)
          (Double/compare (.getDouble column (+ idx Long/BYTES)) x))))))

(defn varlen-column-comparator ^crux.timeline.ILiteralColumnComparator [^long column-type ^bytes x]
  (let [x-eight-bytes (bytes->long x)]
    (reify ILiteralColumnComparator
      (compareAt [_ tuple-lookup-fn column idx]
        (let [idx (* column-width idx)
              column-id (.getLong column idx)
              type (column-id->type column-id)]
          (if (not= column-type type)
            (Long/compare type column-type)
            (let [diff (Long/compareUnsigned x-eight-bytes (.getLong column (+ idx Long/BYTES)))]
              (if (and (zero? diff) (= column-varlen-size (column-id->size column-id)))
                (let [ref ^FlexBuffers$Reference (column->flex tuple-lookup-fn column-id)]
                  (Arrays/compareUnsigned (.getBytes (.asBlob ref)) x))
                diff))))))))

(defn string-column-comparator ^crux.timeline.ILiteralColumnComparator [^String x]
  (varlen-column-comparator column-type-string (.getBytes ^String x StandardCharsets/UTF_8)))

(defn bytes-column-comparator ^crux.timeline.ILiteralColumnComparator [^bytes x]
  (varlen-column-comparator column-type-bytes x))

(defn ->literal-column-comparator ^crux.timeline.ILiteralColumnComparator [x]
  (cond
    (nil? x) nil-column-comparator
    (boolean? x) (boolean-column-comparator x)
    (int? x) (long-column-comparator x)
    (float? x) (double-column-comparator-column x)
    (string? x) (string-column-comparator x)
    (bytes? x) (bytes-column-comparator x)
    :else
    (throw (IllegalArgumentException. "Unknown type: " (.getName (class x))))))

(def ^IColumnComparator column-comparator
  (reify IColumnComparator
    (compareAt [_ tuple-lookup-fn column-a idx-a column-b idx-b]
      (let [idx-a (* column-width idx-a)
            column-id-a (.getLong column-a idx-a)
            type-a (column-id->type column-id-a)
            idx-b (* column-width idx-b)
            column-id-b (.getLong column-b idx-b)
            type-b (column-id->type column-id-b)]
        (cond
          (not= type-a type-b)
          (- type-a type-b)
          (or (= column-type-boolean type-a)
              (= column-type-long type-a))
          (Long/compare (.getLong column-a (+ idx-a Long/BYTES))
                        (.getLong column-b (+ idx-b Long/BYTES)))
          (= column-type-double type-a)
          (Double/compare (.getDouble column-a (+ idx-a Long/BYTES))
                          (.getDouble column-b (+ idx-b Long/BYTES)))
          (or (= column-type-string type-a)
              (= column-type-bytes type-a))
          (let [diff (Long/compareUnsigned (.getLong column-a (+ idx-a Long/BYTES))
                                           (.getLong column-b (+ idx-b Long/BYTES)))]
              (if (and (zero? diff) (= column-varlen-size (column-id->size column-id-a)))
                (let [ref-a ^FlexBuffers$Reference (column->flex tuple-lookup-fn column-id-a)
                      ref-b ^FlexBuffers$Reference (column->flex tuple-lookup-fn column-id-b)]
                  (Arrays/compareUnsigned (.getBytes (.asBlob ref-a))
                                          (.getBytes (.asBlob ref-b))))
                diff))
          :else
          (throw (IllegalArgumentException. "Unknown type: " type-a)))))))

;; See example in figure 4.1 in abadi-column-stores.pdf page 49 / 242

;; TODO: implement complete (MCI) or ripple (MRI) updates and cutter
;;       joins from StratosIdreosDBcrackingThesis.pdf

(defn upper-int ^long [^long x]
  (unsigned-bit-shift-right x Integer/SIZE))

(defn lower-int ^long [^long x]
  (bit-and x (Integer/toUnsignedLong -1)))

(defn two-ints-as-long ^long [^long x ^long y]
  (bit-or (bit-shift-left x Integer/SIZE) y))

(defn long-to-two-ints ^longs [^long x]
  (doto (long-array 2)
    (aset 0 (upper-int x))
    (aset 1 (lower-int x))))

(defn find-min-idx ^long [tuple-lookup-fn ^ByteBuffer column ^long low ^long hi]
  (loop [i (inc low)
         min-idx low]
    (if (< i hi)
      (let [diff (.compareAt column-comparator tuple-lookup-fn column i column min-idx)]
        (if (neg? diff)
          (recur (inc i) i)
          (recur (inc i) min-idx)))
      min-idx)))

(defn three-way-partition-column ^Long [tuple-lookup-fn ^ByteBuffer column ^Long low ^Long hi ^ILiteralColumnComparator pivot-comparator]
  (loop [i ^long low
         j ^long low
         k (inc ^long hi)]
    (if (< j k)
      (let [diff (.compareAt pivot-comparator tuple-lookup-fn column j)]
        (cond
          (neg? diff)
          (do (swap-column column i j)
              (recur (inc i) (inc j) k))

          (pos? diff)
          (let [k (dec k)]
            (swap-column column j k)
            (recur i j k))

          :else
          (recur i (inc j) k)))
      (if (= i k)
        (let [min-idx (find-min-idx tuple-lookup-fn column i hi)
              pivot-comparator (->literal-column-comparator (get-column tuple-lookup-fn column min-idx))]
          (three-way-partition-column tuple-lookup-fn column i hi pivot-comparator))
        (two-ints-as-long i (dec k))))))

(defn quick-sort-column
  (^java.nio.ByteBuffer [tuple-lookup-fn ^ByteBuffer column]
   (quick-sort-column tuple-lookup-fn column 0 (dec (column-size column))))
  (^java.nio.ByteBuffer [tuple-lookup-fn ^ByteBuffer column ^long low ^long hi]
   (if (< low hi)
     (let [pivot-comparator (->literal-column-comparator (get-column tuple-lookup-fn column hi))
           left-right (three-way-partition-column tuple-lookup-fn column low hi pivot-comparator)
           left (dec (upper-int left-right))
           right (inc (lower-int left-right))]
       (if (< (- hi right) (- left low))
         (do (quick-sort-column tuple-lookup-fn column right hi)
             (recur tuple-lookup-fn column low left))
         (do (quick-sort-column tuple-lookup-fn column low left)
             (recur tuple-lookup-fn column right hi))))
     column)))

(defn binary-search ^long [{:index/keys [^ByteBuffer column ^RoaringBitmap boundaries] :as index} tuple-lookup-fn ^ILiteralColumnComparator pivot-comparator]
  (if (.isEmpty boundaries)
    -1
    (loop [low 0
           hi (dec (.getCardinality boundaries))]
      (let [mid (long (Math/floor (/ (+ low hi) 2.0)))]
        (if (<= low hi)
          (let [idx (.select boundaries mid)
                diff (.compareAt pivot-comparator tuple-lookup-fn column idx)]
            (cond
              (pos? diff)
              (recur low (dec mid))
              (neg? diff)
              (recur (inc mid) hi)
              :else
              mid))
          (dec (- (inc mid))))))))

(defn crack-column [{:index/keys [^ByteBuffer column ^RoaringBitmap boundaries] :as index} tuple-lookup-fn at]
  (let [pivot-comparator (->literal-column-comparator at)
        idx (binary-search index tuple-lookup-fn pivot-comparator)]
    (if-not (neg? idx)
      index
      (let [next-idx (- (inc idx))
            left-right (cond
                         (.isEmpty boundaries)
                         (three-way-partition-column tuple-lookup-fn column 0 (dec (column-size column)) pivot-comparator)

                         (< next-idx (.getCardinality boundaries))
                         (let [next-piece-pos (.select boundaries next-idx)
                               prev-idx (dec next-idx)
                               prev-piece-pos (if-not (neg? prev-idx)
                                                (.select boundaries prev-idx)
                                                0)]
                           (three-way-partition-column tuple-lookup-fn column prev-piece-pos (dec next-piece-pos) pivot-comparator))

                         :else
                         (let [last-piece-pos (.last boundaries)]
                           (three-way-partition-column tuple-lookup-fn column last-piece-pos (dec (column-size column)) pivot-comparator)))
            boundary (upper-int left-right)]
        (.add boundaries boundary)
        index))))

(defn ->column-index [^ByteBuffer column]
  {:index/column column :index/boundaries (FastRankRoaringBitmap.)})

(comment
  (let [out (mmap-file "target/foo.flex" 4096)]
    (doseq [x (take 10 (crux.timeline-test/tpch-dbgen))]
      (write-size-prefixed-buffer out (clj->flexbuffer x)))
    (.force out)
    (let [col (->project-column :c_acctbal out)]
      [out
       (column-capacity col)
       (column-size col)
       (get-column (partial buffer-tuple-lookup out) col 9)]))

  (let [out (mmap-file "target/bar.flex" 4096)]
    (doseq [x (for [x [13 16 4 9 2 12 7 1 19 3 14 11 8 6]]
                {:x x})]
      (write-size-prefixed-buffer out (clj->flexbuffer x)))
    (.force out)
    (let [col (->project-column :x out)
          tuple-lookup-fn (partial buffer-tuple-lookup out)]
      [out
       (column-capacity col)
       (column-size col)
       (let [{:index/keys [column boundaries]}
             (-> (->column-index col)
                 (crack-column tuple-lookup-fn 11)
                 (crack-column tuple-lookup-fn 14)
                 (crack-column tuple-lookup-fn 8)
                 (crack-column tuple-lookup-fn 17))]
         [(column->clj tuple-lookup-fn column)
          boundaries])]))

  [[6 4 3 2 7 1 8 9 11 13 12 14 16 19]
   (doto (FastRankRoaringBitmap.)
     (.add (int-array [6 8 11 13])))])
