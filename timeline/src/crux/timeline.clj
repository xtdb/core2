(ns crux.timeline
  (:require [clojure.java.io :as io])
  (:import [java.util Arrays Comparator List Map]
           java.io.RandomAccessFile
           [java.nio ByteBuffer ByteOrder MappedByteBuffer]
           java.nio.channels.FileChannel$MapMode
           java.nio.charset.StandardCharsets
           [clojure.lang IReduceInit MapEntry]
           org.roaringbitmap.RoaringBitmap
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

(defn write-size-prefixed-buffer ^java.nio.ByteBuffer [^ByteBuffer out ^ByteBuffer bb]
  (.putInt out (.remaining bb))
  (.put out bb))

(defn read-size-prefixed-buffer ^java.nio.ByteBuffer [^ByteBuffer in]
  (when (.hasRemaining in)
    (let [size (.getInt in)]
      (when (pos? size)
        (let [position (.position in)
              bb (.slice in position size)]
          (.position in (+ size position))
          bb)))))

(defn read-size-prefixed-buffers-reducible [^ByteBuffer in]
  (reify IReduceInit
    (reduce [this f init]
      (loop [acc init]
        (if (reduced? acc)
          (unreduced acc)
          (let [position (.position in)]
            (if-let [x (read-size-prefixed-buffer in)]
              (recur (f acc (MapEntry/create position x)))
              (unreduced acc))))))))

(defn flex-root ^com.google.flatbuffers.FlexBuffers$Reference [^ByteBuffer b]
  (FlexBuffers/getRoot b))

(defn flex-key->clj [^FlexBuffers$Key k]
  (keyword (str k)))

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

(defn get-flex ^com.google.flatbuffers.FlexBuffers$Reference [^FlexBuffers$Reference ref k]
  (cond
    (.isMap ref) (.get (.asMap ref) (clj->flex-key k))
    (or (.isVector ref)
        (.isTypedVector ref)) (.get (.asVector ref) k)))

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
(def ^:const column-tuple-id-bit-pos (+ column-idx-bits column-key-idx-bit-pos))
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
   :column/type (get column-type->kw (column-id->type id))})

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

(defn ->map-entry ^clojure.lang.MapEntry [k v]
  (MapEntry/create k v))

(defn project-column
  ([f k tuple-id+buffer]
   (project-column f k (key tuple-id+buffer) (val tuple-id+buffer)))
  ([f k tuple-id buffer]
   (let [root (flex-root buffer)
         flex-v (get-flex root k)
         type (get fbt-type->column-type (.getType flex-v))]
     (if (or (.isBlob flex-v) (.isString flex-v))
       (let [b (.asBlob flex-v)]
         (f (->column-id tuple-id
                         (flex-key->idx (.asMap root) k)
                         (if (<= (.size b) Long/BYTES)
                           (.size b)
                           column-varlen-size)
                         type)
            (blob->eight-bytes b)))
       (f (->column-id tuple-id
                       (flex-key->idx (.asMap root) k)
                       Long/BYTES
                       type)
          (flex->eight-bytes flex-v))))))

(defn put-column-absolute ^java.nio.ByteBuffer [^ByteBuffer column ^long idx ^long column-id ^long eight-bytes]
  (let [idx (* column-width idx)]
    (-> column
        (.putLong idx column-id)
        (.putLong (+ idx Long/BYTES) eight-bytes))))

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

(defn ->project-column
  (^java.nio.ByteBuffer [k ^ByteBuffer in]
   (->project-column k in (ByteBuffer/allocateDirect 4096)))
  (^java.nio.ByteBuffer [k ^ByteBuffer in ^ByteBuffer out]
   (loop [col out
          pos (.position in)]
     (if-let [b (read-size-prefixed-buffer in)]
       (recur (project-column (partial put-column col) k pos b)
              (.position in))
       col))))

(defn column-size ^long [^ByteBuffer column]
  (quot (.position column) column-width))

(defn column-capacity ^long [^ByteBuffer column]
  (quot (.capacity column) column-width))

(defn buffer-tuple-lookup ^com.google.flatbuffers.FlexBuffers$Reference [^ByteBuffer tuple-buffer ^long tuple-id]
  (flex-root (read-size-prefixed-buffer (.position (.duplicate tuple-buffer) tuple-id))))

(defn column->flex ^com.google.flatbuffers.FlexBuffers$Reference [tuple-lookup-fn ^long column-id]
  (let [tuple-id (column-id->tuple-id column-id)
        key-idx (column-id->key-idx column-id)
        root ^FlexBuffers$Reference (tuple-lookup-fn tuple-id)]
    (.get (.asMap root) key-idx)))

(defn get-column-absolute [tuple-lookup-fn ^ByteBuffer column ^long idx]
  (let [idx (* column-width idx)
        column-id (.getLong column idx)
        type (column-id->type column-id)
        size (column-id->size column-id)]
    (if (and (= column-varlen-size size)
             (or (= column-type-string type)
                 (= column-type-bytes type)))
      (flex->clj (column->flex tuple-lookup-fn column-id))
      (eight-bytes->clj type size (.getLong column (+ idx Long/BYTES))))))

(defn get-column-absolute->map [tuple-lookup-fn ^ByteBuffer column ^long idx]
  (assoc (column-id->map (.getLong column (* column-width idx)))
         :column/value (get-column-absolute tuple-lookup-fn column idx)))

(defn column->clj [tuple-lookup-fn column]
  (for [idx (range (column-size column))]
    (get-column-absolute tuple-lookup-fn column idx)))

(defn column->maps [tuple-lookup-fn column]
  (for [idx (range (column-size column))]
    (get-column-absolute->map tuple-lookup-fn column idx)))

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

(defn flexbuffer->clj [^ByteBuffer b]
  (flex->clj (flex-root b)))

(defn round-up-to-next-multiple ^long [^long n ^long m]
  (bit-and (+ n (dec m)) (bit-not (dec m))))

(defn mmap-file ^java.nio.MappedByteBuffer [f size]
  (with-open [raf (doto (RandomAccessFile. (doto (io/file f)
                                             (io/make-parents)) "rw")
                    (.setLength size))
              in (.getChannel raf)]
    (.map in FileChannel$MapMode/READ_WRITE 0 (.size in))))

;; See example in figure 4.1 in abadi-column-stores.pdf page 49 / 242

;; TODO: implement ripple updates and cutter joins from
;;       StratosIdreosDBcrackingThesis.pdf

;; Potentially useful for incremental index maintenance.

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

;; NOTE: might not be used.
;; https://stratos.seas.harvard.edu/files/IKM_CIDR07.pdf

(defn crack-in-two-column ^Long [tuple-lookup-fn ^ByteBuffer column ^Long low ^Long hi ^ILiteralColumnComparator med-comparator]
  (loop [x1 ^long low
         x2 ^long hi]
    (if (< x1 x2)
      (if (neg? (.compareAt med-comparator tuple-lookup-fn column x1))
        (recur (inc x1) x2)
        (let [^long x2 (loop [x2 x2]
                         (if (and (not (neg? (.compareAt med-comparator tuple-lookup-fn column x2)))
                                  (> x2 x1))
                           (recur (dec x2))
                           x2))]
          (swap-column column x1 x2)
          (recur (inc x1) (dec x2))))
      (two-ints-as-long x1 x2))))

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
              pivot-comparator (->literal-column-comparator (get-column-absolute tuple-lookup-fn column min-idx))]
          (three-way-partition-column tuple-lookup-fn column i hi pivot-comparator))
        (two-ints-as-long i (dec k))))))

(defn quick-sort-column
  (^java.nio.ByteBuffer [tuple-lookup-fn ^ByteBuffer column]
   (quick-sort-column tuple-lookup-fn column 0 (dec (column-size column))))
  (^java.nio.ByteBuffer [tuple-lookup-fn ^longs column ^long low ^long hi]
   (if (< low hi)
     (let [pivot-comparator (->literal-column-comparator (get-column-absolute tuple-lookup-fn column hi))
           left-right (three-way-partition-column tuple-lookup-fn column low hi pivot-comparator)
           left (dec (upper-int left-right))
           right (inc (lower-int left-right))]
       (if (< (- hi right) (- left low))
         (do (quick-sort-column tuple-lookup-fn column right hi)
             (recur tuple-lookup-fn column low left))
         (do (quick-sort-column tuple-lookup-fn column low left)
             (recur tuple-lookup-fn column right hi))))
     column)))

(defn binary-search ^long [tuple-lookup-fn ^ByteBuffer column ^RoaringBitmap boundaries ^ILiteralColumnComparator pivot-comparator]
  (loop [low 0
         hi (dec (.getCardinality boundaries))]
    (let [mid (quot (+ hi low) 2)
          i (.select boundaries mid)]
      (if (<= low hi)
        (let [diff (.compareAt pivot-comparator tuple-lookup-fn column i)]
          (cond
            (neg? diff)
            (recur low (dec mid))
            (pos? diff)
            (recur (inc mid) hi)
            :else
            mid))
        (- -1 mid)))))

(defn crack-column [{:index/keys [^ByteBuffer column ^RoaringBitmap boundaries] :as index} tuple-lookup-fn at]
  (let [pivot-comparator (->literal-column-comparator at)
        idx (if (.isEmpty boundaries)
              -1
              (binary-search tuple-lookup-fn column boundaries pivot-comparator))]
    (if-not (neg? idx)
      index
      (let [idx (- -1 idx)
            boundary (->> (if (.isEmpty boundaries)
                            (three-way-partition-column tuple-lookup-fn column 0 (dec (column-size column)) pivot-comparator)
                            (if (< idx (.getCardinality boundaries))
                              (let [next-piece-pos (.select boundaries idx)
                                    prev-piece-pos (.select boundaries (dec idx))]
                                (three-way-partition-column tuple-lookup-fn column (or prev-piece-pos 0) (dec next-piece-pos) pivot-comparator))
                              (let [last-piece-pos (.last boundaries)]
                                (three-way-partition-column tuple-lookup-fn column last-piece-pos (dec (column-size column)) pivot-comparator))))
                          (upper-int))]
        (.add boundaries boundary)
        index))))

(defn ->column-index [^ByteBuffer column]
  {:index/column column :index/boundaries (RoaringBitmap.)})

(defn three-way-partition
  (^long [^longs a ^long pivot]
   (three-way-partition a 0 (dec (alength a)) pivot))
  (^long [^longs a ^long low ^long hi ^long pivot]
   (loop [i low
          j low
          k (inc hi)]
     (if (< j k)
       (let [aj (aget a j)
             diff (- aj pivot)]
         (cond
           (neg? diff)
           (do (doto a
                 (aset j (aget a i))
                 (aset i aj))
               (recur (inc i) (inc j) k))

           (pos? diff)
           (let [k (dec k)]
             (doto a
               (aset j (aget a k))
               (aset k aj))
             (recur i j k))

           :else
           (recur i (inc j) k)))
       (two-ints-as-long i (dec k))))))

(defn quick-sort
  (^longs [^longs a]
   (quick-sort a 0 (dec (alength a))))
  (^longs [^longs a ^long low ^long hi]
   (if (< low hi)
     (let [pivot (aget a hi)
           left-right (three-way-partition a low hi pivot)
           left (dec (upper-int left-right))
           right (inc (lower-int left-right))]
       (if (< (- hi right) (- left low))
         (do (quick-sort a right hi)
             (recur a low left))
         (do (quick-sort a low left)
             (recur a right hi))))
     a)))

(defn crack-array [{:index/keys [^longs column pieces] :as index} ^long at]
  (if (contains? pieces at)
    index
    (->> (if (empty? pieces)
           (three-way-partition column at)
           (if-let [next-pieces (not-empty (subseq pieces >= at))]
             (let [[[_ ^long next-piece-pos]] next-pieces
                   [[_ prev-piece-pos]] (rsubseq pieces < at)]
               (three-way-partition column (or prev-piece-pos 0) (dec next-piece-pos) at))
             (let [[_ last-piece-pos] (last pieces)]
               (three-way-partition column last-piece-pos (dec (alength column)) at))))
         (upper-int)
         (assoc-in index [:index/pieces at]))))

(defn ->index [column]
  {:index/column column :index/pieces (sorted-map)})

(comment
  (-> (->array-index (long-array [13 16 4 9 2 12 7 1 19 3 14 11 8 6]))
      (crack-array 11)
      (crack-array 14)
      (crack-array 8)
      (crack-array 17))

  #:index{:column [6, 4, 3, 2, 7, 1, 8, 9, 11, 13, 12, 14, 16, 19],
          :pieces {8                 6,
                   11                      8,
                   14                                  11,
                   17                                          13}})

(comment
  (let [out (mmap-file "target/foo.flex" 4096)]
    (doseq [x (take 10 (crux.timeline-test/tpch-dbgen))]
      (write-size-prefixed-buffer out (clj->flexbuffer x)))
    (.force out)
    (let [col (->project-column :c_acctbal (.rewind out))]
      [out
       (column-capacity col)
       (column-size col)
       (get-column-absolute (partial buffer-tuple-lookup out) col 9)]))


  (let [out (mmap-file "target/bar.flex" 4096)]
    (doseq [x (for [x [13 16 4 9 2 12 7 1 19 3 14 11 8 6]]
                {:x x})]
      (write-size-prefixed-buffer out (clj->flexbuffer x)))
    (.force out)
    (let [col (->project-column :x (.rewind out))
          tuple-lookup-fn (partial buffer-tuple-lookup out)]
      [out
       (column-capacity col)
       (column-size col)
       (-> (->column-index col)
           (crack-column tuple-lookup-fn 11)
           (crack-column tuple-lookup-fn 14)
           (crack-column tuple-lookup-fn 8)
           (crack-column tuple-lookup-fn 17))])))
