(ns crux.timeline
  (:require [clojure.java.io :as io])
  (:import [java.util Arrays Comparator List Map]
           java.io.RandomAccessFile
           [java.nio ByteBuffer ByteOrder MappedByteBuffer]
           java.nio.channels.FileChannel$MapMode
           java.nio.charset.StandardCharsets
           [clojure.lang IReduceInit MapEntry]
           [com.google.flatbuffers FlexBuffers FlexBuffers$Key FlexBuffers$Map FlexBuffers$Reference FlexBuffersBuilder]))

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
    (throw (IllegalArgumentException. (str "Unsupported type: " (.getType ref))))))

(defn get-flex ^com.google.flatbuffers.FlexBuffers$Reference [^FlexBuffers$Reference ref k]
  (cond
    (.isMap ref) (.get (.asMap ref) (clj->flex-key k))
    (or (.isVector ref)
        (.isTypedVector ref)) (.get (.asVector ref) k)))

(def ^:const column-type-nil 0)
(def ^:const column-type-boolean 1)
(def ^:const column-type-long 2)
(def ^:const column-type-double 3)
(def ^:const column-type-string 4)
(def ^:const column-type-bytes 5)

(def ^:const column-type-bit-pos 0)
(def ^:const column-type-bits 4)
(def ^:const column-bytes-bit-pos column-type-bits)
(def ^:const column-bytes-bits 4)
(def ^:const column-idx-bit-pos (+ column-bytes-bits column-bytes-bit-pos))
(def ^:const column-idx-bits 8)
(def ^:const column-tuple-id-bit-pos (+ column-idx-bits column-idx-bit-pos))
(def ^:const column-tuple-id-bits (- Long/SIZE column-tuple-id-bit-pos))

(def fbt-type->column-type {FlexBuffers/FBT_NULL
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

(defn column-id ^long [^long tuple-id ^long idx ^long bytes ^long type]
  (bit-or (bit-shift-left tuple-id column-tuple-id-bit-pos)
          (bit-shift-left idx column-idx-bit-pos)
          (bit-shift-left bytes column-bytes-bit-pos)
          (bit-shift-left type column-type-bit-pos)))

(defn column-id->tuple-id ^long [^long id]
  (bit-and (bit-shift-right id column-tuple-id-bit-pos)
           (dec (bit-shift-left 1 column-tuple-id-bits))))

(defn column-id->idx ^long [^long id]
  (bit-and (bit-shift-right id column-idx-bit-pos)
           (dec (bit-shift-left 1 column-idx-bits))))

(defn column-id->bytes ^long [^long id]
  (bit-and (bit-shift-right id column-bytes-bit-pos)
           (dec (bit-shift-left 1 column-bytes-bits))))

(defn column-id->type ^long [^long id]
  (bit-and (bit-shift-right id column-type-bit-pos)
           (dec (bit-shift-left 1 column-type-bits))))

(defn column-id->map [^long id]
  {:column/tuple-id (column-id->tuple-id id)
   :column/idx (column-id->idx id)
   :column/bytes (column-id->bytes id)
   :column/type (column-id->type id)})

(defn long->bytes ^bytes [^long bytes ^long x]
  (Arrays/copyOf (.array (.putLong (.order (ByteBuffer/allocate Long/BYTES) ByteOrder/BIG_ENDIAN) x)) bytes))

(defn bytes->long ^long [^bytes x]
  (.getLong (.order (ByteBuffer/wrap (Arrays/copyOf x Long/BYTES)) ByteOrder/BIG_ENDIAN) 0))

(defn eight-bytes->clj [^long type ^long bytes ^long x]
  (cond
    (= column-type-nil type) nil
    (= column-type-boolean type) (= 1 x)
    (= column-type-long type) x
    (= column-type-double type) (Double/longBitsToDouble x)
    (= column-type-bytes type)
    (long->bytes bytes x)
    (= column-type-string type)
    (String. ^bytes (long->bytes bytes x) StandardCharsets/UTF_8)
    :else
    (throw (IllegalArgumentException. "Unknown type: " type))))

(defn clj->eight-bytes ^long [x]
  (cond
    (nil? x) 0
    (boolean? x) (if x 1 0)
    (int? x) x
    (float? x) (Double/doubleToLongBits x)
    (bytes? x) (.getLong (.order (ByteBuffer/wrap x) ByteOrder/BIG_ENDIAN) 0)
    (string? x)
    (bytes->long (.getBytes ^String x StandardCharsets/UTF_8))
    :else
    (throw (IllegalArgumentException. "Unknown type: " (.getName (class x))))))

(defn ->map-entry ^clojure.lang.MapEntry [k v]
  (MapEntry/create k v))

(defn project-column [f k tuple-id+buffer]
  (let [tuple-id (key tuple-id+buffer)
        root (flex-root (val tuple-id+buffer))
        flex-v (get-flex root k)
        v (flex->clj flex-v)
        type (get fbt-type->column-type (.getType flex-v))]
    (if (or (.isBlob flex-v)
            (.isString flex-v))
      (let [bs (if (string? v)
                 (.getBytes ^String v StandardCharsets/UTF_8)
                 ^bytes v)]
        (f (column-id tuple-id
                      (flex-key->idx (.asMap root) k)
                      (if (<= (alength bs) Long/BYTES)
                        (alength ^bytes bs)
                        0xf)
                      type)
           (bytes->long bs)))
      (f (column-id tuple-id
                    (flex-key->idx (.asMap root) k)
                    0xf
                    type)
         (clj->eight-bytes v)))))

(defn put-column-absolute ^java.nio.ByteBuffer [^ByteBuffer column ^long idx ^long column-id ^long eight-bytes]
  (let [idx (* (* Long/BYTES 2) idx)]
    (-> column
        (.putLong idx column-id)
        (.putLong (+ idx Long/BYTES) eight-bytes))))

(defn put-column ^java.nio.ByteBuffer [^ByteBuffer column ^long column-id ^long eight-bytes]
  (-> column
      (.putLong column-id)
      (.putLong eight-bytes)))

(defn get-column-absolute [^ByteBuffer tuple-buffer ^ByteBuffer column ^long idx]
  (let [idx (* (* Long/BYTES 2) idx)
        column-id (.getLong column idx)
        eight-bytes (.getLong column (+ idx Long/BYTES))
        type (column-id->type column-id)
        bytes (column-id->bytes column-id)]
    (if (and (= 0xf bytes)
             (or (= column-type-string type)
                 (= column-type-bytes type)))
      (let [tuple-id (column-id->tuple-id column-id)
            col-idx (column-id->idx column-id)
            root (flex-root (read-size-prefixed-buffer (.position (.duplicate tuple-buffer) tuple-id)))]
        (flex->clj (.get (.asMap root) col-idx)))
      (eight-bytes->clj type bytes eight-bytes))))

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

(defn crack-column [{:index/keys [^longs column pieces] :as index} ^long at]
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

(defn ->index [^longs ls]
  {:index/column ls :index/pieces (sorted-map)})

(comment
  (-> (->index (long-array [13 16 4 9 2 12 7 1 19 3 14 11 8 6]))
      (crack-column 11)
      (crack-column 14)
      (crack-column 8)
      (crack-column 17))

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
    [out
     (into []
           (map (partial project-column ->map-entry :c_name))
           (read-size-prefixed-buffers-reducible (.rewind out)))
     (some-> (read-size-prefixed-buffer (.position (.rewind out) 2425))
             (flexbuffer->clj))]))

;; https://stratos.seas.harvard.edu/files/IKM_CIDR07.pdf

;; Algorithm 1 CrackInTwo(c,posL,posH,med,inc)
;; Physically reorganize the piece of column c between posL
;; and posH such that all values lower than med are in a contiguous space. inc indicates whether med is inclusive or not,
;; e.g., if inc = f alse then θ1 is “<” and θ2 is “>=”
;; 1: x1 = point at position posL
;; 2: x2 = point at position posH
;; 3: while position(x1) < position(x2) do
;; 4: if value(x1) θ1 med then
;; 5: x1 = point at next position
;; 6: else
;; 7: while value(x2) θ2 med &&
;; position(x2) > position(x1) do
;; 8: x2 = point at previous position
;; 9: end while
;; 10: exchange(x1,x2)
;; 11: x1 = point at next position
;; 12: x2 = point at previous position
;; 13: end if
;; 14: end while
