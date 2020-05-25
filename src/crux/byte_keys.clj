(ns crux.byte-keys
  (:require [clojure.edn :as edn])
  (:import [java.util Arrays Comparator Date]
           java.time.Instant
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets))

(set! *unchecked-math* :warn-on-boxed)

(def ^Comparator unsigned-bytes-comparator
  (reify Comparator
    (compare [_ x y]
      (Arrays/compareUnsigned ^bytes x ^bytes y))))

(defprotocol ByteKey
  (^bytes ->byte-key [this]))

(defn byte-key->str ^String [^bytes key]
  (String. key 0 (dec (alength key)) StandardCharsets/UTF_8))

(defn byte-key->boolean ^Boolean [^bytes key]
  (= -1 (aget key 0)))

(defn byte-key->int ^long [^bytes key]
  (bit-xor (-> (ByteBuffer/wrap key)
               (.getInt)) Integer/MIN_VALUE))

(defn byte-key->long ^long [^bytes key]
  (bit-xor (-> (ByteBuffer/wrap key)
               (.getLong)) Long/MIN_VALUE))

(defn byte-key->float ^Float [^bytes key]
  (let [x (-> (ByteBuffer/wrap key)
              (.getInt)
              (dec))]
    (Float/intBitsToFloat (bit-xor x (bit-or (bit-shift-right (bit-not x) (dec Integer/SIZE)) Integer/MIN_VALUE)))))

(defn byte-key->double ^double [^bytes key]
  (let [x (-> (ByteBuffer/wrap key)
              (.getLong)
              (dec))]
    (Double/longBitsToDouble (bit-xor x (bit-or (bit-shift-right (bit-not x) (dec Long/SIZE)) Long/MIN_VALUE)))))

(defn byte-key->instant ^java.time.Instant [^bytes key]
  (Instant/ofEpochSecond 0 (byte-key->long key)))

(defn byte-key->date ^java.util.Date [^bytes key]
  (Date/from (byte-key->instant key)))

(defn byte-key->clojure [^bytes key]
  (edn/read-string (String. key StandardCharsets/UTF_8)))

(defn byte-key->var-int ^long [^bytes key]
  (let [header-byte (aget key 0)
        bits (bit-and (dec Long/SIZE) header-byte)
        l (-> (ByteBuffer/allocate Long/BYTES)
              (.put key 1 (dec (alength key)))
              (.rewind)
              (.getLong))]
    (cond
      (neg? header-byte)
      (unsigned-bit-shift-right l (- Long/SIZE bits))

      (zero? bits)
      -1

      :else
      (bit-not (unsigned-bit-shift-right (bit-not l) bits)))))

(defn long->var-int-byte-key ^bytes [^long l]
  (let [bits (- Long/SIZE
                (if (neg? l)
                  (Long/numberOfLeadingZeros (bit-not l))
                  (Long/numberOfLeadingZeros l)))
        used-bytes (inc (quot bits Byte/SIZE))
        header-byte (if (neg? l)
                      (- Long/SIZE bits)
                      (bit-or Byte/MIN_VALUE bits))
        first-long (bit-shift-left l (- Long/SIZE bits))
        long-bytes (-> (ByteBuffer/allocate Long/BYTES)
                       (.putLong first-long)
                       (.array))
        output-bytes (inc used-bytes)]
    (-> (ByteBuffer/allocate output-bytes)
        (.put (unchecked-byte header-byte))
        (.put long-bytes 0 (min Long/BYTES used-bytes))
        (.array))))

(def ^:dynamic *use-var-ints?* true)

(extend-protocol ByteKey
  (class (byte-array 0))
  (->byte-key [this]
    this)

  Boolean
  (->byte-key [this]
    (if this
      (byte-array 1 (byte -1))
      (byte-array 1 (byte 0))))

  ;; Strings needs to be 0 terminated, see IV. CONSTRUCTING BINARY-COMPARABLE KEYS
  ;; Should work for UTF-8, all non ASCII bytes have the highest bit set.
  ;; http://stackoverflow.com/a/6907327
  String
  (->byte-key [this]
    (let [bytes (.getBytes this StandardCharsets/UTF_8) ]
      (Arrays/copyOf bytes (inc (alength bytes)))))

  Integer
  (->byte-key [this]
    (if *use-var-ints?*
      (long->var-int-byte-key this)
      (-> (ByteBuffer/allocate Integer/BYTES)
          (.putInt (bit-xor ^long this Integer/MIN_VALUE))
          (.array))))

  Long
  (->byte-key [this]
    (if *use-var-ints?*
      (long->var-int-byte-key this)
      (-> (ByteBuffer/allocate Long/BYTES)
          (.putLong (bit-xor ^long this Long/MIN_VALUE))
          (.array))))

  Float
  (->byte-key [this]
    (let [l (Float/floatToIntBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Integer/SIZE)) Integer/MIN_VALUE)))]
      (-> (ByteBuffer/allocate Integer/BYTES)
          (.putInt l)
          (.array))))

  Double
  (->byte-key [this]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
      (-> (ByteBuffer/allocate Long/BYTES)
          (.putLong l)
          (.array))))

  Date
  (->byte-key [this]
    (->byte-key (.toInstant this)))

  Instant
  (->byte-key [this]
    (->byte-key (+ (* (.getEpochSecond this) 1000000000)
                   (.getNano this))))

  Object
  (->byte-key [this]
    (->byte-key (pr-str this))))

(defn inc-unsigned-bytes ^bytes [^bytes bs]
  (loop [idx (dec (alength bs))]
    (when-not (neg? idx)
      (let [b (Byte/toUnsignedInt (aget bs idx))]
        (if (= 0xff b)
          (do (aset bs idx (byte 0))
              (recur (dec idx)))
          (doto bs
            (aset idx (unchecked-byte (inc b)))))))))

(defn dec-unsigned-bytes ^bytes [^bytes bs]
  (loop [idx (dec (alength bs))]
    (when-not (neg? idx)
      (let [b (Byte/toUnsignedInt (aget bs idx))]
        (if (zero? b)
          (do (aset bs idx (byte 0xff))
              (recur (dec idx)))
          (doto bs
            (aset idx (unchecked-byte (dec b)))))))))
