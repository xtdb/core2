(ns crux.byte-keys
  (:import [java.util Arrays Date]
           java.time.Instant
           java.nio.ByteBuffer))

(defprotocol ByteKey
  (^bytes ->byte-key [this]))

(defn byte-key->str ^String [^bytes key]
  (String. key 0 (dec (alength key)) "UTF-8"))

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
              (.getInt))]
    (Float/intBitsToFloat (bit-xor x (bit-or (bit-shift-right (bit-xor x Integer/MIN_VALUE) (dec Integer/SIZE)) Integer/MIN_VALUE)))))

(defn byte-key->double ^double [^bytes key]
  (let [x (-> (ByteBuffer/wrap key)
              (.getLong))]
    (Double/longBitsToDouble (bit-xor x (bit-or (bit-shift-right (bit-xor x Long/MIN_VALUE) (dec Long/SIZE)) Long/MIN_VALUE)))))

(defn byte-key->instant ^java.time.Instant [^bytes key]
  (Instant/ofEpochSecond 0 (byte-key->long key)))

(defn byte-key->date ^java.util.Date [^bytes key]
  (Date/from (byte-key->instant key)))

;; Experimental var-int encoder. Idea is to respect lexiographic sort
;; as well as moving interesting bits to the front. First byte is
;; sign bit + number of bytes (4 bits) used for positive numbers or
;; skipped for negative, followed by the actual used bytes.
(defn long->var-int-byte-key ^bytes [^long l]
  (let [positive? (nat-int? l)
        pad-bytes (if (= -1 l)
                    (dec Long/BYTES)
                    (bit-shift-right (if positive?
                                       (Long/numberOfLeadingZeros l)
                                       (Long/numberOfLeadingZeros (bit-not l)))
                                     3))
        used-bytes (- Long/BYTES pad-bytes)
        header-byte (if positive?
                      (bit-or (bit-shift-left Long/BYTES 1) used-bytes)
                      pad-bytes)
        long-bytes (-> (ByteBuffer/allocate Long/BYTES)
                       (.putLong l)
                       (.array))]
    (-> (ByteBuffer/allocate (inc used-bytes))
        (.put (byte header-byte))
        (.put long-bytes pad-bytes used-bytes)
        (.array))))

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
    (let [bytes (.getBytes this "UTF-8") ]
      (Arrays/copyOf bytes (inc (alength bytes)))))

  Integer
  (->byte-key [this]
    (-> (ByteBuffer/allocate Integer/BYTES)
        (.putInt (bit-xor ^long this Integer/MIN_VALUE))
        (.array)))

  Long
  (->byte-key [this]
    (-> (ByteBuffer/allocate Long/BYTES)
        (.putLong (bit-xor ^long this Long/MIN_VALUE))
        (.array)))

  Float
  (->byte-key [this]
    (let [l (Float/floatToIntBits this)
          l (bit-xor l (bit-or (bit-shift-right l (dec Integer/SIZE)) Integer/MIN_VALUE))]
      (-> (ByteBuffer/allocate Integer/BYTES)
          (.putInt l)
          (.array))))

  Double
  (->byte-key [this]
    (let [l (Double/doubleToLongBits this)
          l (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE))]
      (-> (ByteBuffer/allocate Long/BYTES)
          (.putLong l)
          (.array))))

  Date
  (->byte-key [this]
    (->byte-key (.toInstant this)))

  Instant
  (->byte-key [this]
    (->byte-key (+ (* (.getEpochSecond this) 1000000000)
                   (.getNano this)))))
