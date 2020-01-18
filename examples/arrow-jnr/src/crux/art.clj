(ns crux.art
  (:require [clojure.java.io :as io]
            [clojure.string :as s])
  (:import java.util.Arrays
           java.time.Instant
           java.nio.ByteBuffer))

;;; Persistent Adaptive Radix Tree
;; http://www3.informatik.tu-muenchen.de/~leis/papers/ART.pdf
;; https://github.com/armon/libart
;; https://github.com/kellydunn/go-art
;; https://github.com/ankurdave/part.git

(set! *unchecked-math* :warn-on-boxed)

(defprotocol ARTNode
  (lookup [this key-byte])
  (insert [this key-byte value])
  (^bytes prefix [this])
  (minimum [this])
  (maximum [this]))

(defprotocol ARTBaseNode
  (grow-node [this])
  (make-node [this size bytes keys prefix]))

(defprotocol ARTKey
  (^bytes ->key-bytes [this]))

(defn key-position ^long [^long size ^bytes keys ^long key-byte]
  (Arrays/binarySearch keys 0 size (byte key-byte)))

(defn lookup-helper [^long size ^objects nodes ^bytes keys ^long key-byte]
  (let [pos (key-position size keys key-byte)]
    (when-not (neg? pos)
      (aget nodes pos))))

(defn make-gap [^long size ^long pos src dest]
  (System/arraycopy src pos dest (inc pos) (- size pos)))

(defn grow-helper [^long size ^bytes keys ^objects nodes node]
  (loop [idx 0
         node node]
    (if (< idx size)
      (recur (inc idx) (insert node (aget keys idx) (aget nodes idx)))
      node)))

(defn insert-helper [{:keys [^long size ^bytes keys ^objects nodes prefix] :as node}
                     ^long key-byte value]
  (let [pos (key-position size keys key-byte)
        new-key? (neg? pos)]
    (if (and new-key? (= size (alength keys)))
      (insert (grow-node node) key-byte value)
      (let [pos (cond-> pos
                  new-key? (-> (inc) (Math/abs)))
            new-keys (aclone keys)
            new-nodes (aclone nodes)]
        (when new-key?
          (make-gap size pos keys new-keys)
          (make-gap size pos nodes new-nodes))
        (make-node node
                   (cond-> size
                     new-key? (inc))
                   (doto new-keys (aset pos (byte key-byte)))
                   (doto new-nodes (aset pos value))
                   prefix)))))

(declare empty-node16 empty-node48 empty-node256)

(defrecord Node4 [^long size ^bytes keys ^objects nodes ^bytes prefix]
  ARTNode
  (lookup [this key-byte]
    (lookup-helper size nodes keys key-byte))

  (insert [this key-byte value]
    (insert-helper this key-byte value))

  (prefix [this]
    prefix)

  (minimum [this]
    (aget nodes 0))

  (maximum [this]
    (aget nodes (dec size)))

  ARTBaseNode
  (make-node [this size keys nodes prefix]
    (->Node4 size keys nodes prefix))

  (grow-node [this]
    (grow-helper size keys nodes (assoc empty-node16 :prefix prefix))))

(defrecord Node16 [^long size ^bytes keys ^objects nodes ^bytes prefix]
  ARTNode
  (lookup [this key-byte]
    (lookup-helper size nodes keys key-byte))

  (insert [this key-byte value]
    (insert-helper this key-byte value))

  (prefix [this]
    prefix)

  (minimum [this]
    (aget nodes 0))

  (maximum [this]
    (aget nodes (dec size)))

  ARTBaseNode
  (make-node [this size keys nodes prefix]
    (->Node16 size keys nodes prefix))

  (grow-node [this]
    (grow-helper size keys nodes (assoc empty-node48 :prefix prefix))))

(defrecord Node48 [^long size ^bytes key-index ^objects nodes ^bytes prefix]
  ARTNode
  (lookup [this key-byte]
    (let [key-int (Byte/toUnsignedInt key-byte)
          pos (aget key-index key-int)]
      (when-not (neg? pos)
        (aget nodes pos))))

  (insert [this key-byte value]
    (let [key-int (Byte/toUnsignedInt key-byte)
          pos (aget key-index key-int)
          new-key? (neg? pos)
          pos (if new-key?
                (byte size)
                pos)]
      (if (< pos (alength nodes))
        (->Node48 (cond-> size
                    new-key? (inc))
                  (doto (aclone key-index) (aset key-int pos))
                  (doto (aclone nodes) (aset pos value))
                  prefix)
        (loop [key 0
               node (assoc empty-node256 :prefix prefix)]
          (if (< key (alength key-index))
            (let [pos (aget key-index key)]
              (recur (inc key) (cond-> node
                                 (not (neg? pos)) (insert key (aget nodes pos)))))
            (insert node key-byte value))))))

  (prefix [this]
    prefix)

  (minimum [this]
    (loop [idx 0]
      (if-let [key-byte (aget key-index idx)]
        (lookup this key-byte)
        (recur (inc idx)))))

  (maximum [this]
    (loop [idx (dec (alength key-index))]
      (if-let [key-byte (aget key-index idx)]
        (lookup this key-byte)
        (recur (dec idx))))))

(defrecord Node256 [^long size ^objects nodes ^bytes prefix]
  ARTNode
  (lookup [this key-byte]
    (let [key-int (Byte/toUnsignedInt key-byte)]
      (aget nodes key-int)))

  (insert [this key-byte value]
    (let [key-int (Byte/toUnsignedInt key-byte)]
      (->Node256 (cond-> size
                   (nil? (aget nodes key-int)) inc)
                 (doto (aclone nodes) (aset key-int value))
                 prefix)))

  (prefix [this]
    prefix)

  (minimum [this]
    (loop [idx 0]
      (or (aget nodes idx) (recur (inc idx)))))

  (maximum [this]
    (loop [idx (dec (alength nodes))]
      (or (aget nodes idx) (recur (dec idx))))))

(def empty-node4 (->Node4 0 (byte-array 4 (byte -1)) (object-array 4) (byte-array 0)))

(def empty-node16 (->Node16 0 (byte-array 16 (byte -1)) (object-array 16) (byte-array 0)))

(def empty-node48 (->Node48 0 (byte-array 256 (byte -1)) (object-array 48) (byte-array 0)))

(def empty-node256 (->Node256 0 (object-array 256) (byte-array 0)))

(defrecord Leaf [^bytes key value])

(defn leaf-matches-key? [^Leaf leaf ^bytes key-bytes]
  (Arrays/equals key-bytes (bytes (.key leaf))))

(defn leaf-insert-helper [^Leaf leaf depth ^bytes key-bytes value]
  (let [prefix-start (long depth)]
    (loop [depth prefix-start]
      (let [new-key-byte (aget key-bytes depth)
            old-key-byte (aget (bytes (.key leaf)) depth)]
        (if (= new-key-byte old-key-byte)
          (recur (inc depth))
          (-> empty-node4
              (insert new-key-byte (->Leaf key-bytes value))
              (insert old-key-byte leaf)
              (assoc :prefix (Arrays/copyOfRange key-bytes prefix-start depth))))))))

(defn leaf? [node]
  (instance? Leaf node))

(defn common-prefix-length ^long [^bytes key-bytes ^bytes prefix ^long depth]
  (let [max-length (min (- (alength key-bytes) depth) (alength prefix))]
    (loop [idx 0]
      (if (and (< idx max-length)
               (= (aget key-bytes (+ depth idx)) (aget prefix idx)))
        (recur (inc idx))
        idx))))

(defn key-bytes->str ^String [^bytes key]
  (String. key 0 (dec (alength key)) "UTF-8"))

(defn key-bytes->long ^long [^bytes key]
  (bit-xor (.getLong (ByteBuffer/wrap key)) Long/MIN_VALUE))

(defn key-bytes->double ^double [^bytes key]
  (let [x (.getLong (ByteBuffer/wrap key))]
    (Double/longBitsToDouble (bit-xor (inc x) (bit-or Long/MIN_VALUE (bit-shift-left x (dec Long/SIZE)))))))

(defn key-bytes->instant ^java.time.Instant [^bytes key]
  (Instant/ofEpochSecond 0 (key-bytes->long key)))

(extend-protocol ARTKey
  (class (byte-array 0))
  (->key-bytes [this]
    this)

  ;; Strings needs to be 0 terminated, see IV. CONSTRUCTING BINARY-COMPARABLE KEYS
  ;; Should work for UTF-8, all non ASCII bytes have the highest bit set.
  ;; http://stackoverflow.com/a/6907327
  String
  (->key-bytes [this]
    (let [bytes (.getBytes this "UTF-8") ]
      (Arrays/copyOf bytes (inc (alength bytes)))))

  Long
  (->key-bytes [this]
    (-> (ByteBuffer/allocate Long/BYTES)
        (.putLong (bit-xor ^long this Long/MIN_VALUE))
        (.array)))

  Double
  (->key-bytes [this]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
      (-> (ByteBuffer/allocate Long/BYTES)
          (.putLong l)
          (.array))))

  Instant
  (->key-bytes [this]
    (->key-bytes (+ (* (.getEpochSecond this) 1000000000)
                    (.getNano this))))

  Object
  (->key-bytes [this]
    (->key-bytes (str this))))

;;; Public API

(defn art-make-tree []
  empty-node4)

(defn art-lookup [tree key]
  (let [key-bytes (->key-bytes key)]
    (loop [depth 0
           node tree]
      (if (leaf? node)
        (when (leaf-matches-key? node key-bytes)
          (.value ^Leaf node))
        (when node
          (let [prefix (prefix node)
                common-prefix-length (common-prefix-length key-bytes prefix depth)
                depth (+ depth common-prefix-length)]
            (when (= common-prefix-length (alength prefix))
              (recur (inc depth) (lookup node (aget key-bytes depth))))))))))

(defn art-insert
  ([tree key]
   (art-insert tree key key))
  ([tree key value]
   (let [key-bytes (->key-bytes key)]
     ((fn step [^long depth node]
        (let [prefix (prefix node)
              common-prefix-length (common-prefix-length key-bytes prefix depth)
              depth (+ depth common-prefix-length)]
          (if (= common-prefix-length (alength prefix))
            (let [key-byte (aget key-bytes depth)
                  child (lookup node key-byte)
                  new-child (if (and child (not (leaf? child)))
                              (step (inc depth) child)
                              (if (or (nil? child) (leaf-matches-key? child key-bytes))
                                (->Leaf key-bytes value)
                                (leaf-insert-helper child (inc depth) key-bytes value)))]
              (insert node key-byte new-child))
            (-> empty-node4
                (assoc :prefix (Arrays/copyOfRange prefix 0 common-prefix-length))
                (insert (aget key-bytes depth) (->Leaf key-bytes value))
                (insert (aget prefix common-prefix-length)
                        (assoc node :prefix (Arrays/copyOfRange prefix (inc common-prefix-length) (alength prefix))))))))
      0 (or tree (art-make-tree))))))

(defn art-minimum [tree]
  (if (leaf? tree)
    (.value ^Leaf tree)
    (recur (minimum tree))))

(defn art-maximum [tree]
  (if (leaf? tree)
    (.value ^Leaf tree)
    (recur (maximum tree))))
