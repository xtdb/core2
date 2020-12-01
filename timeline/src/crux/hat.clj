(ns crux.cache.hat
  (:import java.nio.ByteBuffer))

(deftype HATTrie [^objects children ^long level])

(defn ->hat-trie
  ([] (->hat-trie 0))
  ([^long level]
   (->HATTrie (object-array 256) level)))

(defn- child-idx ^long [^HATTrie tree ^ByteBuffer buffer]
  (assert (.hasRemaining buffer) "empty elements not allowed")
  (Byte/toUnsignedInt (.get buffer (rem (.level tree) (.remaining buffer)))))

(defn find-element [^HATTrie tree ^ByteBuffer buffer]
  (let [idx (child-idx tree buffer)
        children ^objects (.children tree)
        child (aget children idx)]
    (cond
      (instance? HATTrie child)
      (recur child buffer)

      (nil? child)
      false

      :else
      (let [^ByteBuffer child child]
        (loop [in (.flip (.duplicate child))]
          (if (.hasRemaining in)
            (let [entry-capacity (.getInt in)
                  entry-h (.getInt in)]
              (if (and (= (.remaining buffer) entry-capacity)
                       (= (.hashCode buffer) entry-h)
                       (= buffer (.limit (.slice in) entry-capacity)))
                true
                (recur (.position in (+ (.position in) entry-capacity)))))
            false))))))

(def ^:private ^:const initial-size-bytes 1024)
(def ^:private ^:const split-size-bytes (* 1024 1024))

(declare insert-element)

(defn- split-node ^crux.cache.hat.HATTrie [^ByteBuffer node ^long new-level]
  (let [new-node (->hat-trie new-level)
        in (.flip (.duplicate node))]
    (while (.hasRemaining in)
      (let [entry-capacity (.getInt in)
            entry-h (.getInt in)]
        (insert-element new-node (.limit (.slice in) entry-capacity))))
    new-node))

(defn- ^long find-size [^long size]
  (loop [capacity initial-size-bytes]
    (if (> capacity size)
      capacity
      (recur (* 2 capacity)))))

(defn- ensure-capacity ^java.nio.ByteBuffer [^ByteBuffer b ^long size]
  (if (and b (>= (.remaining b) size))
    b
    (cond-> (ByteBuffer/allocateDirect (find-size size))
      b (.put (.duplicate b)))))

(defn insert-element [^HATTrie tree ^ByteBuffer buffer]
  (if (find-element tree buffer)
    tree
    (let [idx (child-idx tree buffer)
          children ^objects (.children tree)
          child (aget children idx)]
      (if (instance? HATTrie child)
        (recur child buffer)
        (let [^ByteBuffer child child
              capacity (.remaining buffer)
              entry-size (+ capacity (* 2 Integer/BYTES))]
          (if (and child (> (+ entry-size (.capacity child)) split-size-bytes))
            (let [new-child (split-node child (inc (.level tree)))]
              (aset children idx new-child)
              (insert-element new-child buffer))
            (let [^ByteBuffer new-child (ensure-capacity child entry-size)]
              (when-not (identical? child new-child)
                (aset children idx new-child))
              (-> new-child
                  (.putInt capacity)
                  (.putInt (.hashCode buffer))
                  (.put (.duplicate buffer)))))
          tree)))))
