(ns crux.datalog.hquad-tree
  (:require [clojure.string :as str]
            [crux.z-curve :as cz]
            [crux.byte-keys :as cbk]
            [crux.datalog :as d]
            [crux.datalog.parser :as dp])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           org.apache.arrow.vector.complex.FixedSizeListVector
           org.apache.arrow.vector.IntVector
           org.apache.arrow.vector.types.pojo.FieldType
           org.apache.arrow.vector.types.Types$MinorType
           [java.util ArrayList List]
           java.lang.AutoCloseable
           java.lang.reflect.Field
           java.nio.ByteBuffer))

(set! *unchecked-math* :warn-on-boxed)

(def ^BufferAllocator
  default-allocator (RootAllocator. Long/MAX_VALUE))

(def ^:dynamic *default-options* {:leaf-size (* 128 1024)
                                  :leaf-tuple-relation-factory d/new-sorted-set-relation
                                  :post-process-children-after-split nil})

(def ^:private ^{:tag 'long} root-idx 0)
(def ^:private ^{:tag 'long} root-level -1)

(declare insert-tuple insert-into-node walk-tree)

(defn- root-only-tree? [^FixedSizeListVector nodes]
  (zero? (.getValueCount nodes)))

(defn- leaf-idx? [^long idx]
  (neg? idx))

(defn- decode-leaf-idx ^long [^long raw-idx]
  (dec (- raw-idx)))

(defn- encode-leaf-idx ^long [^long idx]
  (- (inc idx)))

(def ^:private z-wildcard-range [0 -1])
(def ^:private z-wildcard-min-bytes (byte-array Long/BYTES (byte 0)))
(def ^:private z-wildcard-max-bytes (byte-array Long/BYTES (byte -1)))

(defn- tuple->z-address ^long [value]
  (.getLong (ByteBuffer/wrap (cz/bit-interleave (map cbk/->byte-key value)))))

(defn- var-bindings->z-range [var-bindings]
  (let [min+max (for [var-binding var-bindings]
                  (if (dp/logic-var? var-binding)
                    (if-let [constraints (:constraints (meta var-binding))]
                      (let [[min-z max-z] (reduce
                                           (fn [[min-z max-z] [op value]]
                                             [(case op
                                                (>= >) (if min-z
                                                         (let [diff (compare value min-z)]
                                                           (if (pos? diff)
                                                             value
                                                             min-z))
                                                         value)
                                                = value
                                                min-z)
                                              (case op
                                                (<= <) (if max-z
                                                         (let [diff (compare value max-z)]
                                                           (if (pos? diff)
                                                             max-z
                                                             value))
                                                         value)
                                                = value
                                                max-z)])
                                           [nil nil]
                                           constraints)]
                        [(or min-z z-wildcard-min-bytes)
                         (or max-z z-wildcard-max-bytes)])
                      [z-wildcard-min-bytes z-wildcard-max-bytes])
                    [var-binding var-binding]))]
    [(tuple->z-address (map first min+max))
     (tuple->z-address (map second min+max))]))

(defn- new-nodes-list [allocator hyper-quads]
  (doto (FixedSizeListVector/empty "" hyper-quads allocator)
    (.setInitialCapacity 0)
    (.setValueCount 0)
    (.addOrGetVector (FieldType/nullable (.getType Types$MinorType/INT)))))

(deftype HyperQuadTree [^:volatile-mutable ^FixedSizeListVector nodes
                        ^List leaves
                        ^String name
                        ^BufferAllocator allocator
                        options]
  d/Relation
  (table-scan [this db]
    (walk-tree this nodes #(d/table-scan % db) z-wildcard-range))

  (table-filter [this db var-bindings]
    (walk-tree this nodes #(d/table-filter % db var-bindings) (var-bindings->z-range var-bindings)))

  (insert [this value]
    (when (nil? nodes)
      (let [dims (count value)
            hyper-quads (cz/dims->hyper-quads dims)]
        (set! (.-nodes this) (new-nodes-list allocator hyper-quads))))
    (do (insert-tuple this nodes value)
        this))

  (delete [this value]
    (let [leaves ^List (.leaves this)]
      (dotimes [n (.size leaves)]
        (let [leaf (.get leaves n)]
          (when leaf
            (.set leaves n (d/delete leaf value))))))
    this)

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    (reduce + (map d/cardinality leaves)))

  AutoCloseable
  (close [_]
    (d/try-close nodes)
    (doseq [leaf leaves]
      (d/try-close leaf))))

(defn new-hyper-quad-tree-relation
  (^crux.datalog.hquad_tree.HyperQuadTree [relation-name]
   (new-hyper-quad-tree-relation default-allocator {} relation-name))
  (^crux.datalog.hquad_tree.HyperQuadTree [allocator opts relation-name]
   (->HyperQuadTree nil (ArrayList.) relation-name allocator (merge *default-options* opts))))

(defn- walk-tree [^HyperQuadTree tree ^FixedSizeListVector nodes leaf-fn [^long min-z ^long max-z :as z-range]]
  (let [leaves ^List (.leaves tree)]
    (cond
      (empty? (.leaves tree))
      nil

      (root-only-tree? nodes)
      (leaf-fn (.get leaves root-idx))

      :else
      (let [hyper-quads (.getListSize nodes)
            h-mask (dec hyper-quads)
            dims (cz/hyper-quads->dims hyper-quads)
            node-vector ^IntVector (.getDataVector nodes)]
        ((fn step [^long level ^long parent-node-idx ^long min-h-mask ^long max-h-mask]
           (lazy-seq
            (let [min-h (bit-and (cz/decode-h-at-level min-z dims level) min-h-mask)
                  max-h (bit-or (cz/decode-h-at-level max-z dims level) max-h-mask)]
              (loop [h min-h
                     acc nil]
                (if (= -1 h)
                  acc
                  (let [node-idx (+ parent-node-idx h)]
                    (recur (cz/inc-h-in-range min-h max-h h)
                           (if (.isNull node-vector node-idx)
                             acc
                             (let [child-idx (.get node-vector node-idx)]
                               (concat acc
                                       (if (leaf-idx? child-idx)
                                         (leaf-fn (.get leaves (decode-leaf-idx child-idx)))
                                         (step (inc level)
                                               child-idx
                                               (cz/propagate-min-h-mask h min-h min-h-mask)
                                               (cz/propagate-max-h-mask h max-h max-h-mask)))))))))))))
         0 root-idx h-mask 0)))))

(defn- leaf-children-of-node [^HyperQuadTree tree ^FixedSizeListVector nodes ^long parent-node-idx]
  (let [leaves ^List (.leaves tree)
        hyper-quads (.getListSize nodes)
        node-vector ^IntVector (.getDataVector nodes)]
    (loop [h 0
           acc []]
      (if (= h hyper-quads)
        acc
        (recur (inc h)
               (let [node-idx (+ parent-node-idx h)]
                 (conj acc (when-not (.isNull node-vector node-idx)
                             (let [child-idx (.get node-vector node-idx)]
                               (when (leaf-idx? child-idx)
                                 (.get leaves (decode-leaf-idx child-idx))))))))))))

(defn- post-process-children-after-split [^HyperQuadTree tree ^FixedSizeListVector nodes ^long new-node-idx]
  (when-let [post-process-children-after-split (:post-process-children-after-split (.options tree))]
    (let [leaves ^List (.leaves tree)
          hyper-quads (.getListSize nodes)
          node-vector ^IntVector (.getDataVector nodes)
          children (post-process-children-after-split (leaf-children-of-node tree nodes new-node-idx))]
      (dotimes [h hyper-quads]
        (let [node-idx (+ new-node-idx h)]
          (when-let [child-leaf (nth children h)]
            (.set leaves (decode-leaf-idx (.get node-vector node-idx)) child-leaf)))))))

(defn- new-node ^long [^FixedSizeListVector nodes parent-node-idx]
  (let [root? (root-only-tree? nodes)
        new-node-idx (.startNewValue nodes (.getValueCount nodes))
        node-vector ^IntVector (.getDataVector nodes)]
    (.setValueCount nodes (inc (.getValueCount nodes)))
    (when-not root?
      (.setSafe node-vector (int parent-node-idx) new-node-idx))
    new-node-idx))

(defn- remove-leaf [^HyperQuadTree tree leaf-idx]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (.set leaves leaf-idx nil)
    (d/try-close leaf)))

(defn- split-leaf [^HyperQuadTree tree ^FixedSizeListVector nodes path parent-node-idx leaf-idx]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)
        new-node-idx (new-node nodes parent-node-idx)]
    (doseq [tuple (d/table-scan leaf nil)]
      (insert-into-node tree nodes path new-node-idx tuple))
    (post-process-children-after-split tree nodes new-node-idx)
    (remove-leaf tree leaf-idx)
    new-node-idx))

(defn- insert-into-leaf [^HyperQuadTree tree ^FixedSizeListVector nodes path parent-node-idx leaf-idx value]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (if (< ^long (d/cardinality leaf) ^long (:leaf-size (.options tree)))
      (.set leaves leaf-idx (d/insert leaf value))
      (let [new-node-idx (split-leaf tree nodes path parent-node-idx leaf-idx)]
        (insert-into-node tree nodes path new-node-idx value)))
    tree))

(defn- new-leaf-relation [^HyperQuadTree tree ^String leaf-name]
  (let [leaf-tuple-relation-factory (:leaf-tuple-relation-factory (.options tree))]
    (leaf-tuple-relation-factory leaf-name)))

(defn- new-leaf ^long [^HyperQuadTree tree leaf-relation]
  (let [leaves ^List (.leaves tree)
        free-leaf-idx (.indexOf leaves nil)
        new-leaf? (= -1 free-leaf-idx)
        leaf-idx (if new-leaf?
                   (.size leaves)
                   free-leaf-idx)]
    (if new-leaf?
      (.add leaves leaf-relation)
      (.set leaves leaf-idx leaf-relation))
    leaf-idx))

(defn leaf-name [^HyperQuadTree tree hyper-quads path]
  (str/join "/" (cons (str (.name tree) "_" hyper-quads) path)))

(defn leaf-name->name+hyper-quads+path [leaf-name]
  (let [[name+hyper-quads & path] (str/split leaf-name #"/")
        [name hyper-quads] (str/split name+hyper-quads #"_")]
    [name
     (Long/parseLong hyper-quads)
     (vec (for [e path]
            (Long/parseLong e)))]))

(defn- insert-into-node [^HyperQuadTree tree ^FixedSizeListVector nodes path parent-node-idx value]
  (let [z-address (tuple->z-address value)
        hyper-quads (.getListSize nodes)
        dims (cz/hyper-quads->dims hyper-quads)
        node-vector ^IntVector (.getDataVector nodes)]
    (loop [parent-node-idx ^long parent-node-idx
           path path]
      (let [level (count path)
            h (cz/decode-h-at-level z-address dims level)
            node-idx (+ parent-node-idx h)
            path (conj path h)]
        (if (.isNull node-vector node-idx)
          (let [leaf-idx (new-leaf tree (new-leaf-relation tree (leaf-name tree hyper-quads path)))]
            (.setSafe node-vector (int node-idx) (encode-leaf-idx leaf-idx))
            (insert-into-leaf tree nodes path node-idx leaf-idx value))
          (let [child-idx (.get node-vector node-idx)]
            (assert (not= root-idx child-idx))
            (if (leaf-idx? child-idx)
              (insert-into-leaf tree nodes path node-idx (decode-leaf-idx child-idx) value)
              (recur child-idx path))))))))

(def ^:private ^java.lang.reflect.Field
  nodes-field (doto (.getDeclaredField HyperQuadTree "nodes")
                (.setAccessible true)))

(defn- get-tree-nodes ^org.apache.arrow.vector.complex.FixedSizeListVector [^HyperQuadTree tree hyper-quads]
  (or (.get nodes-field tree)
      (doto (new-nodes-list (.allocator tree) hyper-quads)
        (->> (.set nodes-field tree)))))

(defn insert-leaf-at-path [^HyperQuadTree tree hyper-quads path leaf-relation]
  (let [nodes (get-tree-nodes tree hyper-quads)
        node-vector ^IntVector (.getDataVector nodes)
        leaf-idx (new-leaf tree leaf-relation)]
    (if (and (empty? path) (root-only-tree? nodes))
      (assert (= root-idx leaf-idx))
      (loop [^long parent-node-idx root-idx
             [^long h & path] path]
        (let [node-idx (+ parent-node-idx h)]
          (let [child-idx (if (.isNull node-vector node-idx)
                            (new-node nodes node-idx)
                            (let [child-idx (.get node-vector node-idx)]
                              (assert (not= root-idx child-idx))
                              (if (leaf-idx? child-idx)
                                (let [leaf-idx (decode-leaf-idx child-idx)]
                                  (remove-leaf tree leaf-idx)
                                  (new-node nodes node-idx))
                                child-idx)))]
            (if path
              (recur child-idx path)
              (.setSafe node-vector (int child-idx) (encode-leaf-idx leaf-idx)))))))))


(defn- insert-tuple [^HyperQuadTree tree ^FixedSizeListVector nodes value]
  (if (root-only-tree? nodes)
    (do (when (empty? (.leaves tree))
          (let [new-leaf-idx (new-leaf tree (new-leaf-relation tree (leaf-name tree (.getListSize nodes) [])))]
            (assert (= root-idx new-leaf-idx))))
        (insert-into-leaf tree nodes [] nil root-idx value))
    (insert-into-node tree nodes [] root-idx value)))
