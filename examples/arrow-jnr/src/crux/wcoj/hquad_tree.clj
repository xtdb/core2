(ns crux.wcoj.hquad-tree
  (:require [crux.datalog :as cd]
            [crux.z-curve :as cz]
            [crux.byte-keys :as cbk]
            [crux.wcoj :as wcoj])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           org.apache.arrow.vector.complex.FixedSizeListVector
           org.apache.arrow.vector.IntVector
           org.apache.arrow.vector.types.pojo.FieldType
           org.apache.arrow.vector.types.Types$MinorType
           [java.util ArrayList List]
           java.lang.AutoCloseable
           java.nio.ByteBuffer))

(def ^:private ^BufferAllocator
  allocator (RootAllocator. Long/MAX_VALUE))

(def ^:dynamic ^{:tag 'long} *leaf-size* (* 128 1024))
(def ^:private ^{:tag 'long} root-idx 0)
(def ^:private ^{:tag 'long} root-level -1)

(def ^:dynamic *leaf-tuple-relation-factory* wcoj/new-sorted-set-relation)

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
                  (if (cd/logic-var? var-binding)
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

(deftype HyperQuadTree [^:volatile-mutable ^FixedSizeListVector nodes
                        ^List leaves
                        ^String name]
  wcoj/Relation
  (table-scan [this db]
    (walk-tree this nodes #(wcoj/table-scan % db) z-wildcard-range))

  (table-filter [this db var-bindings]
    (walk-tree this nodes #(wcoj/table-filter % db var-bindings) (var-bindings->z-range var-bindings)))

  (insert [this value]
    (when (nil? nodes)
      (let [dims (count value)
            hyper-quads (cz/dims->hyper-quads dims)]
        (set! (.-nodes this) (doto (FixedSizeListVector/empty "" hyper-quads allocator)
                               (.setInitialCapacity 0)
                               (.setValueCount 0)
                               (.addOrGetVector (FieldType/nullable (.getType Types$MinorType/INT)))))))
    (do (insert-tuple this nodes value)
        this))

  (delete [this value]
    (walk-tree this nodes #(do (wcoj/delete % value) nil) (var-bindings->z-range value))
    this)

  (cardinality [this]
    (reduce + (map wcoj/cardinality leaves)))

  AutoCloseable
  (close [_]
    (wcoj/try-close nodes)
    (doseq [leaf leaves]
      (wcoj/try-close leaf))))

(defn new-hyper-quad-tree-relation ^crux.wcoj.hquad_tree.HyperQuadTree [relation-name]
  (->HyperQuadTree nil (ArrayList.) relation-name))

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

(defn- split-leaf [^HyperQuadTree tree ^FixedSizeListVector nodes level parent-node-idx leaf-idx]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)
        root? (root-only-tree? nodes)]
    (let [new-node-idx (.startNewValue nodes (.getValueCount nodes))
          new-level (inc level)
          node-vector ^IntVector (.getDataVector nodes)]
      (.setValueCount nodes (inc (.getValueCount nodes)))
      (when-not root?
        (.setSafe node-vector (int parent-node-idx) new-node-idx))
      (doseq [tuple (wcoj/table-scan leaf nil)]
        (insert-into-node tree nodes new-level new-node-idx tuple))
      (.set leaves leaf-idx nil)
      (wcoj/try-close leaf)
      [new-level new-node-idx])))

(defn- insert-into-leaf [^HyperQuadTree tree ^FixedSizeListVector nodes level parent-node-idx leaf-idx value]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (if (< (wcoj/cardinality leaf) *leaf-size*)
      (wcoj/insert leaf value)
      (let [[new-level new-node-idx] (split-leaf tree nodes level parent-node-idx leaf-idx)]
        (insert-into-node tree nodes new-level new-node-idx value)))
    tree))

(defn- new-leaf ^long [^HyperQuadTree tree ^String leaf-name]
  (let [leaves ^List (.leaves tree)
        free-leaf-idx (.indexOf leaves nil)
        new-leaf? (= -1 free-leaf-idx)
        leaf-idx (if new-leaf?
                   (.size leaves)
                   free-leaf-idx)]
    (if new-leaf?
      (.add leaves (*leaf-tuple-relation-factory* leaf-name))
      (.set leaves leaf-idx (*leaf-tuple-relation-factory* leaf-name)))
    leaf-idx))

(defn- leaf-name
  ([^HyperQuadTree tree ^long dims]
   (str (.name tree) "_" dims))
  ([^HyperQuadTree tree ^long dims ^long prefix-z]
   (str (leaf-name tree dims) "_" (Long/toHexString prefix-z))))

(defn- insert-into-node [^HyperQuadTree tree ^FixedSizeListVector nodes level parent-node-idx value]
  (let [leaves ^List (.leaves tree)
        z-address (tuple->z-address value)
        dims (cz/hyper-quads->dims (.getListSize nodes))
        node-vector ^IntVector (.getDataVector nodes)]
    (loop [level level
           parent-node-idx parent-node-idx]
      (let [h (cz/decode-h-at-level z-address dims level)
            node-idx (+ parent-node-idx h)]
        (if (.isNull node-vector node-idx)
          (let [leaf-idx (new-leaf tree (leaf-name tree dims (cz/prefix-z-at-level z-address dims level)))]
            (.setSafe node-vector (int node-idx) (encode-leaf-idx leaf-idx))
            (insert-into-leaf tree nodes level node-idx leaf-idx value))
          (let [child-idx (.get node-vector node-idx)]
            (assert (not= root-idx child-idx))
            (if (leaf-idx? child-idx)
              (insert-into-leaf tree nodes level node-idx (decode-leaf-idx child-idx) value)
              (recur (inc level) child-idx))))))))

(defn- insert-tuple [^HyperQuadTree tree ^FixedSizeListVector nodes value]
  (if (root-only-tree? nodes)
    (do (when (empty? (.leaves tree))
          (let [new-leaf-idx (new-leaf tree (leaf-name tree (count value)))]
            (assert (= root-idx new-leaf-idx))))
        (insert-into-leaf tree nodes root-level nil root-idx value))
    (insert-into-node tree nodes 0 root-idx value)))
