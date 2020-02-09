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

;; Simple N-dimensional quad tree in Rust:
;; https://github.com/reem/rust-n-tree/blob/master/src/lib.rs
;; Python implementation:
;; https://github.com/karimbahgat/Pyqtree/blob/master/pyqtree.py

(def ^:private ^BufferAllocator
  allocator (RootAllocator. Long/MAX_VALUE))

(defn- try-close [c]
  (when (instance? AutoCloseable c)
    (.close ^AutoCloseable c)))

(def ^:dynamic ^{:tag 'long} *leaf-size* (* 128 1024))
(def ^:private ^{:tag 'long} root-idx 0)

(def ^:dynamic *internal-leaf-tuple-relation-factory* wcoj/new-sorted-set-relation)

(declare insert-tuple walk-tree)

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

(deftype HyperQuadTree [^:volatile-mutable ^long dims
                        ^:volatile-mutable ^FixedSizeListVector nodes
                        ^String name
                        ^List leaves]
  wcoj/Relation
  (table-scan [this db]
    (walk-tree this dims nodes #(wcoj/table-scan % db) z-wildcard-range))

  (table-filter [this db var-bindings]
    (walk-tree this dims nodes #(wcoj/table-filter % db var-bindings) (var-bindings->z-range var-bindings)))

  (insert [this value]
    (when (neg? dims)
      (let [dims (count value)
            hyper-quads (cz/dims->hyper-quads dims)]
        (set! (.-dims this) dims)
        (set! (.-nodes this) (doto (FixedSizeListVector/empty "" hyper-quads allocator)
                               (.setInitialCapacity 0)
                               (.setValueCount 0)
                               (.addOrGetVector (FieldType/nullable (.getType Types$MinorType/INT)))))))
    (do (insert-tuple this dims nodes value)
        this))

  (delete [this value]
    (doseq [leaf leaves
            :when (some? leaf)]
      (wcoj/delete leaf value))
    this)

  (cardinality [this]
    (reduce + (map wcoj/cardinality leaves)))

  AutoCloseable
  (close [_]
    (try-close nodes)
    (doseq [leaf leaves]
      (try-close leaf))))

(defn new-hyper-quad-tree-relation ^crux.wcoj.hquad_tree.HyperQuadTree [relation-name]
  (->HyperQuadTree -1 nil relation-name (ArrayList.)))

(defn- walk-tree [^HyperQuadTree tree dims ^FixedSizeListVector nodes leaf-fn [^long min-z ^long max-z :as z-range]]
  (let [leaves ^List (.leaves tree)
        h-mask (dec (cz/dims->hyper-quads dims))]
    (cond
      (empty? (.leaves tree))
      nil

      (root-only-tree? nodes)
      (leaf-fn (.get leaves root-idx))

      :else
      (let [node-vector ^IntVector (.getDataVector nodes)]
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

(declare insert-tuple)

(defn- split-leaf [^HyperQuadTree tree dims ^FixedSizeListVector nodes parent-node-idx leaf-idx]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (try
      (let [new-node-idx (.startNewValue nodes (.getValueCount nodes))
            node-vector ^IntVector (.getDataVector nodes)]
        (.setValueCount nodes (inc (.getValueCount nodes)))
        (when parent-node-idx
          (.setSafe node-vector (int parent-node-idx) new-node-idx))
        (.set leaves leaf-idx nil)
        (doseq [tuple (wcoj/table-scan leaf nil)]
          (insert-tuple tree dims nodes tuple)))
      (finally
        (try-close leaf)))))

(defn- insert-into-leaf [^HyperQuadTree tree dims ^FixedSizeListVector nodes parent-node-idx leaf-idx value]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (if (< (wcoj/cardinality leaf) *leaf-size*)
      (wcoj/insert leaf value)
      (doto tree
        (split-leaf dims nodes parent-node-idx leaf-idx)
        (insert-tuple dims nodes value)))))

(defn- new-leaf ^long [^HyperQuadTree tree]
  (let [leaves ^List (.leaves tree)
        free-leaf-idx (.indexOf leaves nil)
        leaf-idx (if (= -1 free-leaf-idx)
                   (.size leaves)
                   free-leaf-idx)]
    (if (= -1 free-leaf-idx)
      (.add leaves (*internal-leaf-tuple-relation-factory* (.name tree)))
      (.set leaves leaf-idx (*internal-leaf-tuple-relation-factory* (.name tree))))
    leaf-idx))

(defn- insert-tuple [^HyperQuadTree tree ^long dims ^FixedSizeListVector nodes value]
  (let [leaves ^List (.leaves tree)]
    (if (root-only-tree? nodes)
      (do (when (empty? leaves)
            (let [new-leaf-idx (new-leaf tree)]
              (assert (= root-idx new-leaf-idx))))
          (insert-into-leaf tree dims nodes nil root-idx value))
      (let [z-address (tuple->z-address value)
            node-vector ^IntVector (.getDataVector nodes)]
        (loop [level 0
               parent-node-idx 0]
          (let [h (cz/decode-h-at-level z-address dims level)
                node-idx (+ parent-node-idx h)]
            (if (.isNull node-vector node-idx)
              (let [leaf-idx (new-leaf tree)]
                (.setSafe node-vector (int node-idx) (encode-leaf-idx leaf-idx))
                (insert-into-leaf tree dims nodes node-idx leaf-idx value))
              (let [child-idx (.get node-vector node-idx)]
                (assert (not= root-idx child-idx))
                (if (leaf-idx? child-idx)
                  (insert-into-leaf tree dims nodes node-idx (decode-leaf-idx child-idx) value)
                  (recur (inc level) child-idx))))))))))
