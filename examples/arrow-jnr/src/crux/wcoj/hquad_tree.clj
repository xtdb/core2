(ns crux.wcoj.hquad-tree
  (:require [crux.z-curve :as cz]
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

(defn- dims->hyper-quads ^long [^long dims]
  (max (bit-shift-left 2 (dec dims)) 1))

(defn- root-only-tree? [^FixedSizeListVector nodes]
  (zero? (.getValueCount nodes)))

(defn- leaf-idx? [^long idx]
  (neg? idx))

(defn- decode-leaf-idx ^long [^long raw-idx]
  (dec (- raw-idx)))

(defn- encode-leaf-idx ^long [^long idx]
  (- (inc idx)))

(deftype HyperQuadTree [^:volatile-mutable ^long dims
                        ^:volatile-mutable ^long hyper-quads
                        ^:volatile-mutable ^FixedSizeListVector nodes
                        ^String name
                        ^List leaves]
  wcoj/Relation
  (table-scan [this db]
    (walk-tree this dims nodes #(wcoj/table-scan % db)))

  (table-filter [this db var-bindings]
    (walk-tree this dims nodes #(wcoj/table-filter % db var-bindings)))

  (insert [this value]
    (when-not (nat-int? dims)
      (let [dims (count value)
            hyper-quads (dims->hyper-quads dims)]
        (set! (.-dims this) dims)
        (set! (.-hyper-quads this) hyper-quads)
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

(defn- tuple->z-address ^long [value]
  (.getLong (ByteBuffer/wrap (cz/bit-interleave (map cbk/->byte-key value)))))

(defn- decode-h-at-level ^long [^long z-address ^long hyper-quads ^long level]
  (let [shift (- Long/SIZE (* (inc level) hyper-quads))]
    (assert (nat-int? shift))
    (bit-and (unsigned-bit-shift-right z-address shift) (dec hyper-quads))))

(defn- encode-z-prefix-level ^long [^long z-prefix ^long hyper-quads ^long level ^long h]
  (let [shift (- Long/SIZE (* (inc level) hyper-quads))]
    (assert (nat-int? shift))
    (bit-or z-prefix (bit-shift-left h shift))))

(defn- walk-tree [^HyperQuadTree tree ^long dims ^FixedSizeListVector nodes leaf-fn]
  (let [leaves ^List (.leaves tree)]
    (cond
      (empty? (.leaves tree))
      nil

      (root-only-tree? nodes)
      (leaf-fn (.get leaves root-idx))

      :else
      (let [hyper-quads (dims->hyper-quads dims)
            node-vector ^IntVector (.getDataVector nodes)]
        ((fn step [level parent-node-idx z-prefix]
           (lazy-seq
            (loop [h 0
                   acc nil]
              (if (< h hyper-quads)
                (let [node-idx (+ parent-node-idx h)]
                  (recur (inc h)
                         (if (.isNull node-vector node-idx)
                           acc
                           (let [child-idx (.get node-vector node-idx)
                                 child-z-prefix (encode-z-prefix-level z-prefix hyper-quads level h)]
                             (concat acc
                                     (if (leaf-idx? child-idx)
                                       (leaf-fn (.get leaves (decode-leaf-idx child-idx)))
                                       (step (inc level) child-idx child-z-prefix)))))))
                acc))))
         0 root-idx 0)))))

(defn new-hyper-quad-tree-relation ^crux.wcoj.hquad_tree.HyperQuadTree [relation-name]
  (->HyperQuadTree -1 -1 nil relation-name (ArrayList.)))

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
            (assert (= root-idx (new-leaf tree))))
          (insert-into-leaf tree dims nodes nil root-idx value))
      (let [z-address (tuple->z-address value)
            hyper-quads (dims->hyper-quads dims)
            node-vector ^IntVector (.getDataVector nodes)]
        (loop [level 0
               parent-node-idx 0]
          (let [h (decode-h-at-level z-address hyper-quads level)
                node-idx (+ parent-node-idx h)]
            (if (.isNull node-vector node-idx)
              (let [leaf-idx (new-leaf tree)]
                (.setSafe node-vector (int node-idx) (encode-leaf-idx leaf-idx))
                (insert-into-leaf tree dims nodes node-idx leaf-idx value))
              (let [child-idx (.get node-vector node-idx)]
                (assert (not (zero? child-idx)))
                (if (leaf-idx? child-idx)
                  (insert-into-leaf tree dims nodes node-idx (decode-leaf-idx child-idx) value)
                  (recur (inc level) child-idx))))))))))
