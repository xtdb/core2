(ns crux.datalog.hquad-tree
  (:require [clojure.string :as str]
            [crux.z-curve :as cz]
            [crux.byte-keys :as cbk]
            [crux.datalog :as d]
            [crux.datalog.parser :as dp])
  (:import [java.util Arrays ArrayList Comparator List]
           java.lang.AutoCloseable
           java.nio.ByteBuffer))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^{:tag 'long} root-idx 0)
(def ^:private ^{:tag 'long} root-level -1)
(def ^:private ^{:tag 'long} initial-nodes-capacity 128)

(declare insert-tuple insert-into-node walk-tree init-hyper-quads new-leaf var-bindings->z-range non-z-range-var-bindings)

(defn- leaf-idx? [^long idx]
  (neg? idx))

(defn- undefined-idx? [^long idx]
  (zero? idx))

(defn- decode-leaf-idx ^long [^long raw-idx]
  (dec (- raw-idx)))

(defn- encode-leaf-idx ^long [^long idx]
  (- (inc idx)))

(def ^:private z-wildcard-min-bytes (byte-array Long/BYTES (byte 0)))
(def ^:private z-wildcard-max-bytes (byte-array Long/BYTES (byte -1)))
(def ^:private z-wildcard-range [z-wildcard-min-bytes
                                 z-wildcard-max-bytes])

(defn tuple->z-address ^bytes [value]
  (cz/bit-interleave (mapv cbk/->byte-key value)))

(defn new-z-sorted-map-relation [relation-name]
  (vary-meta (d/new-sorted-map-relation cbk/unsigned-bytes-comparator relation-name)
             assoc
             'crux.datalog/insert
             (fn [this tuple]
               (assoc this (tuple->z-address tuple) tuple))
             'crux.datalog/delete
             (fn [this tuple]
               (dissoc this (tuple->z-address tuple)))
             'crux.datalog/table-filter
             (fn [this db var-bindings]
               (let [[^bytes min-z ^bytes max-z :as z-range] (var-bindings->z-range var-bindings)
                     non-z-var-bindings (non-z-range-var-bindings var-bindings)
                     dims (count var-bindings)
                     ;; TODO: Doesn't work properly yet, infinite
                     ;; loop.
                     ;; Repro from test-connection-recursion-rules,
                     ;; using longs, z-get-next-address returns litmax
                     ;; == end:
                     _ (comment

                         (seq (crux.z-curve/z-get-next-address
                               1175461566330508304
                               -4973453124906008902
                               2))
                         [-4973453124906008902,
                          -8047910470524267504]

                         (crux.z-curve/z-range-search
                          1175461566330508304
                          -4973453124906008902
                          3477408534101704484
                          2))

                     ;; after-max-z (some-> (subseq this > max-z) (first) (key))
                     ;; s ((fn step [^bytes z]
                     ;;      (reduce
                     ;;       (fn [acc [^bytes k v]]
                     ;;         (cond
                     ;;           (identical? k after-max-z)
                     ;;           (reduced acc)

                     ;;           (cz/in-z-range? min-z max-z k dims)
                     ;;           (conj acc v)

                     ;;           :else
                     ;;           (if-let [^bytes bigmin (second (cz/z-range-search-arrays min-z max-z k dims))]
                     ;;             (reduced (concat acc (step bigmin)))
                     ;;             (reduced acc))))
                     ;;       []
                     ;;       (subseq this >= z)))
                     ;;    min-z)
                     s (for [[k v] (subseq this >= min-z <= max-z)
                             :when (cz/in-z-range? min-z max-z k dims)]
                         v)]
                 (d/table-filter s db non-z-var-bindings)))))

(def ^:dynamic *default-options* {::leaf-size (* 128 1024)
                                  ::leaf-tuple-relation-factory new-z-sorted-map-relation
                                  ::post-process-children-after-split nil
                                  ::split-leaf-tuple-relation-factory nil})

(defn- non-z-range-var-bindings [var-bindings]
  (vec (for [var-binding var-bindings]
         (if (dp/logic-var? var-binding)
           (if (some (comp #{'!=} first) (:constraints (meta var-binding)))
             var-binding
             (with-meta var-binding nil))
           var-binding))))

(defn- var-bindings->z-range [var-bindings]
  (let [min+max (for [var-binding var-bindings]
                  (if (dp/logic-var? var-binding)
                    (if-let [constraints (:constraints (meta var-binding))]
                      (reduce
                       (fn [[min-z max-z] [op value]]
                         [(case op
                            (>= >) (let [value-bs (if (= '> op)
                                                    (if (int? value)
                                                      (cbk/->byte-key (inc ^long value))
                                                      (cz/inc-unsigned-bytes (cbk/->byte-key value)))
                                                    (cbk/->byte-key value))
                                         diff (.compare cbk/unsigned-bytes-comparator value-bs min-z)]
                                     (if (pos? diff)
                                       value-bs
                                       min-z))
                            = (cbk/->byte-key value)
                            min-z)
                          (case op
                            (<= <) (let [value-bs (if (= '< op)
                                                    (if (int? value)
                                                      (cbk/->byte-key (dec ^long value))
                                                      (cz/dec-unsigned-bytes (cbk/->byte-key value)))
                                                    (cbk/->byte-key value))
                                         diff (.compare cbk/unsigned-bytes-comparator value-bs max-z)]
                                     (if (pos? diff)
                                       max-z
                                       value-bs))
                            = (cbk/->byte-key value)
                            max-z)])
                       [z-wildcard-min-bytes
                        z-wildcard-max-bytes]
                       constraints)
                      [z-wildcard-min-bytes z-wildcard-max-bytes])
                    (let [value-bs (cbk/->byte-key var-binding)]
                      [value-bs value-bs])))]
    [(cz/bit-interleave (mapv first min+max))
     (cz/bit-interleave (mapv second min+max))]))

(definterface INodesAccessor
  (^ints getNodes [])
  (^void setNodes [^ints nodes])
  (^long getNodesSize [])
  (^void setNodesSize [^long size]))

(definterface IHyperQuadsAccessor
  (^long getHyperQuads [])
  (^void setHyperQuads [^long hyperQuads]))

(definterface IFreeLeafIndexAccessor
  (^long getFreeLeafIndex [])
  (^void setFreeLeafIndex [^long free-leaf-idx]))

(deftype HyperQuadTree [^:volatile-mutable ^ints nodes
                        ^:volatile-mutable ^long nodes-size
                        ^:volatile-mutable ^long hyper-quads
                        ^:volatile-mutable ^long free-leaf-idx
                        ^List leaves
                        ^String name
                        options]
  d/Relation
  (table-scan [this db]
    (walk-tree this #(d/table-scan % db) z-wildcard-range))

  (table-filter [this db var-bindings]
    (walk-tree this #(d/table-filter % db var-bindings) (var-bindings->z-range var-bindings)))

  (insert [this value]
    (doto this
      (init-hyper-quads (cz/dims->hyper-quads (count value)))
      (insert-tuple value)))

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

  INodesAccessor
  (getNodes [this]
    nodes)

  (setNodes [this nodes]
    (set! (.-nodes this) nodes))

  (getNodesSize [this]
    nodes-size)

  (setNodesSize [this size]
    (set! (.-nodes-size this) size))

  IHyperQuadsAccessor
  (getHyperQuads [this]
    hyper-quads)

  (setHyperQuads [this hyper-quads]
    (set! (.-hyper-quads this) hyper-quads))

  IFreeLeafIndexAccessor
  (getFreeLeafIndex [this]
    free-leaf-idx)

  (setFreeLeafIndex [this free-leaf-idx]
    (set! (.-free-leaf-idx this) free-leaf-idx))

  AutoCloseable
  (close [_]
    (d/try-close nodes)
    (doseq [leaf leaves]
      (d/try-close leaf))))

(defn- init-hyper-quads [^HyperQuadTree tree ^long hyper-quads]
  (when (= -1 (.getHyperQuads tree))
    (.setHyperQuads tree hyper-quads)))

(defn- root-only-tree? [^HyperQuadTree tree]
  (zero? (.getNodesSize tree)))

(defn new-hyper-quad-tree-relation
  (^crux.datalog.hquad_tree.HyperQuadTree [relation-name]
   (new-hyper-quad-tree-relation {} relation-name))
  (^crux.datalog.hquad_tree.HyperQuadTree [opts relation-name]
   (->HyperQuadTree (int-array initial-nodes-capacity) 0 -1 -1 (ArrayList.) relation-name (merge *default-options* opts))))

(defn- walk-tree [^HyperQuadTree tree leaf-fn [^bytes min-z ^bytes max-z :as z-range]]
  (let [node-vector (.getNodes tree)
        leaves ^List (.leaves tree)]
    (cond
      (empty? (.leaves tree))
      nil

      (root-only-tree? tree)
      (leaf-fn (.get leaves root-idx))

      :else
      (let [hyper-quads (.getHyperQuads tree)
            h-mask (dec hyper-quads)
            dims (cz/hyper-quads->dims hyper-quads)]
        ((fn step [^long level ^long parent-node-idx ^long min-h-mask ^long max-h-mask]
           (lazy-seq
            (let [min-h (bit-and (cz/decode-h-at-level min-z dims level) min-h-mask)
                  max-h (bit-or (cz/decode-h-at-level max-z dims level) max-h-mask)]
              (loop [h min-h
                     acc nil]
                (if (= -1 h)
                  acc
                  (let [node-idx (+ parent-node-idx h)
                        child-idx (aget node-vector node-idx)]
                    (recur (cz/inc-h-in-range min-h max-h h)
                           (if (undefined-idx? child-idx)
                             acc
                             (concat acc
                                     (if (leaf-idx? child-idx)
                                       (leaf-fn (.get leaves (decode-leaf-idx child-idx)))
                                       (step (inc level)
                                             child-idx
                                             (cz/propagate-min-h-mask h min-h min-h-mask)
                                             (cz/propagate-max-h-mask h max-h max-h-mask))))))))))))
         0 root-idx h-mask 0)))))

(defn- leaf-children-of-node [^HyperQuadTree tree ^long parent-node-idx]
  (let [leaves ^List (.leaves tree)
        hyper-quads (.getHyperQuads tree)
        node-vector (.getNodes tree)]
    (loop [h 0
           acc []]
      (if (= h hyper-quads)
        acc
        (recur (inc h)
               (let [node-idx (+ parent-node-idx h)
                     child-idx (aget node-vector node-idx)]
                 (conj acc (when-not (undefined-idx? child-idx)
                             (when (leaf-idx? child-idx)
                               (.get leaves (decode-leaf-idx child-idx)))))))))))

(defn- post-process-children-after-split [^HyperQuadTree tree leaf ^long new-node-idx]
  (when-let [post-process-children-after-split (::post-process-children-after-split (.options tree))]
    (let [leaves ^List (.leaves tree)
          node-vector (.getNodes tree)
          children (post-process-children-after-split leaf (leaf-children-of-node tree new-node-idx))]
      (dotimes [h (.getHyperQuads tree)]
        (let [node-idx (+ new-node-idx h)]
          (when-let [child-leaf (nth children h)]
            (let [leaf-idx (aget node-vector node-idx)]
              (if (undefined-idx? leaf-idx)
                (aset node-vector node-idx (encode-leaf-idx (new-leaf tree child-leaf)))
                (.set leaves (decode-leaf-idx (aget node-vector node-idx)) child-leaf)))))))))

(defn- ensure-nodes-capacity ^ints [^HyperQuadTree tree ^long capacity]
  (let [node-vector (.getNodes tree)]
    (if (>= capacity (alength node-vector))
      (doto (Arrays/copyOf node-vector (* 2 capacity))
        (->> (.setNodes tree)))
      node-vector)))

(defn- new-node ^long [^HyperQuadTree tree parent-node-idx]
  (let [root? (root-only-tree? tree)
        new-node-idx (.getNodesSize tree)
        capacity (+ (.getHyperQuads tree) new-node-idx)
        node-vector (ensure-nodes-capacity tree capacity)]
    (.setNodesSize tree capacity)
    (when-not root?
      (aset ^ints node-vector (int parent-node-idx) new-node-idx))
    new-node-idx))

(defn- remove-leaf [^HyperQuadTree tree ^long leaf-idx]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (.set leaves leaf-idx (.getFreeLeafIndex tree))
    (.setFreeLeafIndex tree leaf-idx)
    (d/try-close leaf)))

(def ^:private ^:dynamic *leaf-split?* false)

(defn- split-leaf [^HyperQuadTree tree path parent-node-idx ^long leaf-idx]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)
        new-node-idx (new-node tree parent-node-idx)]
    (binding [*leaf-split?* true]
      (doseq [tuple (d/table-scan leaf nil)]
        (insert-into-node tree path new-node-idx tuple)))
    (post-process-children-after-split tree leaf new-node-idx)
    (remove-leaf tree leaf-idx)
    new-node-idx))

(defn- insert-into-leaf [^HyperQuadTree tree path parent-node-idx leaf-idx value]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (if (< ^long (d/cardinality leaf) ^long (::leaf-size (.options tree)))
      (.set leaves leaf-idx (d/insert leaf value))
      (let [new-node-idx (split-leaf tree path parent-node-idx leaf-idx)]
        (insert-into-node tree path new-node-idx value)))
    tree))

(defn- new-leaf-relation [^HyperQuadTree tree ^String leaf-name]
  (let [leaf-tuple-relation-factory (::leaf-tuple-relation-factory (.options tree))
        leaf-tuple-relation-factory (if *leaf-split?*
                                      (or (::split-leaf-tuple-relation-factory (.options tree))
                                          leaf-tuple-relation-factory)
                                      leaf-tuple-relation-factory)]
    (leaf-tuple-relation-factory leaf-name)))

(defn- new-leaf ^long [^HyperQuadTree tree leaf-relation]
  (let [leaves ^List (.leaves tree)
        free-leaf-idx (.getFreeLeafIndex tree)
        new-leaf? (= -1 free-leaf-idx)
        leaf-idx (if new-leaf?
                   (.size leaves)
                   (do (.setFreeLeafIndex tree (.get leaves free-leaf-idx))
                       free-leaf-idx))]
    (if new-leaf?
      (.add leaves leaf-relation)
      (.set leaves leaf-idx leaf-relation))
    leaf-idx))

(defn leaf-name
  ([^HyperQuadTree tree path]
   (leaf-name (.name tree) (.getHyperQuads tree) path))
  ([name hyper-quads path]
   (str/join "/" (cons (str name "_" hyper-quads) path))))

(defn leaf-name->name+hyper-quads+path [leaf-name]
  (let [[name+hyper-quads & path] (str/split leaf-name #"/")
        [name hyper-quads] (str/split name+hyper-quads #"_")]
    [name
     (Long/parseLong hyper-quads)
     (vec (for [e path]
            (Long/parseLong e)))]))

(defn- insert-into-node [^HyperQuadTree tree path ^long parent-node-idx value]
  (let [z-address (tuple->z-address value)
        dims (cz/hyper-quads->dims (.getHyperQuads tree))
        node-vector (.getNodes tree)]
    (loop [parent-node-idx parent-node-idx
           path path]
      (let [level (count path)
            h (cz/decode-h-at-level z-address dims level)
            node-idx (+ parent-node-idx h)
            path (conj path h)]
        (if (zero? (aget node-vector node-idx))
          (let [leaf-idx (new-leaf tree (new-leaf-relation tree (leaf-name tree path)))]
            (aset node-vector (int node-idx) (encode-leaf-idx leaf-idx))
            (insert-into-leaf tree path node-idx leaf-idx value))
          (let [child-idx (aget node-vector node-idx)]
            (assert (not= root-idx child-idx))
            (if (leaf-idx? child-idx)
              (insert-into-leaf tree path node-idx (decode-leaf-idx child-idx) value)
              (recur child-idx path))))))))

(defn insert-leaf-at-path [^HyperQuadTree tree ^long hyper-quads path leaf-relation]
  (init-hyper-quads tree hyper-quads)
  (let [node-vector (.getNodes tree)
        leaf-idx (new-leaf tree leaf-relation)]
    (if (and (empty? path) (root-only-tree? tree))
      (assert (= root-idx leaf-idx))
      (do (when (root-only-tree? tree)
            (new-node tree nil))
          (loop [^long parent-node-idx root-idx
                 [^long h & path] path]
            (let [node-idx (+ parent-node-idx h)
                  node-vector ^ints (ensure-nodes-capacity tree node-idx)
                  child-idx (aget node-vector node-idx)]
              (when (leaf-idx? child-idx)
                (remove-leaf tree (decode-leaf-idx child-idx)))
              (if path
                (recur (if (or (leaf-idx? child-idx)
                               (undefined-idx? child-idx))
                         (new-node tree node-idx)
                         child-idx)
                       path)
                (aset node-vector node-idx (encode-leaf-idx leaf-idx)))))))))

(defn ensure-root-node [^HyperQuadTree tree hyper-quads]
  (when (empty? (.leaves tree))
    (init-hyper-quads tree hyper-quads)
    (let [new-leaf-idx (new-leaf tree (new-leaf-relation tree (leaf-name tree [])))]
      (assert (= root-idx new-leaf-idx)))))

(defn- insert-tuple [^HyperQuadTree tree value]
  (if (root-only-tree? tree)
    (do (ensure-root-node tree (.getHyperQuads tree))
        (insert-into-leaf tree [] nil root-idx value))
    (insert-into-node tree [] root-idx value)))
