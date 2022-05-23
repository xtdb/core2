(ns core2.vector.indirect
  (:require [core2.types :as ty]
            [core2.util :as util])
  (:import core2.DenseUnionUtil
           core2.types.LegType
           [core2.vector IIndirectRelation IIndirectVector IListElementCopier IListReader IRowCopier IStructReader IVectorWriter]
           [java.util LinkedHashMap Map]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector FieldVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector ListVector StructVector]
           [org.apache.arrow.vector.types.pojo ArrowType$Struct ArrowType$Union Field]))

(declare ^core2.vector.IIndirectVector ->direct-vec
         ->IndirectVector)

(defrecord NullIndirectVector []
  IIndirectVector
  (isPresent [_ _] false)
  (rowCopier [_ w]
    (reify IRowCopier
      (copyRow [_ _]))))

(defrecord StructReader [^ValueVector v]
  IStructReader
  (structKeys [_]
    (letfn [(struct-keys [^Field field]
              (let [arrow-type (.getType field)]
                (cond
                  (instance? ArrowType$Struct arrow-type)
                  (->> (.getChildren field)
                       (into #{} (map #(.getName ^Field %))))

                  (instance? ArrowType$Union arrow-type)
                  (into #{} (mapcat struct-keys) (.getChildren field)))))]
      (struct-keys (.getField v))))

  (readerForKey [_ col-name]
    (letfn [(reader-for-key [^FieldVector v, ^String col-name]
              ;; TODO have only implemented a fraction of the required methods thus far
              (cond
                (instance? StructVector v)
                (if-let [child-vec (.getChild ^StructVector v col-name ValueVector)]
                  (->direct-vec child-vec)
                  (->NullIndirectVector))

                (instance? DenseUnionVector v)
                (let [^DenseUnionVector v v
                      rdrs (mapv #(reader-for-key % col-name) (.getChildrenFromFields v))]
                  (reify IIndirectVector
                    (isPresent [_ idx]
                      ;; TODO `(.getOffset v idx)` rather than just `idx`?
                      ;; haven't made a test fail with it yet, either way.
                      (.isPresent ^IIndirectVector (nth rdrs (.getTypeId v idx)) idx))

                    (rowCopier [_ivec w]
                      (let [copiers (mapv #(.rowCopier ^IIndirectVector % w) rdrs)]
                        (reify IRowCopier
                          (copyRow [_ idx]
                            (.copyRow ^IRowCopier (nth copiers (.getTypeId v idx))
                                      (.getOffset v idx))))))))

                :else (->NullIndirectVector)))]

      (reader-for-key v col-name))))

(definterface IAbstractListVector
  (^int getElementStartIndex [^int idx])
  (^int getElementEndIndex [^int idx])
  (^org.apache.arrow.vector.ValueVector getVector [])
  (^org.apache.arrow.vector.ValueVector getDataVector []))

(defn- ->list-reader [^ValueVector v]
  (letfn [(->list-reader* [^IAbstractListVector alv]
            (let [list-vec (.getVector alv)
                  data-vec (->direct-vec (.getDataVector alv))]
              (reify IListReader
                (isPresent [_ idx] (not (.isNull list-vec idx)))
                (getElementStartIndex [_ idx] (.getElementStartIndex alv idx))
                (getElementEndIndex [_ idx] (.getElementEndIndex alv idx))
                (elementCopier [_ w]
                  (let [copier (.rowCopier data-vec w)]
                    (reify IListElementCopier
                      (copyElement [_ idx n]
                        (let [copy-idx (+ (.getElementStartIndex alv idx) n)]
                          (if (or (neg? n) (>= copy-idx (.getElementEndIndex alv idx)))
                            (throw (IndexOutOfBoundsException.))
                            (.copyRow copier copy-idx))))))))))]
    (cond

      (instance? ListVector v)
      (let [^ListVector v v]
        (->list-reader* (reify IAbstractListVector
                          (getElementStartIndex [_ idx] (.getElementStartIndex v idx))
                          (getElementEndIndex [_ idx] (.getElementEndIndex v idx))
                          (getVector [_] v)
                          (getDataVector [_] (.getDataVector v)))))

      (instance? FixedSizeListVector v)
      (let [^FixedSizeListVector v v]
        (->list-reader* (reify IAbstractListVector
                          (getElementStartIndex [_ idx] (.getElementStartIndex v idx))
                          (getElementEndIndex [_ idx] (.getElementEndIndex v idx))
                          (getVector [_] v)
                          (getDataVector [_] (.getDataVector v)))))

      (instance? DenseUnionVector v)
      (let [^DenseUnionVector v v
            rdrs (mapv ->list-reader (.getChildrenFromFields v))]
        (reify IListReader
          (isPresent [_ idx]
            (.isPresent ^IListReader (nth rdrs (.getTypeId v idx)) (.getOffset v idx)))
          (getElementStartIndex [_ idx]
            (let [^IListReader rdr (nth rdrs (.getTypeId v idx))]
              (.getElementStartIndex rdr idx)))
          (getElementEndIndex [_ idx]
            (let [^IListReader rdr (nth rdrs (.getTypeId v idx))]
              (.getElementEndIndex rdr idx)))
          (elementCopier [this w]
            (let [copiers (mapv #(some-> ^IListReader % (.elementCopier w)) rdrs)
                  !null-writer (delay (.writerForType (.asDenseUnion w) LegType/NULL))]
              (reify IListElementCopier
                (copyElement [_ idx n]
                  (let [^IListElementCopier copier (nth copiers (.getTypeId v idx))]
                    (if (.isPresent this idx)
                      (.copyElement copier (.getOffset v idx) n)
                      (doto ^IVectorWriter @!null-writer (.startValue) (.endValue))))))))))

      :else
      (reify IListReader
        (isPresent [_ _idx] false)
        (getElementStartIndex [_ _idx] (throw (UnsupportedOperationException. "Not implemented")))
        (getElementEndIndex [_ _idx] (throw (UnsupportedOperationException. "Not implemented")))
        (elementCopier [_ _w]
          (reify IListElementCopier
            (copyElement [_ _idx _n]
              (throw (UnsupportedOperationException. "Not implemented")))))))))

(defrecord DirectVector [^ValueVector v, ^String name]
  IIndirectVector
  (isPresent [_ _] true)
  (getVector [_] v)
  (getIndex [_ idx] idx)
  (getName [_] name)
  (getValueCount [_] (.getValueCount v))

  (withName [_ name] (->DirectVector v name))
  (select [_ idxs] (->IndirectVector v name idxs))

  (copyTo [_ out-vec]
    ;; we'd like to use .getTransferPair here but DUV is broken again
    ;; - it doesn't pass the fieldType through so you get a DUV with empty typeIds
    (doto (.makeTransferPair v out-vec)
      (.splitAndTransfer 0 (.getValueCount v)))

    (DirectVector. out-vec name))

  (rowCopier [_ w] (.rowCopier w v))
  (structReader [_] (->StructReader v))
  (listReader [_] (->list-reader v)))

(defn compose-selection
  "Returns the composition of the selections sel1, sel2 which when applied to a vector will be the same as (select (select iv sel1) sel2).

  Use to avoid intermediate vector allocations.

  A selection looks like this: [3, 1, 2, 0] which when applied to a vector, will yield a new vector [vec[3], vec[1], vec[2], vec[0]].

  Selections are composed to form a new selection.

  composing [3, 1, 2, 0] and [2, 2, 0] => [1, 1, 3]

  See also: IIndirectVector .select"
  ^ints [^ints sel1 ^ints sel2]
  (let [new-left (int-array (alength sel2))]
    (dotimes [idx (alength sel2)]
      (aset new-left idx (aget sel1 (aget sel2 idx))))
    new-left))

(defrecord IndirectVector [^ValueVector v, ^String col-name, ^ints idxs]
  IIndirectVector
  (getVector [_] v)
  (getIndex [_ idx] (aget idxs idx))
  (getName [_] col-name)
  (getValueCount [_] (alength idxs))

  (withName [_ col-name] (IndirectVector. v col-name idxs))

  (select [this new-idxs]
    (IndirectVector. v col-name (compose-selection (.idxs this) new-idxs)))

  (copyTo [_ out-vec]
    (.clear out-vec)

    (if (instance? DenseUnionVector v)
      ;; DUV.copyValueSafe is broken - it's not safe, and it calls DenseUnionWriter.setPosition which NPEs
      (let [^DenseUnionVector from-duv v
            ^DenseUnionVector to-duv out-vec]
        (dotimes [idx (alength idxs)]
          (let [src-idx (aget idxs idx)
                type-id (.getTypeId from-duv src-idx)
                dest-offset (DenseUnionUtil/writeTypeId to-duv idx type-id)
                tp (.makeTransferPair (.getVectorByType from-duv type-id) (.getVectorByType to-duv type-id))]
            (.copyValueSafe tp (.getOffset from-duv src-idx) dest-offset))))
      (let [tp (.makeTransferPair v out-vec)]
        (dotimes [idx (alength idxs)]
          (.copyValueSafe tp (aget idxs idx) idx))))

    (DirectVector. (doto out-vec
                     (.setValueCount (alength idxs)))
                   col-name))

  (rowCopier [this-vec w]
    (let [copier (.rowCopier w v)]
      (reify IRowCopier
        (copyRow [_ idx]
          (.copyRow copier (.getIndex this-vec idx)))))))

(defn ->direct-vec ^core2.vector.IIndirectVector [^ValueVector in-vec]
  (DirectVector. in-vec (.getName in-vec)))

(defn ->indirect-vec ^core2.vector.IIndirectVector [^ValueVector in-vec, ^ints idxs]
  (IndirectVector. in-vec (.getName in-vec) idxs))

(deftype IndirectRelation [^Map cols, ^long row-count]
  IIndirectRelation
  (vectorForName [_ col-name] (.get cols col-name))
  (rowCount [_] row-count)

  (iterator [_] (.iterator (.values cols)))

  (close [_] (run! util/try-close (.values cols))))

(defn ->indirect-rel
  (^core2.vector.IIndirectRelation [cols]
   (->indirect-rel cols
                   (if-let [^IIndirectVector col (first cols)]
                     (.getValueCount col)
                     0)))

  (^core2.vector.IIndirectRelation [cols ^long row-count]
   (IndirectRelation. (let [col-map (LinkedHashMap.)]
                        (doseq [^IIndirectVector col cols]
                          (.put col-map (.getName col) col))
                        col-map)
                      row-count)))

(defn <-root [^VectorSchemaRoot root]
  (let [cols (LinkedHashMap.)]
    (doseq [^ValueVector in-vec (.getFieldVectors root)]
      (.put cols (.getName in-vec) (->direct-vec in-vec)))
    (IndirectRelation. cols (.getRowCount root))))

(defn select ^core2.vector.IIndirectRelation [^IIndirectRelation in-rel, ^ints idxs]
  (->indirect-rel (for [^IIndirectVector in-col in-rel]
                    (.select in-col idxs))))

(defn copy ^core2.vector.IIndirectRelation [^IIndirectRelation in-rel, ^BufferAllocator allocator]
  (->indirect-rel (for [^IIndirectVector in-col in-rel]
                    (.copy in-col allocator))
                  (.rowCount in-rel)))

(defn rel->rows ^java.lang.Iterable [^IIndirectRelation rel]
  (let [ks (for [^IIndirectVector col rel]
             (keyword (.getName col)))]
    (mapv (fn [idx]
            (zipmap ks
                    (for [^IIndirectVector col rel]
                      (let [v (.getVector col)
                            i (.getIndex col idx)]
                        (when-not (.isNull v i)
                          (ty/get-object v i))))))
          (range (.rowCount rel)))))

(deftype DuvChildReader [^IIndirectVector parent-col
                         ^DenseUnionVector parent-duv
                         ^byte type-id
                         ^ValueVector type-vec]
  IIndirectVector
  (getVector [_] type-vec)
  (getIndex [_ idx] (.getOffset parent-duv (.getIndex parent-col idx)))
  (getName [_] (.getName parent-col))
  (withName [_ name] (DuvChildReader. (.withName parent-col name) parent-duv type-id type-vec)))

(defn duv-type-id ^java.lang.Byte [^DenseUnionVector duv, ^LegType leg-type]
  (let [field (.getField duv)
        type-ids (.getTypeIds ^ArrowType$Union (.getType field))]
    (-> (keep-indexed (fn [idx ^Field sub-field]
                        (when (= leg-type (ty/field->leg-type sub-field))
                          (aget type-ids idx)))
                      (.getChildren field))
        (first))))

(defn reader-for-type ^core2.vector.IIndirectVector [^IIndirectVector col, ^LegType leg-type]
  (let [v (.getVector col)
        field (.getField v)
        v-type (ty/field->leg-type field)]
    (cond
      (= leg-type v-type) col

      (instance? DenseUnionVector v)
      (let [type-id (or (duv-type-id v leg-type)
                        (throw (IllegalArgumentException.)))
            type-vec (.getVectorByType ^DenseUnionVector v type-id)]
        (DuvChildReader. col v type-id type-vec)))))

(defn col->leg-types [^IIndirectVector col]
  (let [col-vec (.getVector col)
        field (.getField col-vec)]
    (if (instance? DenseUnionVector col-vec)
      (into #{} (for [^ValueVector vv (.getChildrenFromFields ^DenseUnionVector col-vec)
                      :when (pos? (.getValueCount vv))]
                  (ty/field->leg-type (.getField vv))))
      #{(ty/field->leg-type field)})))
