(ns crux.wcoj
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [crux.datalog :as cd])
  (:import [clojure.lang IPersistentCollection IPersistentMap Symbol Keyword]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BaseFixedWidthVector BaseVariableWidthVector BigIntVector BitVector
            ElementAddressableVector Float4Vector Float8Vector IntVector TimeStampNanoVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.StructVector
           org.apache.arrow.vector.types.pojo.FieldType
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.vector.util.Text
           org.apache.arrow.memory.util.ArrowBufPointer
           [java.util Arrays Date]
           java.time.Instant))

(set! *unchecked-math* :warn-on-boxed)
(s/check-asserts true)

;; generic-join

(defprotocol Relation
  (table-scan [this db])
  (table-filter [this db var-bindings])
  (insert [this value])
  (delete [this value]))

(defprotocol Db
  (assertion [this relation-name value])
  (retraction [this relation-name value])
  (ensure-relation [this relation-name relation-factory])
  (relation-by-name [this relation-name]))

(defprotocol Unification
  (unify [this that]))

(defn- constraint-satisfied? [value op arg]
  (let [diff (if (instance? ArrowBufPointer value)
               (.compareTo ^ArrowBufPointer value arg)
               (compare value arg))]
    (case op
      < (neg? diff)
      <= (not (pos? diff))
      > (pos? diff)
      >= (nat-int? diff)
      = (zero? diff)
      != (not (zero? diff)))))

(defn- execute-constraints [constraints value]
  (reduce
   (fn [value [op arg]]
     (if (constraint-satisfied? value op arg)
       value
       (reduced nil)))
   value
   constraints))

(defn- constrained [this that]
  (if-let [constraints (::constraints (meta this))]
    (if (cd/logic-var? that)
      (let [that (vary-meta that update ::constraints into constraints)]
        that)
      (when (execute-constraints constraints that)
        that))
    that))

(extend-protocol Unification
  (class (byte-array 0))
  (unify [this that]
    (cond
      (cd/logic-var? that)
      (unify that this)

      (and (bytes? that)
           (Arrays/equals ^bytes this ^bytes that))
      that))

  Symbol
  (unify [this that]
    (cond
      (cd/logic-var? that)
      (constrained that this)

      (cd/logic-var? this)
      (constrained this that)

      (= this that)
      this))

  IPersistentCollection
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (and (coll? that)
                 (= (count this) (count that)))
        (first
         (reduce
          (fn [[acc smap] [x y]]
            (if-let [u (unify (get smap x x) (get smap y y))]
              [(conj acc u)
               (cond-> smap
                 (not= cd/blank-var x) (assoc x u)
                 (not= cd/blank-var y) (assoc y u))]
              (reduced nil)))
          [[] {}]
          (mapv vector this that))))))

  Object
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (= this that)
        this)))

  nil
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (nil? that)
        this))))

(defn- find-vars [body]
  (let [vars (atom [])]
    (w/postwalk (fn [x]
                  (when (cd/logic-var? x)
                    (swap! vars conj x))
                  x)
                body)
    (set @vars)))

(defn- ensure-unique-logic-var [var]
  (if (cd/logic-var? var)
    (with-meta (gensym var) (meta var))
    var))

(defn- contains-duplicate-vars? [var-bindings]
  (let [vars (filter cd/logic-var? var-bindings)]
    (not= (distinct vars) vars)))

(defn- projection [var-bindings]
  (mapv (partial not= cd/blank-var) var-bindings))

(declare term->value)

(defmulti ^:private new-bound-vars
  (fn [bound-vars extra-logical-vars [type]]
    type))

(defmethod new-bound-vars :predicate [bound-vars extra-logical-vars [_ literal]]
  (let [vars (find-vars literal)]
    (when (set/superset? bound-vars (set/intersection vars extra-logical-vars))
      vars)))

(defmethod new-bound-vars :equality-predicate [bound-vars _ [_ {:keys [lhs op rhs] :as literal}]]
  (let [vars (find-vars literal)]
    (if (and (= '= op)
             (= 1 (count (set/difference vars bound-vars))))
      vars
      (when (set/subset? vars bound-vars)
        #{}))))

(defmethod new-bound-vars :not-predicate [bound-vars _ [_ literal]]
  (let [vars (find-vars literal)]
    (when (set/superset? bound-vars vars)
      #{})))

(defmethod new-bound-vars :external-query [bound-vars _ [_ {:keys [terms variable]}]]
  (when (set/superset? bound-vars (find-vars terms))
    #{variable}))

(defn- reorder-body [head body bound-head-vars]
  (let [{:keys [external-query equality-predicate predicate not-predicate] :as literals} (group-by first body)
        extra-logical-vars (set (for [[_ {:keys [variable]}] external-query]
                                  variable))]
    (loop [[literal :as body] (concat equality-predicate external-query predicate not-predicate)
           acc []
           bound-vars bound-head-vars]
      (if literal
        (if-let  [[[literal new-vars]] (for [literal body
                                             :let [new-vars (new-bound-vars bound-vars extra-logical-vars literal)]
                                             :when new-vars]
                                         [literal new-vars])]
          (recur (vec (remove #{literal} body))
                 (vec (conj acc (with-meta (vec literal) {:bound-vars bound-vars})))
                 (into bound-vars new-vars))
          (throw (IllegalArgumentException. "Circular dependency.")))
        acc))))

(defn- min-aggregate [x y]
  ((fnil min x x) x y))

(defn- max-aggregate [x y]
  ((fnil max x x) x y))

(defn- count-aggregate [x y]
  ((fnil inc 0) y))

(defn- sum-aggregate [x y]
  ((fnil + x 0) x y))

(defn- build-aggregates [{:keys [terms] :as head}]
  (when-let [aggregate-ops (seq (for [[idx [type term]] (map-indexed vector terms)
                                      :when (= :aggregate type)]
                                  [idx (get {'min crux.wcoj/min-aggregate
                                             'max crux.wcoj/max-aggregate
                                             'count crux.wcoj/count-aggregate
                                             'sum crux.wcoj/sum-aggregate}
                                            (:op term))]))]
    {:group-idxs (vec (for [[idx [type term]] (map-indexed vector terms)
                            :when (not= :aggregate type)]
                        idx))
     :aggregate-ops aggregate-ops}))

(defn new-constraint [var op value lhs?]
  (vary-meta var
             update
             ::constraints
             conj
             [(if-not lhs?
                (get '{< >=
                       <= >
                       > <=
                       >= <} op op)
                op)
              value]))

(defmulti ^:private term->binding
  (fn [[type term]]
    type))

(defmethod term->binding :constant [[_ term]]
  (gensym 'constant))

(defmethod term->binding :aggregate [[_ term]]
  (first (:variable term)))

(defmethod term->binding :default [[_ term]]
  term)

(defn- quote-term [x]
  (list 'quote x))

(defmulti ^:private term->value
  (fn [[type term]]
    type))

(defmethod term->value :constant [[_ term]]
  (if (symbol? term)
    (quote-term term)
    term))

(defmethod term->value :aggregate [[_ term]]
  (first (:variable term)))

(defmethod term->value :default [[_ term]]
  term)

(defmulti ^:private term->signature
  (fn [[type term]]
    type))

(defmethod term->signature :aggregate [[_ term]]
  (first (:variable term)))

(defmethod term->signature :default [[_ term]]
  term)

(defn- rule->query-plan [rule bound-head-var-idxs]
  (let [{:keys [head body] :as conformed-rule} (s/conform :crux.datalog/rule rule)
        _ (assert (set/superset? (find-vars body) (disj (find-vars head) cd/blank-var))
                  "rule does not satisfy safety requirement for head variables")
        bound-head-vars (set (map (mapv term->binding (:terms head)) bound-head-var-idxs))
        body (reorder-body head body bound-head-vars)]
    {:existential-vars (set/difference (find-vars body) (find-vars head))
     :bound-head-vars bound-head-vars
     :aggregates (build-aggregates head)
     :rule rule
     :head head
     :body body}))

(defn- predicate->clojure [{:keys [db-sym]} {:keys [symbol terms]}]
  `(crux.wcoj/table-filter
    (crux.wcoj/relation-by-name ~db-sym '~symbol)
    ~db-sym ~(mapv term->value terms)))

(defmulti ^:private datalog->clojure (fn [query-plan [type]]
                                       type))

(defmethod datalog->clojure :predicate [query-plan [_ {:keys [terms] :as predicate}]]
  (let [term-vars (mapv term->binding terms)]
    [term-vars (predicate->clojure query-plan predicate)]))

(defmethod datalog->clojure :equality-predicate [_ [_ {:keys [lhs op rhs]} :as literal]]
  (let [bound-vars (:bound-vars (meta literal))
        lhs-binding (term->binding lhs)
        rhs-binding (term->binding rhs)]
    (cond
      (and (cd/logic-var? rhs-binding)
           (not (contains? bound-vars rhs-binding)))
      `[:let [~rhs-binding (crux.wcoj/new-constraint ~(term->value rhs) '~op ~(term->value lhs) false)]]

      (and (cd/logic-var? lhs-binding)
           (not (contains? bound-vars lhs-binding)))
      `[:let [~lhs-binding (crux.wcoj/new-constraint ~(term->value lhs) '~op ~(term->value rhs) true)]]

      :else
      (if (= '= op)
        `[:let [~lhs-binding (crux.wcoj/unify ~(term->value lhs) ~(term->value rhs))]
          :when (some? ~lhs-binding)
          :let [~rhs-binding ~lhs-binding]]
        (let [op-fn (get '{!= (complement crux.wcoj/unify)} op op)]
          `[:when (~op-fn ~(term->value lhs) ~(term->value rhs))])))))

(defmethod datalog->clojure :not-predicate [query-plan [_ {:keys [predicate]}]]
  `[:when (empty? ~(predicate->clojure query-plan predicate))])

(defmethod datalog->clojure :external-query [_ [_ {:keys [variable symbol terms]}]]
  `[:let [~variable (crux.wcoj/unify ~variable (~symbol ~@(mapv term->value terms)))]
    :when (some? ~variable)])

(defn aggregate [{:keys [group-idxs aggregate-ops]} tuples]
  (vals
   (reduce
    (fn [acc tuple]
      (update acc (mapv tuple group-idxs)
              (fn [group-acc]
                (reduce
                 (fn [acc [idx op]]
                   (update acc idx op (get group-acc idx)))
                 tuple aggregate-ops))))
    {}
    tuples)))

(defn- query-plan->clojure [{:keys [existential-vars aggregates head body] :as query-plan}]
  (let [{:keys [symbol terms]} head
        db-sym (gensym 'db)
        args-sym (gensym 'args)
        query-plan (assoc query-plan :db-sym db-sym)
        bindings (mapcat (partial datalog->clojure query-plan) body)
        arg-vars (mapv term->binding terms)
        args-signature (mapv (comp quote-term term->signature) terms)]
    `(fn ~symbol
       ([~db-sym] (~symbol ~db-sym '~(mapv ensure-unique-logic-var (repeat (count arg-vars) cd/blank-var))))
       ([~db-sym ~args-sym]
        (->> (for [loop# [nil]
                   :let [[~@arg-vars :as unified?#] (crux.wcoj/unify ~args-sym ~args-signature)]
                   :when unified?#
                   :let [~@(interleave existential-vars (map quote-term existential-vars))]
                   ~@bindings]
               ~arg-vars)
             ~(if aggregates
                `(crux.wcoj/aggregate '~aggregates)
                `(identity)))))))

(defn- compile-rule-no-memo [rule bound-head-vars]
  (let [bound-head-var-idxs (set (for [[idx bound?] (map-indexed vector bound-head-vars)
                                       :when bound?]
                                   idx))
        query-plan (rule->query-plan rule bound-head-var-idxs)
        clojure-source (query-plan->clojure query-plan)]
    (with-meta
      (eval clojure-source)
      (assoc query-plan :source clojure-source))))

(def ^:private compile-rule-memo (memoize compile-rule-no-memo))

(defn- compile-rule [rule var-bindings]
  (compile-rule-memo rule (mapv (complement cd/logic-var?) var-bindings)))

(defn- execute-rules-no-memo [rules db var-bindings]
  (let [var-bindings (mapv ensure-unique-logic-var var-bindings)]
    (->> (for [rule rules
               :let [compiled-rule (compile-rule rule var-bindings)]]
           (if (empty? var-bindings)
             (compiled-rule db)
             (compiled-rule db var-bindings)))
         (apply concat)
         (distinct))))

(defn- rule-memo-key [rules var-bindings]
  [(System/identityHashCode rules)
   (let  [smap (zipmap (filter cd/logic-var? var-bindings)
                       (for [id (range)]
                         (symbol (str "crux.wcoj/variable_" id))))
          smap (->> (for [[var memo-key] smap]
                      [var [memo-key (::constraints (meta var))]])
                    (into {}))]
     (replace smap var-bindings))])

(defn- execute-rules-memo [rules db var-bindings]
  (let [db (vary-meta db update :rule-memo-state #(or % (atom {})))
        {:keys [rule-memo-state]} (meta db)
        memo-key (rule-memo-key rules var-bindings)
        memo-value (get @rule-memo-state memo-key ::not-found)]
    (if (= ::not-found memo-value)
      (doto (execute-rules-no-memo rules db var-bindings)
        (->> (swap! rule-memo-state assoc memo-key)))
      memo-value)))

(defn- execute-rules [rules db var-bindings]
  (cond->> (execute-rules-memo rules db var-bindings)
    (contains-duplicate-vars? var-bindings) (filter #(unify var-bindings %))))

(defrecord RuleRelation [name rules]
  Relation
  (table-scan [this db]
    (table-filter this db nil))

  (table-filter [this db var-bindings]
    (execute-rules rules db var-bindings))

  (insert [this rule]
    (s/assert :crux.datalog/rule rule)
    (update this :rules conj rule))

  (delete [this rule]
    (update this :rules disj rule)))

(defn- new-rule-relation [name]
  (->RuleRelation name #{}))

(extend-protocol Relation
  nil
  (table-scan [this db])

  (table-filter [this db var-bindings])

  (insert [this tuple]
    (throw (UnsupportedOperationException.)))

  (delete [this tuple]
    (throw (UnsupportedOperationException.)))

  IPersistentCollection
  (table-scan [this db]
    (table-filter this db nil))

  (table-filter [this db var-bindings]
    (let [projection (projection var-bindings)]
      (for [tuple (seq this)
            :when (or (nil? var-bindings)
                      (unify tuple var-bindings))]
        (mapv (fn [v p]
                (if p
                  v
                  cd/blank-var)) tuple projection))))

  (insert [this tuple]
    (conj this tuple))

  (delete [this tuple]
    (disj this tuple)))

(defn new-sorted-set-relation [relation-name]
  (with-meta (sorted-set) {:name relation-name}))

(defrecord CombinedRelation [name rules tuples]
  Relation
  (table-scan [this db]
    (concat (table-scan tuples db)
            (table-scan rules db)))

  (table-filter [this db var-bindings]
    (concat (table-filter tuples db var-bindings)
            (table-filter rules db var-bindings)))

  (insert [this value]
    (if (s/valid? :crux.datalog/rule value)
      (update this :rules insert value)
      (update this :tuples insert value)))

  (delete [this value]
    (if (s/valid? :crux.datalog/rule value)
      (update this :rules delete value)
      (update this :tuples delete value))))

(def ^:dynamic *tuple-relation-factory* new-sorted-set-relation)

(defn- new-combined-relation [relation-name]
  (->CombinedRelation
   relation-name
   (new-rule-relation relation-name)
   (*tuple-relation-factory* relation-name)))

(def ^:dynamic *relation-factory* new-combined-relation)

(extend-type IPersistentMap
  Db
  (assertion [this relation-name value]
    (update (ensure-relation this relation-name *relation-factory*)
            relation-name
            insert
            value))

  (retraction [this relation-name value]
    (update this
            relation-name
            delete
            value))

  (ensure-relation [this relation-name relation-factory]
    (cond-> this
      (not (contains? this relation-name)) (assoc relation-name (relation-factory relation-name))))

  (relation-by-name [this relation-name]
    (get this relation-name)))

(defn query-by-name
  ([db rule-name]
   (table-scan (relation-by-name db rule-name) db))
  ([db rule-name args]
   (table-filter (relation-by-name db rule-name) db args)))

(defn- query-conformed-datalog [db {{:keys [symbol terms]} :head}]
  (let [args (mapv second terms)]
    (query-by-name db symbol args)))

(defn query [db query]
  (s/assert :crux.datalog/query query)
  (query-conformed-datalog db (s/conform :crux.datalog/query query)))

(defn assert-all [db relation-name tuples]
  (reduce (fn [db tuple]
            (assertion db relation-name tuple)) db tuples))

(defn tuple->datalog-str [relation-name tuple]
  (str relation-name
       (when (seq tuple)
         (str "(" (str/join ", " tuple) ")"))
       "."))

(def ^:private execution-hierarchy
  (-> (make-hierarchy)
      (derive :assertion :modification)
      (derive :retraction :modification)
      (atom)))

(defmulti ^:private execute-statement
  (fn [db [type]]
    type)
  :hierarchy execution-hierarchy)

(defmethod execute-statement :query [db [type {{:keys [symbol]} :head :as statement}]]
  (doseq [tuple (sort (distinct (query-conformed-datalog db statement)))]
    (println (tuple->datalog-str symbol tuple)))
  db)

(defmethod execute-statement :requirement [db [type {:keys [identifier]}]]
  (require (first identifier))
  db)

(defmethod execute-statement :modification [db [type {:keys [clause]}]]
  (let [op (get {:assertion assertion :retraction retraction} type)
        [type clause] clause
        {:keys [symbol terms]} (:head clause)]
    (case type
      :fact
      (op db symbol (mapv second terms))

      :rule
      (op db symbol (vec (s/unform :crux.datalog/rule clause))))))

(defn execute
  ([datalog]
   (execute {} datalog))
  ([db datalog]
   (s/assert :crux.datalog/program datalog)
   (->> (s/conform :crux.datalog/program datalog)
        (reduce execute-statement db))))

(defn -main [& [f :as args]]
  (execute (cd/parse-datalog (io/reader (or f *in*)))))

;; Arrow

(def ^:private ^BufferAllocator
  allocator (RootAllocator. Long/MAX_VALUE))

(defn- init-struct [^StructVector struct column-template]
  (reduce
   (fn [^StructVector struct [idx column-template]]
     (let [column-template (if-let [[[_ value]] (and (cd/logic-var? column-template)
                                                     (::constraints (meta column-template)))]
                             value
                             column-template)
           column-type (.getSimpleName (class column-template))
           [^FieldType field-type ^Class vector-class]
           (case (symbol column-type)
             Integer [(FieldType/nullable (.getType Types$MinorType/INT))
                      IntVector]
             Long [(FieldType/nullable (.getType Types$MinorType/BIGINT))
                   BigIntVector]
             Float [(FieldType/nullable (.getType Types$MinorType/FLOAT4))
                    Float4Vector]
             Double [(FieldType/nullable (.getType Types$MinorType/FLOAT8))
                     Float8Vector]
             String [(FieldType/nullable (.getType Types$MinorType/VARCHAR))
                     VarCharVector]
             Boolean [(FieldType/nullable (.getType Types$MinorType/BIT))
                      BitVector]
             (Date Instant) [(FieldType/nullable (.getType Types$MinorType/TIMESTAMPNANO))
                             TimeStampNanoVector]
             [(FieldType/nullable (.getType Types$MinorType/VARBINARY))
              VarBinaryVector])]
       (doto struct
         (.addOrGet
          (str idx "_" (str/lower-case column-type))
          field-type
          vector-class))))
   struct
   (map-indexed vector column-template)))

(defn- arrow->clojure [value]
  (cond
    (instance? Text value)
    (str value)

    (bytes? value)
    (edn/read-string (String. ^bytes value "UTF-8"))

    :else
    value))

(defn- clojure->arrow [value]
  (cond
    (string? value)
    (Text. (.getBytes ^String value "UTF-8"))

    (boolean? value)
    (if value 1 0)

    (number? value)
    value

    (instance? Date value)
    (clojure->arrow (.toInstant ^Date value))

    (instance? Instant value)
    (+ (* (.getEpochSecond ^Instant value) 1000000000)
       (.getNano ^Instant value))

    :else
    (.getBytes (pr-str value) "UTF-8")))

(defn- insert-clojure-value-into-column [^ValueVector column ^long idx v]
  (if-let [[[_ value]] (and (cd/logic-var? v)
                            (::constraints (meta v)))]
    (insert-clojure-value-into-column column idx value)
    (cond
      (instance? IntVector column)
      (.setSafe ^IntVector column idx ^int (clojure->arrow v))

      (instance? BigIntVector column)
      (.setSafe ^BigIntVector column idx ^long (clojure->arrow v))

      (instance? Float4Vector column)
      (.setSafe ^Float4Vector column idx ^float (clojure->arrow v))

      (instance? Float8Vector column)
      (.setSafe ^Float8Vector column idx ^double (clojure->arrow v))

      (instance? VarCharVector column)
      (.setSafe ^VarCharVector column idx ^Text (clojure->arrow v))

      (instance? BitVector column)
      (.setSafe ^BitVector column idx ^long (clojure->arrow v))

      (instance? TimeStampNanoVector column)
      (.setSafe ^TimeStampNanoVector column idx ^long (clojure->arrow v))

      (instance? VarBinaryVector column)
      (.setSafe ^VarBinaryVector column idx ^bytes (clojure->arrow v))

      :else
      (throw (IllegalArgumentException.)))))

(def ^:private ^{:tag 'long} default-vector-size 128)

(defn- unifiers->arrow [unifier-vector var-bindings]
  (vec (for [[^ElementAddressableVector unify-column var-binding] (map vector unifier-vector var-bindings)]
         (cond
           (cd/logic-var? var-binding)
           (if-let [constraints (::constraints (meta var-binding))]
             var-binding
             ::wildcard)

           (boolean? var-binding)
           var-binding

           :else
           (.getDataPointer ^ElementAddressableVector unify-column 0)))))

(defn- selected-indexes ^org.apache.arrow.vector.BitVector
  [^VectorSchemaRoot record-batch unifiers ^BitVector selection-vector-out]
  (let [pointer (ArrowBufPointer.)]
    (loop [n 0
           selection-vector selection-vector-out]
      (if (< n (count (.getFieldVectors record-batch)))
        (let [unifier (get unifiers n)]
          (if (and (pos? n) (= ::wildcard unifier))
            (recur (inc n) selection-vector)
            (let [column ^ElementAddressableVector (.getVector record-batch n)]
              (recur (inc n)
                     (loop [idx 0
                            selection-vector selection-vector]
                       (if (< idx (.getValueCount column))
                         (recur (inc idx)
                                (cond
                                  (.isNull column idx)
                                  (doto selection-vector
                                    (.setSafe idx 0))

                                  (and (pos? n)
                                       (zero? (.get selection-vector idx)))
                                  selection-vector

                                  :else
                                  (doto selection-vector
                                    (.setSafe idx (if (or (= ::wildcard unifier)
                                                          (unify unifier
                                                                 (if (instance? ArrowBufPointer unifier)
                                                                   (.getDataPointer column idx pointer)
                                                                   (arrow->clojure (.getObject column idx)))))
                                                    1
                                                    0)))))
                         selection-vector))))))
        selection-vector))))

(defn- project-column [^VectorSchemaRoot record-batch ^long idx ^long n projection]
  (if (get projection n)
    (let [column (.getVector record-batch n)
          value (.getObject column idx)]
      (arrow->clojure value))
    cd/blank-var))

(defn- selected-tuples [^VectorSchemaRoot record-batch projection ^long base-offset ^BitVector selection-vector]
  (let [row-count (.getRowCount record-batch)
        column-count (count (.getFieldVectors record-batch))]
    (loop [n 0
           acc []]
      (if (= n column-count)
        acc
        (recur (inc n)
               (loop [selection-offset 0
                      acc acc
                      idx 0]
                 (if (< selection-offset row-count)
                   (if (zero? (.get selection-vector selection-offset))
                     (recur (inc selection-offset)
                            acc
                            idx)
                     (recur (inc selection-offset)
                            (let [value (project-column record-batch selection-offset n projection)]
                              (if (zero? n)
                                (conj acc (with-meta [value] {::index (+ base-offset selection-offset)}))
                                (update acc idx conj value)))
                            (inc idx)))
                   acc)))))))

(defn- arrow-seq [^StructVector struct var-bindings]
  (let [struct-batch (VectorSchemaRoot. struct)
        vector-size (min default-vector-size (.getRowCount struct-batch))
        selection-vector (doto (BitVector. "" allocator)
                           (.setValueCount vector-size)
                           (.setInitialCapacity vector-size))
        unify-tuple? (contains-duplicate-vars? var-bindings)
        unifier-vector (insert
                        (StructVector/empty nil allocator)
                        var-bindings)
        unifiers (unifiers->arrow unifier-vector var-bindings)
        projection (projection var-bindings)]
    (if (zero? (count (.getFieldVectors struct-batch)))
      (repeat vector-size (with-meta [] {::index 0}))
      (->> (for [start-idx (range 0 (.getRowCount struct-batch) vector-size)
                 :let [start-idx (long start-idx)
                       record-batch (if (< (.getRowCount struct-batch) (+ start-idx vector-size))
                                      (.slice struct-batch start-idx)
                                      (.slice struct-batch start-idx vector-size))]]
             (cond->> (selected-indexes record-batch unifiers selection-vector)
               true (selected-tuples record-batch projection start-idx)
               unify-tuple? (filter (partial unify var-bindings))))
           (apply concat)))))

(extend-protocol Relation
  StructVector
  (table-scan [this db]
    (arrow-seq this nil))

  (table-filter [this db var-bindings]
    (arrow-seq this var-bindings))

  (insert [this value]
    (if (and (zero? (.size this)) (pos? (count value)))
      (insert (init-struct this value) value)
      (let [idx (.getValueCount this)]
        (dotimes [n (.size this)]
          (let [v (get value n)
                column (.getChildByOrdinal this n)]
            (insert-clojure-value-into-column column idx v)))

        (doto this
          (.setIndexDefined idx)
          (.setValueCount (inc idx))))))

  (delete [this value]
    (doseq [to-delete (arrow-seq this value)
            :let [idx (::index (meta to-delete))]]
      (dotimes [n (.size this)]
        (let [column (.getChildByOrdinal this n)]
          (when (instance? BaseFixedWidthVector column)
            (.setNull ^BaseFixedWidthVector column idx))
          (when (instance? BaseVariableWidthVector column)
            (.setNull ^BaseVariableWidthVector column idx))))
      (.setNull this idx))
    this))

(defn new-arrow-struct-relation
  ^org.apache.arrow.vector.complex.StructVector [relation-name]
  (doto (StructVector/empty (str relation-name) allocator)
    (.setInitialCapacity 0)))
