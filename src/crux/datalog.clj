(ns crux.datalog
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [crux.io :as cio]
            [crux.datalog.parser :as dp])
  (:import [clojure.lang IPersistentCollection IPersistentMap Symbol]
           java.util.Arrays
           [java.util.function Predicate LongPredicate DoublePredicate]
           java.lang.AutoCloseable))

(set! *unchecked-math* :warn-on-boxed)
(s/check-asserts true)

(defprotocol Relation
  (table-scan [this db])
  (table-filter [this db var-bindings])
  (insert [this value])
  (delete [this value])
  (truncate [this])
  (cardinality [this])
  (relation-name [this]))

(defprotocol Db
  (assertion [this relation-name value])
  (retraction [this relation-name value])
  (ensure-relation [this relation-name relation-factory])
  (relation-by-name [this relation-name])
  (relations [this]))

(defprotocol Unification
  (unify [this that]))

(defn- and-constraint [c1 c2]
  (cond
    (nil? c1)
    c2

    (instance? DoublePredicate c1)
    (.and ^DoublePredicate c1 c2)

    (instance? LongPredicate c1)
    (.and ^LongPredicate c1 c2)

    :else
    (.and ^Predicate c1 c2)))

(defn- constrained [this that]
  (let [{:keys [constraints constraint-fn]} (meta this)]
    (if constraints
      (if (dp/logic-var? that)
        (let [that (-> that
                       (vary-meta update :constraints into constraints)
                       (vary-meta update :constraint-fn and-constraint constraint-fn))]
          that)
        (when (cond
                (instance? LongPredicate constraint-fn) (.test ^LongPredicate constraint-fn (long that))
                (instance? DoublePredicate constraint-fn) (.test ^DoublePredicate constraint-fn (double that))
                :else (.test ^Predicate constraint-fn that))
          that))
      that)))

(extend-protocol Unification
  (class (byte-array 0))
  (unify [this that]
    (cond
      (dp/logic-var? that)
      (unify that this)

      (and (bytes? that)
           (Arrays/equals ^bytes this ^bytes that))
      that))

  Symbol
  (unify [this that]
    (cond
      (dp/logic-var? this)
      (constrained this that)

      (dp/logic-var? that)
      (unify that this)

      (= this that)
      this))

  IPersistentCollection
  (unify [this that]
    (if (dp/logic-var? that)
      (unify that this)
      (when (and (coll? that)
                 (= (count this) (count that)))
        (first
         (reduce
          (fn [[acc smap] [x y]]
            (if-let [u (unify (get smap x x) (get smap y y))]
              [(conj acc u)
               (cond-> smap
                 (not= dp/blank-var x) (assoc x u)
                 (not= dp/blank-var y) (assoc y u))]
              (reduced nil)))
          [[] {}]
          (mapv vector this that))))))

  Object
  (unify [this that]
    (if (dp/logic-var? that)
      (unify that this)
      (when (= this that)
        this)))

  nil
  (unify [this that]
    (if (dp/logic-var? that)
      (unify that this)
      (when (nil? that)
        this))))

(defn try-close [c]
  (when (instance? AutoCloseable c)
    (.close ^AutoCloseable c)))

(defn- find-vars [body]
  (let [vars (atom [])]
    (w/postwalk (fn [x]
                  (when (dp/logic-var? x)
                    (swap! vars conj x))
                  x)
                body)
    (set @vars)))

(defn ensure-unique-logic-var [var]
  (if (dp/logic-var? var)
    (with-meta (gensym var) (meta var))
    var))

(defn contains-duplicate-vars? [var-bindings]
  (let [vars (filter dp/logic-var? var-bindings)]
    (not= (distinct vars) vars)))

(defn projection [var-bindings]
  (vec (for [var var-bindings]
         (cond
           (= dp/blank-var var) ::blank-var
           (dp/logic-var? var) ::logic-var
           :else
           var))))

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
    (when (or (not-empty (set/intersection vars bound-vars))
              (and (< (count vars) 2) (not= lhs rhs)))
      #{})))

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
                                  [idx (get {'min crux.datalog/min-aggregate
                                             'max crux.datalog/max-aggregate
                                             'count crux.datalog/count-aggregate
                                             'sum crux.datalog/sum-aggregate}
                                            (:op term))]))]
    {:group-idxs (vec (for [[idx [type term]] (map-indexed vector terms)
                            :when (not= :aggregate type)]
                        idx))
     :aggregate-ops aggregate-ops}))

(defn new-constraint [var constraint-fn op value]
  (-> var
      (vary-meta update :constraints conj [op value])
      (vary-meta update :constraint-fn and-constraint constraint-fn)))

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
  (let [{:keys [head body] :as conformed-rule} (s/conform :crux.datalog.parser/rule rule)
        _ (when-not (set/superset? (find-vars body) (disj (find-vars head) dp/blank-var))
            (throw (IllegalArgumentException. (str "rule does not satisfy safety requirement for head variables: " rule))))
        bound-head-vars (set (map (mapv term->binding (:terms head)) bound-head-var-idxs))
        body (reorder-body head body bound-head-vars)]
    {:existential-vars (set/difference (find-vars body) (find-vars head))
     :bound-head-vars bound-head-vars
     :aggregates (build-aggregates head)
     :rule rule
     :head head
     :body body}))

(defn- predicate->clojure [{:keys [db-sym]} {:keys [symbol terms]}]
  `(crux.datalog/table-filter
    (crux.datalog/relation-by-name ~db-sym '~symbol)
    ~db-sym ~(mapv term->value terms)))

(defn- flip-constraint-op [op]
  (get '{< >
         <= >=
         > <
         >= <=} op op))

(defn- normalize-op [op]
  (get '{!= not=} op op))

(defn- maybe-primitive-tag [x]
  (cond
    (float? x) 'double
    (int? x) 'long))

(defn- constraint->clojure [op x y]
  (if (number? y)
    `(~(get '{!= not=} op op) ~x ~y)
    (let [diff-sym (gensym 'diff)]
      `(let [~diff-sym (compare ~x ~y)]
         ~(case op
            < `(neg? ~diff-sym)
            <= `(not (pos? ~diff-sym))
            > `(pos? ~diff-sym)
            >= `(not (neg? ~diff-sym))
            = `(zero? ~diff-sym)
            != `(not (zero? ~diff-sym)))))))

(defn- constraint->java-predicate [op arg-sym value]
  (case (maybe-primitive-tag value)
    long `(reify LongPredicate
            (test [_ ~arg-sym]
              ~(constraint->clojure op arg-sym value)))
    double `(reify DoublePredicate
              (test [_ ~arg-sym]
                ~(constraint->clojure op arg-sym value)))
    `(reify Predicate
       (test [_ ~arg-sym]
         ~(constraint->clojure op arg-sym value)))))

(defmulti ^:private datalog->clojure (fn [query-plan [type]]
                                       type))

(defmethod datalog->clojure :predicate [query-plan [_ {:keys [terms] :as predicate}]]
  (let [term-vars (mapv term->binding terms)]
    [term-vars (predicate->clojure query-plan predicate)]))

(defmethod datalog->clojure :equality-predicate [_ [_ {:keys [lhs op rhs]} :as literal]]
  (let [bound-vars (:bound-vars (meta literal))
        lhs-binding (term->binding lhs)
        rhs-binding (term->binding rhs)
        arg-sym (gensym 'arg)]
    (cond
      (and (dp/logic-var? rhs-binding)
           (not (contains? bound-vars rhs-binding)))
      (if (= '= op)
        `[:let [~rhs-binding (crux.datalog/unify ~(term->value rhs) ~(term->value lhs))]
          :when (some? ~rhs-binding)]
        (let [op (flip-constraint-op op)]
          `[:let [~rhs-binding (crux.datalog/new-constraint ~(term->value rhs)
                                                            ~(constraint->java-predicate op arg-sym (term->value lhs))
                                                            '~op
                                                            ~(term->value lhs))]]))

      (and (dp/logic-var? lhs-binding)
           (not (contains? bound-vars lhs-binding)))
      (if (= '= op)
        `[:let [~lhs-binding (crux.datalog/unify ~(term->value lhs) ~(term->value rhs))]
          :when (some? ~lhs-binding)]
        `[:let [~lhs-binding (crux.datalog/new-constraint ~(term->value lhs)
                                                          ~(constraint->java-predicate op arg-sym (term->value rhs))
                                                          '~op
                                                          ~(term->value rhs))]])

      :else
      `[:when ~(constraint->clojure op (term->value lhs) (term->value rhs))])))

(defmethod datalog->clojure :not-predicate [query-plan [_ {:keys [predicate]}]]
  `[:when (empty? ~(predicate->clojure query-plan predicate))])

(defmethod datalog->clojure :external-query [_ [_ {:keys [variable symbol terms]}]]
  `[:let [~variable (crux.datalog/unify ~variable (~symbol ~@(mapv term->value terms)))]
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

;; generic-join

(defn- query-plan->clojure [{:keys [existential-vars aggregates head body] :as query-plan}]
  (let [{:keys [symbol terms]} head
        db-sym (gensym 'db)
        args-sym (gensym 'args)
        query-plan (assoc query-plan :db-sym db-sym)
        bindings (mapcat (partial datalog->clojure query-plan) body)
        arg-vars (mapv term->binding terms)
        args-signature (mapv (comp quote-term term->signature) terms)
        query-loop `(for [loop# [nil]
                          :let [[~@arg-vars :as unified?#] (crux.datalog/unify ~args-sym ~args-signature)]
                          :when unified?#
                          :let [~@(interleave existential-vars (map quote-term existential-vars))]
                          ~@bindings]
                      ~arg-vars)]
    `(fn ~symbol
       ([~db-sym] (~symbol ~db-sym '~(mapv ensure-unique-logic-var (repeat (count arg-vars) dp/blank-var))))
       ([~db-sym ~args-sym]
        ~(if aggregates
           `(->> ~query-loop (crux.datalog/aggregate '~aggregates))
           query-loop)))))

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
  (compile-rule-memo rule (mapv (complement dp/logic-var?) var-bindings)))

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
   (let  [smap (zipmap (filter dp/logic-var? var-bindings)
                       (for [id (range)]
                         (symbol (str "crux.datalog/variable_" id))))
          smap (->> (for [[var memo-key] smap]
                      [var [memo-key (:constraints (meta var))]])
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
    (s/assert :crux.datalog.parser/rule rule)
    (update this :rules conj rule))

  (delete [this rule]
    (update this :rules disj rule))

  (truncate [this]
    (update this :rules empty))

  (cardinality [this]
    0)

  (relation-name [this]
    name))

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

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    0)

  (relation-name [this])

  IPersistentCollection
  (table-scan [this db]
    (seq this))

  (table-filter [this db var-bindings]
    (let [projection (projection var-bindings)]
      (for [tuple (seq this)
            :when (or (nil? var-bindings)
                      (unify tuple var-bindings))]
        (mapv (fn [v p]
                (case p
                  ::blank-var dp/blank-var
                  ::logic-var v
                  p)) tuple projection))))

  (insert [this tuple]
    (conj this tuple))

  (delete [this tuple]
    (disj this tuple))

  (truncate [this]
    (empty this))

  (cardinality [this]
    (count this))

  (relation-name [this]
    (:name (meta this))))

(defn new-sorted-set-relation
  ([relation-name]
   (with-meta (sorted-set) {:name relation-name}))
  ([comparator relation-name]
   (with-meta (sorted-set-by comparator) {:name relation-name})))

(defrecord CombinedRelation [name rules tuples]
  Relation
  (table-scan [this db]
    (concat (table-scan tuples db)
            (table-scan rules db)))

  (table-filter [this db var-bindings]
    (concat (table-filter tuples db var-bindings)
            (table-filter rules db var-bindings)))

  (insert [this value]
    (if (s/valid? :crux.datalog.parser/rule value)
      (update this :rules insert value)
      (update this :tuples insert value)))

  (delete [this value]
    (if (s/valid? :crux.datalog.parser/rule value)
      (update this :rules delete value)
      (update this :tuples delete value)))

  (truncate [this]
    (-> this
        (update :rules truncate)
        (update :tuples truncate)))

  (cardinality [this]
    (cardinality tuples))

  (relation-name [this]
    name)

  AutoCloseable
  (close [this]
    (try-close rules)
    (try-close tuples)))

(def ^:dynamic *tuple-relation-factory* new-sorted-set-relation)

(defn new-combined-relation
  ([relation-name]
   (new-combined-relation relation-name *tuple-relation-factory*))
  ([relation-name tuple-relation-factory]
   (->CombinedRelation
    relation-name
    (new-rule-relation relation-name)
    (tuple-relation-factory relation-name))))

(def ^:dynamic *relation-factory* new-combined-relation)

(defrecord ParentChildRelation [deletion-set parent child comparator]
  Relation
  (table-scan [this db]
    (let [parent-seq (cond->> (table-scan parent db)
                       (seq deletion-set) (remove deletion-set))
          child-seq (table-scan child db)]
      (if comparator
        (cio/merge-sort comparator [parent-seq child-seq])
        (concat parent-seq child-seq))))

  (table-filter [this db var-bindings]
    (let [parent-seq (cond->> (table-filter parent db var-bindings)
                       (seq deletion-set) (remove deletion-set))
          child-seq (table-filter child db var-bindings)]
      (if comparator
        (cio/merge-sort comparator [parent-seq child-seq])
        (concat parent-seq child-seq))))

  (insert [this value]
    (-> this
        (update :child insert value)
        (update :deletion-set disj value)))

  (delete [this value]
    (cond-> (update this :child delete value)
      (seq (table-filter parent nil value)) (update :deletion-set conj value)))

  (truncate [this]
    (-> this
        (update :child truncate)
        (update :deletion-set empty)))

  (cardinality [this]
    (+ ^long (cardinality parent)
       ^long (cardinality child)))

  (relation-name [this]
    (relation-name child))

  AutoCloseable
  (close [_]
    (try-close child)
    (try-close parent)))

(defn new-parent-child-relation
  ([parent child]
   (new-parent-child-relation parent child nil))
  ([parent child comparator]
   (->ParentChildRelation #{} parent child comparator)))

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
    (get this relation-name))

  (relations [this]
    (vals this)))

(defn query-by-name
  ([db rule-name]
   (table-scan (relation-by-name db rule-name) db))
  ([db rule-name args]
   (table-filter (relation-by-name db rule-name) db args)))

(defn- query-conformed-datalog [db {{:keys [symbol terms]} :head}]
  (let [args (mapv second terms)]
    (query-by-name db symbol args)))

(defn query [db query]
  (s/assert :crux.datalog.parser/query query)
  (query-conformed-datalog db (s/conform :crux.datalog.parser/query query)))

(defn assert-all [db relation-name tuples]
  (reduce (fn [db tuple]
            (assertion db relation-name tuple)) db tuples))

(defn close-db [db]
  (doseq [relation (relations db)]
    (try-close relation))
  (try-close db))

(defn tuple->datalog-str [relation-name tuple]
  (str relation-name
       (when (seq tuple)
         (str "(" (str/join ", " (map pr-str tuple)) ")"))
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
      (op db symbol (vec (s/unform :crux.datalog.parser/rule clause))))))

(defn execute
  ([datalog]
   (execute {} datalog))
  ([db datalog]
   (s/assert :crux.datalog.parser/program datalog)
   (->> (s/conform :crux.datalog.parser/program datalog)
        (reduce execute-statement db))))

(defn -main [& [f :as args]]
  (execute (dp/parse-datalog (io/reader (or f *in*)))))
