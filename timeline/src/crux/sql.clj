(ns crux.sql
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.instant :as i]
            [clojure.set :as set]
            [clojure.string :as s]
            [clojure.walk :as w]
            [instaparse.core :as insta])
  (:import [java.util Comparator Date]
           java.util.function.Function
           [java.time Duration Period ZoneOffset]
           [java.time.temporal ChronoField TemporalAmount]))

(defn parse-string [x]
  (s/replace (subs x 1 (dec (count x)))
             "''"
             "'"))

(defn parse-date [x]
  (i/read-instant-date (s/replace (parse-string x) " " "T")))

(defn parse-number [x]
  (edn/read-string x))

(defn parse-interval [x [field]]
  (let [x (parse-number (parse-string x))]
    (case field
      :year (Period/ofYears x)
      :month (Period/ofMonths x)
      :day (Period/ofDays x)
      :hour (Duration/ofHours x)
      :minute (Duration/ofMinutes x))))

(defn parse-identifier [x]
  (symbol x))

(defn parse-boolean [[x]]
  (= :true x))

(defn parse-like-pattern [x & [escape]]
  (let [pattern (parse-string x)
        escape (or (some-> escape (parse-string)) "\\")
        regex (if (= "\\" escape)
                (-> pattern
                    (s/replace #"([^\\]|^)(_)" "$1.")
                    (s/replace #"([^\\]|^)(%)" "$1.*")
                    (s/replace "\\_" "_")
                    (s/replace "\\%" "%"))
                (-> pattern
                    (s/replace (re-pattern (str "([^"
                                                escape
                                                "]|^)(_)"))
                               "$1.")
                    (s/replace (re-pattern (str "([^"
                                                escape
                                                "]|^)(%)"))
                               "$1.*")
                    (s/replace (str escape "_") "_")
                    (s/replace (str escape "%") "%")))]
    (re-pattern regex)))

(def parse-sql
  (insta/parser (io/resource "crux/sql.ebnf")
                :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*\\s*' | #'\\s*/[*].*([*]/\\s*|$)'")
                :string-ci true))

(def literal-transform
  {:boolean-literal parse-boolean
   :numeric-literal parse-number
   :unsigned-numeric-literal parse-number
   :date-literal parse-date
   :timestamp-literal parse-date
   :interval-literal parse-interval
   :string-literal parse-string
   :like-pattern parse-like-pattern
   :identifier parse-identifier})

(def constant-folding-transform
  {:numeric-minus (fn [x y]
                    (cond
                      (and (instance? Date x)
                           (instance? TemporalAmount y))
                      (Date/from (.toInstant (.minus (.atOffset (.toInstant ^Date x) ZoneOffset/UTC) ^TemporalAmount y)))
                      (and (number? x) (number? y))
                      (- x y)
                      :else
                      [:numeric-minus x y]))
   :numeric-plus (fn [x y]
                   (cond
                     (and (instance? Date x)
                          (instance? TemporalAmount y))
                     (Date/from (.toInstant (.plus (.atOffset (.toInstant ^Date x) ZoneOffset/UTC) ^TemporalAmount y)))
                     (and (number? x) (number? y))
                     (+ x y)
                     :else
                     [:numeric-plus x y]))
   :numeric-multiply (fn [x y]
                       (if (and (number? x) (number? y))
                         (* x y)
                         [:numeric-multiply x y]))
   :numeric-divide (fn [x y]
                     (if (and (number? x) (number? y))
                       (/ x y)
                       [:numeric-divide x y]))
   :numeric-modulo (fn [x y]
                     (if (and (number? x) (number? y))
                       (rem x y)
                       [:numeric-modulo x y]))
   :like-exp (fn
               ([x pattern]
                [:like-exp x pattern])
               ([x not pattern]
                [:boolean-not [:like-exp x pattern]]))
   :in-exp (fn
               ([x y]
                [:in-exp x y])
               ([x not y]
                [:boolean-not [:in-exp x y]]))
   :in-value-list hash-set
   :routine-invocation (fn [f & args]
                         (case f
                           'date (apply i/read-instant-date args)
                           (vec (cons :routine-invocation (cons f args)))))
   :between-exp (fn
                  ([v x y]
                   [:boolean-and
                    [:comp-ge v x]
                    [:comp-le v y]])
                  ([v not x y]
                   [:boolean-or
                    [:comp-lt v x]
                    [:comp-gt v y]]))})

(def nary-transform
  {:boolean-and (fn [x y]
                  (cond
                    (and (vector? x)
                         (= :boolean-and (first x))
                         (vector? y)
                         (= :boolean-and (first y)))
                    (apply conj x (rest y))
                    (and (vector? x)
                         (= :boolean-and (first x)))
                    (conj x y)
                    :else
                    [:boolean-and x y]))
   :boolean-or (fn [x y]
                 (cond
                   (and (vector? x)
                        (= :boolean-or (first x))
                        (vector? y)
                        (= :boolean-or (first y)))
                   (apply conj x (rest y))
                   (and (vector? x)
                        (= :boolean-or (first x)))
                   (conj x y)
                   :else
                   [:boolean-or x y]))})

(defn symbol-suffix [x]
  (symbol (s/replace x #"^.+\." "")))

(defn symbol-prefix [x]
  (symbol (s/replace x #"\..+$" "")))

(defn normalize-where [where]
  (cond
    (set? where)
    where
    (= :and (first where))
    (set (rest where))
    :else
    #{where}))

(defn find-joins [where]
  (->> (for [x (normalize-where where)
             :when (= := (first x))
             :let [[_ a b] x]
             :when (and (symbol? a)
                        (symbol? b)
                        (not= (symbol-prefix a)
                              (symbol-prefix b)))]
         [(symbol-prefix a)
          (symbol-prefix b)
          {(symbol-suffix a)
           (symbol-suffix b)}
          x])))

(defn join-order [db joins]
  (sort-by (fn [[x y]]
             (* (count (get db (str x)))
                (count (get db (str y)))))
           joins))

(defn find-selections-for-attrs [where known-attrs]
  (let [known-attrs (set (map symbol known-attrs))]
    (set (for [x (normalize-where where)
               :when (set/superset? known-attrs (set (map symbol-suffix (filter symbol? x))))]
           x))))

(defn find-base-selections [where]
  (->> (for [x (normalize-where where)
             :when (and (= 3 (count x))
                        (= 1 (count (distinct (map symbol-prefix (filter symbol? x)))))
                        (not-any? map? x))
             :let [[a] (filter symbol? x)]]
         {(symbol-prefix a) #{x}})
       (apply merge-with set/union)))

(def normalize-transform
  (merge
   {:table-spec (fn [x & [y]]
                  (if (vector? x)
                    x
                    [x (or y x)]))
    :select-item (fn [x & [y]]
                   [x (or y (if (symbol? x)
                              (symbol-suffix x)
                              (gensym "column_")))])
    :sort-spec (fn [x & [dir]]
                 [x (or dir :asc)])
    :set-function-spec (fn [& args]
                         (vec args))}
   (let [constants [:count :sum :avg :min :max :star :distinct :asc :desc :year :month :day :hour :minute]]
     (zipmap constants (map constantly constants)))))

(def map-transform
  {:select-exp (fn [& args]
                 (let [select (zipmap (map first args)
                                      (map (comp vec rest) args))]
                   (reduce
                    (fn [acc k]
                      (cond-> acc
                        (contains? acc k)
                        (update k first)))
                    select [:where :having :offset :limit])))})

(def simplify-transform
  (->> (for [[x y] {:like-exp :like
                    :in-exp :in
                    :case-exp :case
                    :exists-exp :exists
                    :extract-exp :extract
                    :match-exp :match
                    :unique-exp :unique
                    :all-exp :all
                    :any-exp :any
                    :numeric-multiply :*
                    :numeric-divide :/
                    :numeric-plus :+
                    :numeric-minus :-
                    :numeric-modulo :%
                    :boolean-and :and
                    :boolean-or :or
                    :boolean-not :not
                    :comp-eq :=
                    :comp-ne :<>
                    :comp-lt :<
                    :comp-le :<=
                    :comp-gt :>
                    :comp-ge :>=}]
         [x (fn [& args]
              (vec (cons y args)))])
       (into {})))

(defn qualify-transform [column->tables]
  {:identifier (fn [x]
                 (if-let [ts (get column->tables (name x))]
                   (if (= 1 (count ts))
                     (symbol (str (first ts) "." (name x)))
                     (throw (IllegalArgumentException. (str "Column not unique:" x))))
                   (symbol x)))})

(defn remove-symbol-prefixes [x]
  (w/postwalk #(if (and (symbol? %) (nil? (namespace %)))
                 (symbol-suffix %)
                 %) x))

(defn find-symbol-suffixes [x]
  (let [acc (volatile! #{})]
    (w/postwalk #(do (when (and (symbol? %) (nil? (namespace %)))
                       (vswap! acc conj (symbol-suffix %)))
                     %) x)
    @acc))

(def codegen-transform
  {:not (fn [x]
          `(not ~x))
   :and (fn [& xs]
          `(and ~@xs))
   :or (fn [& xs]
         `(or ~@xs))
   := (fn [x y]
        `(= ~x ~y))
   :<> (fn [x y]
         `(not= ~x ~y))
   :< (fn [x y]
        (if (or (number? x) (number? y))
          `(< ~x ~y)
          `(neg? (compare ~x ~y))))
   :<= (fn [x y]
         (if (or (number? x) (number? y))
           `(<= ~x ~y)
           `(not (pos? (compare ~x ~y)))))
   :> (fn [x y]
        (if (or (number? x) (number? y))
          `(> ~x ~y)
          `(pos? (compare ~x ~y))))
   :>= (fn [x y]
         (if (or (number? x) (number? y))
           `(>= ~x ~y)
           `(not (neg? (compare ~x ~y)))))
   :+ (fn [x y]
        `(+ ~x ~y))
   :- (fn [x y]
        `(- ~x ~y))
   :* (fn [x y]
        `(* ~x ~y))
   :/ (fn [x y]
        `(/ ~x ~y))
   :% (fn [x y]
        `(rem ~x ~y))
   :like (fn [x pattern]
           `(boolean (re-find ~pattern ~x)))
   :case (fn [cond then else]
           `(if ~cond ~then ~else))
   :extract (fn [field x]
              `(.get (.atOffset (.toInstant ~(if (symbol? x)
                                               (with-meta x {:tag `Date})
                                               x)) ZoneOffset/UTC)
                     ~(case field
                        :year `ChronoField/YEAR
                        :month `ChronoField/MONTH_OF_YEAR
                        :day `ChronoField/DAY_OF_MONTH
                        :hour `ChronoField/HOUR_OF_DAY
                        :minute `ChronoField/MINUTE_OF_HOUR)))
   :routine-invocation (fn [f & args]
                         (case f
                           (substr
                            substring) (let [[x start length] args]
                            `(subs ~x (dec ~start) (+ (dec ~start) ~length)))
                           `(~f ~@args)))
   :sum (fn [x]
          `(fn [group#]
             (reduce
              (fn [acc# {:strs ~(vec (find-symbol-suffixes x))}]
                (+ acc# ~(remove-symbol-prefixes x)))
              0 group#)))
   :avg (fn [x]
          `(fn [group#]
             (/ (reduce
                 (fn [acc# {:strs ~(vec (find-symbol-suffixes x))}]
                   (+ acc# ~(remove-symbol-prefixes x)))
                 0 group#)
                (count group#))))
   :min (fn [x]
          `(fn [group#]
             (reduce
              (fn [acc# {:strs ~(vec (find-symbol-suffixes x))}]
                (min acc# ~(remove-symbol-prefixes x)))
              Long/MAX_VALUE group#)))
   :max (fn [x]
          `(fn [group#]
             (reduce
              (fn [acc# {:strs ~(vec (find-symbol-suffixes x))}]
                (max acc# ~(remove-symbol-prefixes x)))
              Long/MIN_VALUE group#)))
   :count (fn [x]
            (if (= :star x)
              `(fn [group#]
                 (count group#))
              `(fn [group#]
                 (count (map (fn [{:strs ~(vec (find-symbol-suffixes x))}]
                               ~(remove-symbol-prefixes x))
                             group#)))))})

(defn maybe-add-group-by [{:keys [group-by select] :as q}]
  (if (or group-by (every? symbol? (map first select)))
    q
    (assoc q :group-by [])))

(defn codegen-predicate [pred]
  `(fn [{:strs ~(vec (find-symbol-suffixes pred))}]
     ~(insta/transform codegen-transform (remove-symbol-prefixes pred))))

(defn codegen-select [{:keys [select]}]
  `(fn [result#]
     (->> (set/project result# ~(for [[_ as] select]
                                  (str (symbol-suffix as))))
          (remove empty?))))

(defn codegen-group-by [{:keys [select group-by having]}]
  (when group-by
    (let [result-var (gensym 'result)
          group-var (gensym 'group)]
      `(fn [~result-var]
         ~(cond->> `(->> (group-by (fn [{:strs ~(mapv symbol-suffix group-by)}]
                                     ~(mapv symbol-suffix group-by))
                                   ~result-var)
                         (vals)
                         (remove empty?)
                         (into #{} (map (fn [[{:strs ~(mapv symbol-suffix group-by)} :as ~group-var]]
                                          (hash-map
                                           ~@(->> (for [[exp as] select]
                                                    [(str (symbol-suffix as))
                                                     (if (symbol? exp)
                                                       (symbol-suffix exp)
                                                       `(~(insta/transform codegen-transform (remove-symbol-prefixes exp))
                                                         ~group-var))])
                                                  (reduce into [])))))))
            having (list `set/select (codegen-predicate having)))))))

(defn codegen-order-by [{:keys [order-by]}]
  (when order-by
    `(fn [result#]
       (sort (-> ~@(reduce
                    (fn [acc [col dir]]
                      (cond->> `(Comparator/comparing
                                 (reify Function
                                   (apply [_# {:strs [~(symbol-suffix col)]}]
                                     ~(symbol-suffix col))))
                        (= :desc dir) (list '.reversed)
                        (not-empty acc) (list '.thenComparing)
                        true (conj acc)))
                    []
                    order-by)) result#))))

(defn codegen-offset-limit [{:keys [offset limit] :as query}]
  (when (or offset limit)
    `(fn [result#]
       (into [] (comp ~@(concat
                         (when offset
                           [`(drop ~offset)])
                         (when limit
                           [`(take ~limit)])))
             result#))))

(comment
  (for [q (map inc (range 22))]
    (parse-sql (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" q)))))

  (reduce
   (fn [acc transform-map]
     (insta/transform transform-map acc))
   (parse-sql (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 16))))
   [(qualify-transform (crux.tpch/build-column->tables crux.tpch/db-sf-0_01))
    literal-transform
    constant-folding-transform
    nary-transform
    simplify-transform
    normalize-transform
    map-transform]))

;; High level SQL grammar, from
;; https://calcite.apache.org/docs/reference.html

;; See also Date, SQL and Relational Theory, p. 455-458, A Simplified
;; BNF Grammar

;; SQLite grammar:
;; https://github.com/bkiers/sqlite-parser/blob/master/src/main/antlr4/nl/bigo/sqliteparser/SQLite.g4
;; https://www.sqlite.org/lang_select.html

;; SQL BNF from the spec:
;; https://ronsavage.github.io/SQL/

;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/frontend/parser/SQLAST.scala
;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/frontend/parser/SQLParser.scala

;; RelaX - relational algebra calculator
;; https://dbis-uibk.github.io/relax/

;; https://www.academia.edu/2280992/Translating_SQL_Into_Relational_Algebra_Optimization_Semantics_and_Equivalence_of_SQL_Queries
;; https://www.cs.purdue.edu/homes/rompf/papers/rompf-icfp15.pdf

;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/queryengine/push/Operators.scala
;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/queryengine/volcano/Operators.scala
