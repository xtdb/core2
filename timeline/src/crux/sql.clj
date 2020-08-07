(ns crux.sql
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.instant :as i]
            [clojure.set :as set]
            [clojure.string :as s]
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
  {:boolean-not (fn [x]
                  (if (boolean? x)
                    (not x)
                    [:boolean-not x]))
   :boolean-and (fn [x y]
                  (if (and (boolean? x) (boolean? y))
                    (and x y)
                    [:boolean-and x y]))
   :boolean-or (fn [x y]
                 (if (and (boolean? x) (boolean? y))
                   (or x y)
                   [:boolean-or x y]))
   :comp-eq (fn [x y]
              (if (or (symbol? x)
                      (symbol? y))
                [:comp-eq x y]
                (= x y)))
   :comp-lt (fn [x y]
              (if (or (symbol? x)
                      (symbol? y))
                [:comp-lt x y]
                (neg? (compare x y))))
   :comp-le (fn [x y]
              (if (or (symbol? x)
                      (symbol? y))
                [:comp-le x y]
                (not (pos? (compare x y)))))
   :comp-gt (fn [x y]
              (if (or (symbol? x)
                      (symbol? y))
                [:comp-gt x y]
                (pos? (compare x y))))
   :comp-ge (fn [x y]
              (if (or (symbol? x)
                      (symbol? y))
                [:comp-ge x y]
                (not (neg? (compare x y)))))
   :comp-ne (fn [x y]
              (if (or (symbol? x)
                      (symbol? y))
                [:comp-ne x y]
                (not= x y)))
   :numeric-minus (fn [x y]
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
   :between-exp (fn
                  ([v x y]
                   [:boolean-and
                    [:comp-ge v x]
                    [:comp-le v y]])
                  ([v not x y]
                   [:boolean-or
                    [:comp-lt v x]
                    [:comp-gt v y]]))})

(def codegen-transform
  {:boolean-not (fn [x]
                  `(not ~x))
   :boolean-and (fn [x y]
                  `(and ~x ~y))
   :boolean-or (fn [x y]
                 `(or ~x ~y))
   :comp-eq (fn [x y]
              `(= ~x ~y))
   :comp-ne (fn [x y]
              `(not= ~x ~y))
   :comp-lt (fn [x y]
              (if (or (number? x) (number? y))
                `(< ~x ~y)
                `(neg? (compare ~x ~y))))
   :comp-le (fn [x y]
              (if (or (number? x) (number? y))
                `(<= ~x ~y)
                `(not (pos? (compare ~x ~y)))))
   :comp-gt (fn [x y]
              (if (or (number? x) (number? y))
                `(> ~x ~y)
                `(pos? (compare ~x ~y))))
   :comp-ge (fn [x y]
              (if (or (number? x) (number? y))
                `(>= ~x ~y)
                `(not (neg? (compare ~x ~y)))))
   :numeric-plus (fn [x y]
                   `(+ ~x ~y))
   :numeric-minus (fn [x y]
                    `(- ~x ~y))
   :numeric-multiply (fn [x y]
                       `(* ~x ~y))
   :numeric-divide (fn [x y]
                     `(/ ~x ~y))
   :like-exp (fn [x pattern]
               `(boolean (re-find ~pattern ~x)))
   :case-exp (fn [cond then else]
               `(if ~cond ~then ~else))
   :extract-exp (fn [[field] x]
                  `(.get (.atOffset (.toInstant ~x) ZoneOffset/UTC)
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
                           `(~f ~@args)))})

(comment
  (for [q (map inc (range 22))]
    (parse-sql (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" q))))))

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


(comment
  (def db (->> (for [[t ts] (group-by (comp :table meta) (crux.timeline-test/tpch-dbgen 0.01))]
                 [t (set ts)])
               (into {})))

  ;; See https://github.com/cwida/duckdb/tree/master/third_party/dbgen/answers

  ;; (parse-sql (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1))))

  (defn tpch-01 [{:strs [lineitem] :as db}]
    ;; :select-exp
    (->> (set/select
          #_[:where
             [:comp-le
              [:identifier "l_shipdate"]
              [:numeric-minus
               [:date-literal "'1998-12-01'"]
               [:interval-literal "'90'" [:day]]]]]
          (fn [{:strs [l_shipdate]}]
            (not (pos? (compare l_shipdate #inst "1998-09-02T00:00:00.000-00:00"))))
          #_[:from [:table-spec [:identifier "lineitem"]]]
          lineitem)
         #_[:group-by
            [:identifier "l_returnflag"]
            [:identifier "l_linestatus"]]
         (group-by (fn [{:strs [l_returnflag l_linestatus]}]
                     [l_returnflag l_linestatus]))
         (vals)
         ;; :select
         (into #{} (map (fn [[{:strs [l_returnflag l_linestatus]} :as ts]]
                          {#_[:select-item [:identifier "l_returnflag"]]
                           "l_returnflag" l_returnflag
                           #_[:select-item [:identifier "l_linestatus"]]
                           "l_linestatus" l_linestatus
                           #_[:select-item
                              [:set-function-spec [:sum] [:identifier "l_quantity"]]
                              [:identifier "sum_qty"]]
                           "sum_qty" (reduce (fn [acc {:strs [l_quantity]}]
                                               (+ acc l_quantity))
                                             0 ts)
                           #_[:select-item
                              [:set-function-spec [:sum] [:identifier "l_extendedprice"]]
                              [:identifier "sum_base_price"]]
                           "sum_base_price" (reduce (fn [acc {:strs [l_extendedprice]}]
                                                      (+ acc l_extendedprice))
                                                    0 ts)
                           #_[:select-item
                              [:set-function-spec
                               [:sum]
                               [:numeric-multiply
                                [:identifier "l_extendedprice"]
                                [:numeric-minus
                                 [:numeric-literal "1"]
                                 [:identifier "l_discount"]]]]
                              [:identifier "sum_disc_price"]]
                           "sum_disc_price" (reduce (fn [acc {:strs [l_extendedprice l_discount]}]
                                                      (+ acc (* l_extendedprice
                                                                (- 1 l_discount))))
                                                    0 ts)
                           #_[:select-item
                              [:set-function-spec
                               [:sum]
                               [:numeric-multiply
                                [:numeric-multiply
                                 [:identifier "l_extendedprice"]
                                 [:numeric-minus
                                  [:numeric-literal "1"]
                                  [:identifier "l_discount"]]]
                                [:numeric-plus [:numeric-literal "1"] [:identifier "l_tax"]]]]
                              [:identifier "sum_charge"]]
                           "sum_charge" (reduce (fn [acc  {:strs [l_extendedprice l_discount l_tax]}]
                                                  (+ (* l_extendedprice
                                                        (- 1 l_discount)
                                                        (+ 1 l_tax))))
                                                0 ts)
                           #_[:select-item
                              [:set-function-spec [:avg] [:identifier "l_quantity"]]
                              [:identifier "avg_qty"]]
                           "avg_qty" (/ (reduce (fn [acc {:strs [l_quantity]}]
                                                  (+ acc l_quantity))
                                                0 ts)
                                        (count ts))
                           #_[:select-item
                              [:set-function-spec [:avg] [:identifier "l_extendedprice"]]
                              [:identifier "avg_price"]]
                           "avg_price" (/ (reduce (fn [acc {:strs [l_extendedprice]}]
                                                    (+ acc l_extendedprice))
                                                  0 ts)
                                          (count ts))
                           #_[:select-item
                              [:set-function-spec [:avg] [:identifier "l_discount"]]
                              [:identifier "avg_disc"]]
                           "avg_disc" (/ (reduce (fn [acc {:strs [l_discount]}]
                                                   (+ acc l_discount))
                                                 0 ts)
                                         (count ts))
                           #_[:select-item
                              [:set-function-spec [:count] [:star]]
                              [:identifier "count_order"]]
                           "count_order" (count ts)})))
         #_[:order-by
            [:sort-spec [:identifier "l_returnflag"]]
            [:sort-spec [:identifier "l_linestatus"]]]
         (sort (-> (Comparator/comparing
                    (reify Function
                      (apply [_ {:strs [l_returnflag]}]
                        l_returnflag)))
                   (.thenComparing
                    (Comparator/comparing
                     (reify Function
                       (apply [_ {:strs [l_linestatus]}]
                         l_linestatus)))))))))
