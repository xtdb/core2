(ns core2.sql.logic-test.xtdb-engine
  (:require [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as z]
            [core2.api :as c2]
            [core2.rewrite :as r]
            [core2.snapshot :as snap]
            [core2.sql :as sql]
            [core2.sql.logic-test.runner :as slt]
            [core2.operator :as op]
            [core2.test-util :as tu]
            [instaparse.core :as insta])
  (:import java.util.UUID
           core2.local_node.Node))

(defn- create-table [node {:keys [table-name columns]}]
  (assert (nil? (get-in node [:tables table-name])))
  (assoc-in node [:tables table-name] columns))

(defn- create-view [node {:keys [view-name as]}]
  (assert (nil? (get-in node [:views view-name])))
  (assoc-in node [:views view-name] as))

(defn- execute-record [node record]
  (case (:type record)
    :create-table (create-table node record)
    :create-view (create-view node record)))

;; TODO: this is a temporary hack.
(defn- normalize-literal [x]
  (if (vector? x)
    (case (first x)
      :boolean_literal (= "TRUE" (second x))
      ;; TODO: is parsing NULL as an identifier correct?
      :regular_identifier (if (= "NULL" (str/upper-case (second x)))
                            nil
                            x)
      :character_string_literal (let [s (second x)]
                                  (subs s 1 (dec (count s))))
      :exact_numeric_literal (let [s (str/join "." (remove keyword? (flatten x)))]
                               (if (= 1 (count (rest x)))
                                 (Long/parseLong s)
                                 (Double/parseDouble s)))
      ;; TODO: should this parse as signed_numeric_literal?
      :factor (if (and (= 2 (count (rest x)))
                       (= [:minus_sign "-"] (second x)))
                (- (first (filter number? (flatten x))))
                x)
      x)
    x))

(defn insert->doc [tables insert-statement]
  (let [[_ _ _ insertion-target insert-columns-and-source] insert-statement
        table (first (filter string? (flatten insertion-target)))
        from-subquery insert-columns-and-source
        columns (if (= 1 (count (rest from-subquery)))
                  (get tables table)
                  (let [insert-column-list (second from-subquery)]
                    (->> (flatten insert-column-list)
                         (filter string?))))
        query-expression (last from-subquery)
        [_ values] (->> query-expression
                        (w/postwalk normalize-literal)
                        (flatten)
                        (filter (some-fn number? string? boolean? nil?))
                        (split-with #{"VALUES"}))]
    (merge {:_table table} (zipmap (map keyword columns) values))))

(defn- insert-statement [{:keys [tables] :as node} insert-statement]
  (let [doc (insert->doc tables insert-statement)]
    (-> (c2/submit-tx node [[:put (merge {:_id (UUID/randomUUID)} doc)]])
        (tu/then-await-tx node))
    node))

(defn skip-statement? [^String x]
  (boolean (re-find #"(?is)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((.+)\)\s*$" x)))

(defn- execute-statement [node direct-sql-data-statement]
  (case (first direct-sql-data-statement)
    :insert_statement (insert-statement node direct-sql-data-statement)))

;; TODO:
;; - needs cleanup.
;; - this should really use proper rewrite rules.
;; - should be able to reuse basic rules.

(defn normalize-query [tables query]
  (letfn [(identifier [ag]
            (case (r/ctor ag)
              :identifier (identifier (r/$ ag 1))
              :regular_identifier (r/lexme ag 1)))
          (number [ag]
            (case (r/ctor ag)
              (:numeric_value_expression
               :term
               :factor
               :exact_numeric_literal) (number (r/$ ag 1))
              :unsigned_integer (r/lexme ag 1)))
          (qualify [ag]
            (case (r/ctor ag)
              :identifier_chain (z/node
                                 (if (r/single-child? ag)
                                   ;; TODO: does not take renamed tables into account.
                                   (let [column (identifier (r/$ ag 1))
                                         table (first (for [[table columns] tables
                                                            :when (contains? (set columns) column)]
                                                        table))]
                                     (z/replace ag [:identifier_chain
                                                    [:identifier
                                                     [:regular_identifier table]]
                                                    [:identifier
                                                     [:regular_identifier column]]]))
                                   ag))
              (if (z/branch? ag)
                (r/use-children qualify ag)
                (z/node ag))))
          (normalize [ag]
            (case (r/ctor ag)
              :derived_column (if (r/single-child? ag)
                                (let [column (str "col" (count (z/lefts ag)))]
                                  (conj (qualify ag)
                                        [:as_clause
                                         "AS"
                                         [:column_name
                                          [:identifier
                                           [:regular_identifier column]]]]))
                                (z/node ag))
              :identifier_chain (qualify ag)
              :sort_key (let [ordinal (number (r/$ ag 1))
                              column (str "col" ordinal)]
                          (z/node (z/replace ag [:sort_key
                                                 [:numeric_value_expression
                                                  [:term
                                                   [:factor
                                                    [:column_reference
                                                     [:basic_identifier_chain
                                                      [:identifier_chain
                                                       [:identifier
                                                        [:regular_identifier column]]]]]]]]])))
              (if (z/branch? ag)
                (r/use-children normalize ag)
                (z/node ag))))]
    (normalize (z/vector-zip query))))

(defn parse-create-table [^String x]
  (when-let [[_ table-name columns] (re-find #"(?is)^\s*CREATE\s+TABLE\s+(\w+)\s*\((.+)\)\s*$" x)]
    {:type :create-table
     :table-name table-name
     :columns (vec (for [column (str/split columns #",")]
                     (->> (str/split column #"\s+")
                          (remove str/blank?)
                          (first))))}))

(defn parse-create-view [^String x]
  (when-let [[_ view-name query] (re-find #"(?is)^\s*CREATE\s+VIEW\s+(\w+)\s+AS\s+(.+)\s*$" x)]
    {:type :create-view
     :view-name view-name
     :as query}))

(extend-protocol slt/DbEngine
  Node
  (get-engine-name [_] "xtdb")

  (execute-statement [this statement]
    (if (skip-statement? statement)
      this
      (let [tree (sql/parse statement :directly_executable_statement)]
        (if (insta/failure? tree)
          (if-let [record (or (parse-create-table statement)
                              (parse-create-view statement))]
            (execute-record this record))
          (let [direct-sql-data-statement-tree (second tree)]
            (execute-statement this direct-sql-data-statement-tree))))))

  (execute-query [this query]
    (let [tree (sql/parse query :query_expression)
          snapshot-factory (tu/component this ::snap/snapshot-factory)
          db (snap/snapshot snapshot-factory)]
      (when (insta/failure? tree)
        (throw (IllegalArgumentException. (prn-str (insta/get-failure tree)))))
      (let [tree (normalize-query (:tables this) tree)]
        (->> (op/query-ra '[:scan [_id]] db)
             (mapv (comp vec keys)))))))
