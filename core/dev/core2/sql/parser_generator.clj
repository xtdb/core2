(ns core2.sql.parser-generator
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [instaparse.core :as insta])
  (:import java.io.File))

(set! *unchecked-math* :warn-on-boxed)

(def sql-spec-grammar-ebnf "
spec: (HEADER_COMMENT / definition)* ;
definition: NAME <'::='> <'|'>? syntax? ;
syntax_element: (optional / mandatory / NAME / TOKEN) REPEATABLE? ;
syntax: syntax_element+ choice* ;
optional: <'['> syntax+ <']'> ;
mandatory: <'{'> syntax+ <'}'> ;
choice: <'|'> syntax_element+ ;
REPEATABLE: <'...'> ;
NAME: #'<[-_:/a-zA-Z 0-9]+?>' ;
TOKEN: !'::=' #'[^ |\\n\\r\\t.!/]+' ;
HEADER_COMMENT: #'// *\\d.*?\\n' ;
        ")

(def parse-sql-spec
  (insta/parser sql-spec-grammar-ebnf
                :auto-whitespace (insta/parser "
whitespace: (#'\\s*//\\s*' !#'\\d' #'.*?\\n\\s*' | #'\\s*' | #'!!.*?\\n')+")))

(def ^:private reserved-words
  ["NULL" "WITH" "SELECT" "FROM" "WHERE"
   "GROUP" "HAVING" "ORDER" "OFFSET" "FETCH"
   "UNION" "EXCEPT" "INTERSECT"
   "CURRENT_TIME" "CURRENT_TIMESTAMP" "CURRENT_DATE" "LOCALTIME" "LOCALTIMESTAMP"])

;; NOTE: A rule must exist to be overridden and cannot be commented
;; out. This is to ensure the override ends up in the right place in
;; the grammar.
(def rule-overrides
  (-> '{<space> "' '"
        <quote> "'\\''"
        <period> "'.'"
        solidus "'/'"
        <left_bracket> "'['"
        <right_bracket> "']'"
        vertical_bar "'|'"
        concatenation_operator "'||'"
        <left_brace> "'{'"
        <right_brace> "'}'"
        delimited_identifier
        "#'\"(\"\"|[^\"])+\"'"
        ;; replaces <local or schema qualified name>
        table_name
        "identifier"
        exact_numeric_literal
        "#'(\\d*\\.\\d+|\\d+\\.\\d*|\\d+)'"
        unsigned_integer
        "#'[0-9]+'"
        large_object_length_token
        "#'[0-9]+' multiplier"
        character_representation
        "#'(\\'\\'|[^\\'])*'"
        ;; removes <introducer> <character set specification>
        character_string_literal
        "#'\\'(\\'\\'|[^\\'])*\\''+"
        binary_string_literal
        "#'X(\\'[a-fA-F0-9\\s]+\\'\\s*)+'"
        host_parameter_name
        "<colon> #'[a-zA-Z][a-zA-Z0-9_]*'"
        ;; removes <indicator parameter>
        host_parameter_specification
        "host_parameter_name"
        ;; removes <path-resolved user-defined type name> and <reference type>
        predefined_type
        "character_string_type
    / binary_string_type
    / numeric_type
    / boolean_type
    / datetime_type
    / interval_type"
        ;; removes <collate clause>
        character_factor
        "character_primary"
        ;; removes <sample clause>
        table_factor
        "table_primary"
        ;; adds check for reserved words in <correlation or recognition> from SQL:2016
        table_primary
        "table_or_query_name [ query_system_time_period_specification ] [ correlation_or_recognition ]
    / derived_table correlation_or_recognition
    / lateral_derived_table correlation_or_recognition
    / collection_derived_table correlation_or_recognition
    / parenthesized_joined_table"
        ;; removes support for more than one <collection value expression>.
        collection_derived_table
        "'UNNEST' <left_paren> collection_value_expression <right_paren> [ 'WITH' 'ORDINALITY' ]"
        ;; adds check for reserved words
        as_clause
        "( 'AS' column_name ) | !#'(?i)(\\b(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)\\b)' column_name"
        ;; removes <search or cycle clause>
        with_list_element
        "query_name [ <left_paren> with_column_list <right_paren> ] 'AS' table_subquery"
        ;; adds <trigonometric function>, <general logarithm function> and <common logarithm> from SQL:2016
        numeric_value_function
        "position_expression
    / regex_occurrences_function
    / regex_position_expression
    / extract_expression
    / length_expression
    / cardinality_expression
    / max_cardinality_expression
    / absolute_value_expression
    / modulus_expression
    / trigonometric_function
    / general_logarithm_function
    / common_logarithm
    / natural_logarithm
    / exponential_function
    / power_function
    / square_root
    / floor_function
    / ceiling_function"
        ;; removes <collate clause>
        grouping_column_reference
        "column_reference"
        aggregate_function
        ;; removes <filter clause>, <binary set function> and <ordered set function>
        "'COUNT' <left_paren> asterisk <right_paren>
    / general_set_function
    / array_aggregate_function"
        ;; precedence issue - was confusing `CURRENT_TIMESTAMP` for `CURRENT_TIME STAMP`
        datetime_value_function
        "current_date_value_function
    / current_timestamp_value_function
    / current_time_value_function
    / current_local_timestamp_value_function
    / current_local_time_value_function"
        ;; removes <partitioned join table>
        qualified_join
        "table_reference [ join_type ] 'JOIN' table_reference join_specification"
        ;; removes <corresponding spec>
        query_expression_body
        "query_term
    / query_expression_body 'UNION' [ 'ALL' / 'DISTINCT' ] query_term
    / query_expression_body 'EXCEPT' [ 'ALL' / 'DISTINCT' ] query_term"
        ;; removes <corresponding spec>
        query_term
        "query_primary
    / query_term 'INTERSECT' [ 'ALL' / 'DISTINCT' ] query_primary"
        ;; removes <order by clause>, <result offset clause, and <fetch first clause>
        query_primary
        "simple_table
    / <left_paren> query_expression_body <right_paren>"
        ;; inlines <cursor specification> and removes <updatability clause>
        direct_select_statement__multiple_rows
        "query_expression"}

      ;; adds check for reserved words, these should really be allowed after 'AS'
      (assoc 'regular_identifier
             (format "!#'(?i)(\\b(%s)\\b)' #'[a-zA-Z][a-zA-Z0-9_]*'"
                     (str/join "|" reserved-words)))))

(def ^:private delimiter-set
  '#{space
     colon
     semicolon
     quote
     period
     comma
     left_paren
     right_paren
     left_bracket
     right_bracket
     left_bracket_trigraph
     right_bracket_trigraph
     left_brace
     right_brace})

(def ^:private redundant-non-terminal-overrides
  '#{table_name
     host_parameter_specification
     numeric_value_function
     predefined_type
     character_factor
     parenthesized_value_expression
     parenthesized_boolean_value_expression
     table_factor
     parenthesized_joined_table
     grouping_column_reference
     direct_select_statement__multiple_rows
     datetime_value_function})

(def ^:private keep-non-terminal-overrides
  '#{column_reference
     case_operand
     row_value_constructor_element
     contextually_typed_row_value_constructor_element
     directly_executable_statement
     dynamic_parameter_specification})

(def sql2016-numeric-value-function
  "(* SQL:2016 7.6 <table reference> *)

<correlation_or_recognition>
    : ( ( 'AS' correlation_name ) | !#'(?i)(\\b(ON|JOIN|INNER|LEFT|RIGHT|USING|OUTER)\\b)' correlation_name ) [ <left_paren> derived_column_list <right_paren> ]

(* SQL:2016 6.30 <numeric value function> *)

trigonometric_function
    : trigonometric_function_name <left_paren> numeric_value_expression <right_paren>
    ;

trigonometric_function_name
    : 'SIN'
    / 'COS'
    / 'TAN'
    / 'SINH'
    / 'COSH'
    / 'TANH'
    / 'ASIN'
    / 'ACOS'
    / 'ATAN'
    ;

general_logarithm_function
    : 'LOG' <left_paren> general_logarithm_base <comma> general_logarithm_argument <right_paren>
    ;

<general_logarithm_base>
    : numeric_value_expression
    ;

<general_logarithm_argument>
    : numeric_value_expression
    ;

common_logarithm
    : 'LOG10' <left_paren> numeric_value_expression <right_paren>
    ;
")

(def extra-rules (str/join "\n" [sql2016-numeric-value-function]))

(def ^:private ^:dynamic *sql-ast-print-nesting* 0)
(def ^:private sql-print-indent "    ")
(def ^:private ^:dynamic *sql-redundant-non-terminals* #{})

(defn- redundant-non-terminal [[_ n [_ & xs]]]
  (let [single-name? (fn [x]
                       (and (= :syntax_element (first x))
                            (= :NAME (first (second x)))
                            (= 1 (count (rest x)))))
        names (->> (for [[^long idx x] (map-indexed vector xs)
                         :when (and (vector? x)
                                    (if (zero? idx)
                                      (single-name? x)
                                      (and (= :choice (first x))
                                           (single-name? (second x))
                                           (= 1 (count (rest x))))))]
                     x)
                   (flatten)
                   (filter string?))]
    (when (= (count xs) (count names))
      (second n))))

(defmulti print-sql-ast first)

(defn print-sql-ast-list [xs]
  (loop [[x & [next-x :as xs]] xs]
    (when x
      (print-sql-ast x)
      (when next-x
        (print " "))
      (recur xs))))

(defmethod print-sql-ast :spec [[_ & xs]]
  (doseq [x xs]
    (print-sql-ast x)))

(defmethod print-sql-ast :HEADER_COMMENT [[_ x]]
  (println)
  (println "(*" (str/replace (str/trim x) #"^//" "") "*)"))

(defmethod print-sql-ast :TOKEN [[_ x]]
  (print (str "'" x "'")))

(defmethod print-sql-ast :NAME [[_ x]]
  (let [x (str/lower-case x)
        x (subs x 1 (dec (count x)))
        x (str/replace x #"[-: ]" "_")]
    (if (contains? delimiter-set (symbol x))
      (print (str "<" x ">"))
      (print x))))

(defmethod print-sql-ast :REPEATABLE [_]
  (print "+"))

(defmethod print-sql-ast :choice [[_ & xs]]
  (if (pos? (long *sql-ast-print-nesting*))
    (do (print "/ ")
        (print-sql-ast-list xs))
    (do (println)
        (print (str sql-print-indent "/ "))
        (print-sql-ast-list xs))))

(defmethod print-sql-ast :optional [[_ & xs]]
  (binding [*sql-ast-print-nesting* (inc (long *sql-ast-print-nesting*))]
    (print "[ ")
    (print-sql-ast-list xs)
    (print " ]")))

(defmethod print-sql-ast :mandatory [[_ & xs]]
  (binding [*sql-ast-print-nesting* (inc (long *sql-ast-print-nesting*))]
    (print "( ")
    (print-sql-ast-list xs)
    (print " )")))

(defmethod print-sql-ast :syntax_element [[_ x repeatable?]]
  (print-sql-ast x)
  (when repeatable?
    (print-sql-ast repeatable?)))

(defmethod print-sql-ast :syntax [[_ & xs]]
  (print-sql-ast-list xs))

(defmethod print-sql-ast :definition [[_ n & xs]]
  (let [redundant-non-terminal? (contains? *sql-redundant-non-terminals* (second n))
        n (symbol (with-out-str
                    (print-sql-ast n)))]
    (println)
    (if (and (not (contains? keep-non-terminal-overrides n))
             (or (contains? redundant-non-terminal-overrides n)
                 (and (not (contains? rule-overrides n))
                      redundant-non-terminal?)))
      (println (str "<" n ">"))
      (println n))
    (print sql-print-indent)
    (print ": ")
    (if-let [override (get rule-overrides n)]
      (do (println override)
          (print sql-print-indent)
          (println ";"))
      (do (print-sql-ast-list xs)
          (println)
          (print sql-print-indent)
          (println ";")))))

(defn sql-spec-ast->ebnf-grammar-string [extra-rules sql-ast]
  (binding [*sql-redundant-non-terminals* (set (keep redundant-non-terminal (rest sql-ast)))]
    (->> (with-out-str
           (print-sql-ast sql-ast)
           (println)
           (println extra-rules))
         (str/split-lines)
         (map str/trimr)
         (str/join "\n"))))

(def sql2011-grammar-file (File. (.toURI (io/resource "core2/sql/SQL2011.ebnf"))))
(def sql2011-spec-file (File. (.toURI (io/resource "core2/sql/SQL2011.txt"))))

(defn generate-parser [sql-spec-file ebnf-grammar-file]
  (->> (parse-sql-spec (slurp sql-spec-file))
       (sql-spec-ast->ebnf-grammar-string extra-rules)
       (spit ebnf-grammar-file)))

(defn -main [& _args]
  (generate-parser sql2011-spec-file sql2011-grammar-file))
