(ns core2.sql.parser-generator
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [instaparse.core :as insta])
  (:import java.io.File))

(set! *unchecked-math* :warn-on-boxed)

(def sql-spec-grammar-ebnf "
spec: (HEADER_COMMENT / definition)* ;
definition: NAME '::=' <'|'>? syntax? ;
syntax_element: (optional / mandatory / NAME / TOKEN) REPEATABLE? ;
syntax: syntax_element+ choice* ;
optional: '[' syntax+ ']' ;
mandatory: '{' syntax+ '}' ;
choice: '|' syntax_element+ ;
REPEATABLE: '...' ;
NAME: #'<[-_:/a-zA-Z 0-9]+?>' ;
TOKEN: !'::=' #'[^ |\\n\\r\\t.!/]+' ;
HEADER_COMMENT: #'// *\\d.*?\\n' ;
        ")

(def parse-sql-spec
  (insta/parser sql-spec-grammar-ebnf
                :auto-whitespace (insta/parser "
whitespace: (#'\\s*//\\s*' !#'\\d' #'.*?\\n\\s*' | #'\\s*' | #'!!.*?\\n')+")))

;; NOTE: A rule must exist to be overridden and cannot be commented
;; out. This is to ensure the override ends up in the right place in
;; the grammar.
(def rule-overrides
  {'space "' '"
   'quote "'\\''"
   'period "'.'"
   'solidus "'/'"
   'left_bracket "'['"
   'right_bracket "']'"
   'vertical_bar "'|'"
   'concatenation_operator "'||'"
   'left_brace "'{'"
   'right_brace "'}'"
   'regular_identifier
   "#'[a-zA-Z][a-zA-Z0-9_]*'"
   'delimited_identifier
   "#'\"(\"\"|[^\"])+\"'"
   ;; replaces <local or schema qualified name>
   'table_name
   "identifier"
   'unsigned_integer
   "#'[0-9]+'"
   'large_object_length_token
   "#'[0-9]+' multiplier"
   'character_representation
   "#'(\\'\\'|[^\\'])*'"
   ;; removes <introducer> <character set specification>
   'character_string_literal
   "#'\\'(\\'\\'|[^\\'])*\\''+"
   'binary_string_literal
   "#'X(\\'[a-fA-F0-9\\s]+\\'\\s*)+'"
   ;; removes <indicator parameter>
   'host_parameter_specification
   "host_parameter_name"
   ;; removes <path-resolved user-defined type name> and <reference type>
   'predefined_type
   "character_string_type
    / binary_string_type
    / numeric_type
    / boolean_type
    / datetime_type
    / interval_type"
   ;; removes <collate clause>
   'character_factor
   "character_primary"
   ;; removes <sample clause>
   'table_factor
   "table_primary"
   ;; adds <graph table> from SQL:2022 Property Graph Queries
   'table_primary
   "table_or_query_name [ query_system_time_period_specification ] [ [ 'AS' ] correlation_name [ left_paren derived_column_list right_paren ] ]
    / derived_table [ 'AS' ] correlation_name [ left_paren derived_column_list right_paren ]
    / lateral_derived_table [ 'AS' ] correlation_name [ left_paren derived_column_list right_paren ]
    / collection_derived_table [ 'AS' ] correlation_name [ left_paren derived_column_list right_paren ]
    / table_function_derived_table [ 'AS' ] correlation_name [ left_paren derived_column_list right_paren ]
    / graph_table [ 'AS' ] correlation_name [ left_paren derived_column_list right_paren ]
    / parenthesized_joined_table"
   ;; removes <search or cycle clause>
   'with_list_element
   "query_name [ left_paren with_column_list right_paren ] 'AS' table_subquery"
   ;; adds <trigonometric function>, <general logarithm function> and <common logarithm> from SQL:2016
   'numeric_value_function
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
   'grouping_column_reference
   "column_reference"
   'aggregate_function
   ;; removes <filter clause>, <binary set function> and <ordered set function>
   "'COUNT' left_paren asterisk right_paren
    / general_set_function
    / array_aggregate_function"
   ;; removes <partitioned join table>
   'qualified_join
   "table_reference [ join_type ] 'JOIN' table_reference join_specification"
   ;; removes <partitioned join table>
   'natural_join
   "table_reference 'NATURAL' [ join_type ] 'JOIN' table_factor"})

(def sql2016-numeric-value-function
  "(* SQL:2016 6.30 <numeric value function> *)

trigonometric_function
    : trigonometric_function_name left_paren numeric_value_expression right_paren
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
    : 'LOG' left_paren general_logarithm_base comma general_logarithm_argument right_paren
    ;

general_logarithm_base
    : numeric_value_expression
    ;

general_logarithm_argument
    : numeric_value_expression
    ;

common_logarithm
    : 'LOG10' left_paren numeric_value_expression right_paren
    ;
")

(def sql2022-property-graph-queries
  "(* SQL:2022 Property Graph Queries *)

graph_table
    : 'GRAPH_TABLE' left_paren [ graph_name comma ] match_expression graph_table_columns_clause right_paren
    ;

graph_name
    : identifier
    ;

graph_table_columns_clause
    : 'COLUMNS' left_paren derived_column [ ( comma derived_column )+ ] right_paren
    ;

match_expression
    : [ 'OPTIONAL' | 'MANDATORY' ] 'MATCH' path_pattern_list [ where_clause ]
    ;

path_pattern_list
    : path_pattern ( comma path_pattern )*
    ;

path_pattern
    : [ path_variable '=' ] [ path_pattern_prefix ] path_pattern_expression
    ;

path_pattern_prefix
    : 'WALK'
    | 'TRAIL'
    | 'ACYCLIC'
    | 'SIMPLE'
    ;

path_pattern_expression
    : path_term
    ;

path_term
    : path

path
    : node_pattern ( edge_pattern node_pattern )*
    ;

node_pattern
    : left_paren element_pattern_filler right_paren
    ;

edge_pattern
    : ( full_edge_pointing_left | full_edge_undirected | full_edge_pointing_right ) [ edge_length ]
    ;

full_edge_pointing_left
    : '<-[' element_pattern_filler ']-'
    ;

full_edge_undirected
    : '~[' element_pattern_filler ']~'
    ;

full_edge_pointing_right
    : '-[' element_pattern_filler ']->'
    ;

element_pattern_filler
    : [ element_variable ] [ is_label_expression ] [ element_predicate ]
    ;

element_predicate
    : where_clause
    | [ left_brace property_list right_brace ]
    ;

property_list
    : property_key colon value_expression ( comma property_key colon value_expression )*
    ;

edge_length
    : left_brace edge_quantifier right_brace
    ;

edge_quantifier
    : unsigned_integer comma unsigned_integer
    | unsigned_integer
    ;

is_label_expression
    : [ 'IS' | colon ] label_expression
    ;

label_expression
    : label_term ( vertical_bar label_term )*
    ;

label_term
    : label_factor ( ampersand label_factor )*
    ;

label_factor
    : label_primary
    | label_negation
    ;

label_negation
    : '!' label_primary
    ;

label_primary
    : label
    | label_wildcard
    | parenthesized_label_expression
    ;

label
    : identifier
    ;

label_wildcard
    : percent
    ;

parenthesized_label_expression
    : left_paren label_expression right_paren
    | left_bracket label_expression right_bracket
    ;

path_variable
    : identifier
    ;

element_variable
    : identifier
    ;

property_key
    : identifier
    ;
")

(def extra-rules (->> [sql2016-numeric-value-function
                       sql2022-property-graph-queries]
                     (str/join "\n")))

(def ^:private ^:dynamic *sql-ast-print-nesting* 0)
(def ^:private sql-print-indent "    ")

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
    (print x)))

(defmethod print-sql-ast :REPEATABLE [[_ x]]
  (print "+"))

(defmethod print-sql-ast :choice [[_ _ & xs]]
  (if (pos? (long *sql-ast-print-nesting*))
    (do (print "/ ")
        (print-sql-ast-list xs))
    (do (println)
        (print (str sql-print-indent "/ "))
        (print-sql-ast-list xs))))

(defmethod print-sql-ast :optional [[_ _ & xs]]
  (binding [*sql-ast-print-nesting* (inc (long *sql-ast-print-nesting*))]
    (let [xs (butlast xs)]
      (print "[ ")
      (print-sql-ast-list xs)
      (print " ]"))))

(defmethod print-sql-ast :mandatory [[_ _ & xs]]
  (binding [*sql-ast-print-nesting* (inc (long *sql-ast-print-nesting*))]
    (let [xs (butlast xs)]
      (print "( ")
      (print-sql-ast-list xs)
      (print " )"))))

(defmethod print-sql-ast :syntax_element [[_ x repeatable?]]
  (print-sql-ast x)
  (when repeatable?
    (print-sql-ast repeatable?)))

(defmethod print-sql-ast :syntax [[_ & xs]]
  (print-sql-ast-list xs))

(defmethod print-sql-ast :definition [[_ n _ & xs]]
  (let [n (symbol (with-out-str
                    (print-sql-ast n)))]
    (println)
    (println n)
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
  (->> (with-out-str
         (print-sql-ast sql-ast)
         (println)
         (println extra-rules))
       (str/split-lines)
       (map str/trimr)
       (str/join "\n")))

(def sql2011-grammar-file (File. (.toURI (io/resource "core2/sql/SQL2011.ebnf"))))
(def sql2011-spec-file (File. (.toURI (io/resource "core2/sql/SQL2011.txt"))))

(defn generate-parser [sql-spec-file ebnf-grammar-file]
  (->> (parse-sql-spec (slurp sql-spec-file))
       (sql-spec-ast->ebnf-grammar-string extra-rules)
       (spit ebnf-grammar-file)))

(defn -main [& args]
  (generate-parser sql2011-spec-file sql2011-grammar-file))
