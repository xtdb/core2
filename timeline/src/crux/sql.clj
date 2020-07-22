(ns crux.sql
  (:require [clojure.java.io :as io]
            [instaparse.core :as insta]))

(def parse-sql
  (insta/parser (io/resource "crux/sql.ebnf")
   :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*' | #'\\s*/[*].*([*]/\\s*|$)'")
   :string-ci true))

;; High level SQL grammar, from
;; https://calcite.apache.org/docs/reference.html

;; See also Date, SQL and Relational Theory, p. 455-458, A Simplified
;; BNF Grammar

;; SQLite grammar:
;; https://github.com/bkiers/sqlite-parser/blob/master/src/main/antlr4/nl/bigo/sqliteparser/SQLite.g4
;; https://www.sqlite.org/lang_select.html

;; SQL BNF from the spec:
;; https://ronsavage.github.io/SQL/

;; statement:
;;       setStatement
;;   |   resetStatement
;;   |   explain
;;   |   describe
;;   |   insert
;;   |   update
;;   |   merge
;;   |   delete
;;   |   query

;; statementList:
;;       statement [ ';' statement ]* [ ';' ]

;; setStatement:
;;       [ ALTER ( SYSTEM | SESSION ) ] SET identifier '=' expression

;; resetStatement:
;;       [ ALTER ( SYSTEM | SESSION ) ] RESET identifier
;;   |   [ ALTER ( SYSTEM | SESSION ) ] RESET ALL

;; explain:
;;       EXPLAIN PLAN
;;       [ WITH TYPE | WITH IMPLEMENTATION | WITHOUT IMPLEMENTATION ]
;;       [ EXCLUDING ATTRIBUTES | INCLUDING [ ALL ] ATTRIBUTES ]
;;       [ AS JSON | AS XML ]
;;       FOR ( query | insert | update | merge | delete )

;; describe:
;;       DESCRIBE DATABASE databaseName
;;    |  DESCRIBE CATALOG [ databaseName . ] catalogName
;;    |  DESCRIBE SCHEMA [ [ databaseName . ] catalogName ] . schemaName
;;    |  DESCRIBE [ TABLE ] [ [ [ databaseName . ] catalogName . ] schemaName . ] tableName [ columnName ]
;;    |  DESCRIBE [ STATEMENT ] ( query | insert | update | merge | delete )

;; insert:
;;       ( INSERT | UPSERT ) INTO tablePrimary
;;       [ '(' column [, column ]* ')' ]
;;       query

;; update:
;;       UPDATE tablePrimary
;;       SET assign [, assign ]*
;;       [ WHERE booleanExpression ]

;; assign:
;;       identifier '=' expression

;; merge:
;;       MERGE INTO tablePrimary [ [ AS ] alias ]
;;       USING tablePrimary
;;       ON booleanExpression
;;       [ WHEN MATCHED THEN UPDATE SET assign [, assign ]* ]
;;       [ WHEN NOT MATCHED THEN INSERT VALUES '(' value [ , value ]* ')' ]

;; delete:
;;       DELETE FROM tablePrimary [ [ AS ] alias ]
;;       [ WHERE booleanExpression ]

;; query:
;;       values
;;   |   WITH withItem [ , withItem ]* query
;;   |   {
;;           select
;;       |   selectWithoutFrom
;;       |   query UNION [ ALL | DISTINCT ] query
;;       |   query EXCEPT [ ALL | DISTINCT ] query
;;       |   query MINUS [ ALL | DISTINCT ] query
;;       |   query INTERSECT [ ALL | DISTINCT ] query
;;       }
;;       [ ORDER BY orderItem [, orderItem ]* ]
;;       [ LIMIT [ start, ] { count | ALL } ]
;;       [ OFFSET start { ROW | ROWS } ]
;;       [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]

;; withItem:
;;       name
;;       [ '(' column [, column ]* ')' ]
;;       AS '(' query ')'

;; orderItem:
;;       expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]

;; select:
;;       SELECT [ '/*+' hint [, hint]* '*/' ] [ STREAM ] [ ALL | DISTINCT ]
;;           { * | projectItem [, projectItem ]* }
;;       FROM tableExpression
;;       [ WHERE booleanExpression ]
;;       [ GROUP BY { groupItem [, groupItem ]* } ]
;;       [ HAVING booleanExpression ]
;;       [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]

;; selectWithoutFrom:
;;       SELECT [ ALL | DISTINCT ]
;;           { * | projectItem [, projectItem ]* }

;; projectItem:
;;       expression [ [ AS ] columnAlias ]
;;   |   tableAlias . *

;; tableExpression:
;;       tableReference [, tableReference ]*
;;   |   tableExpression [ NATURAL ] [ ( LEFT | RIGHT | FULL ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]
;;   |   tableExpression CROSS JOIN tableExpression
;;   |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

;; joinCondition:
;;       ON booleanExpression
;;   |   USING '(' column [, column ]* ')'

;; tableReference:
;;       tablePrimary
;;       [ FOR SYSTEM_TIME AS OF expression ]
;;       [ matchRecognize ]
;;       [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

;; tablePrimary:
;;       [ [ catalogName . ] schemaName . ] tableName
;;       '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
;;   |   tablePrimary [ '/*+' hint [, hint]* '*/' ] [ EXTEND ] '(' columnDecl [, columnDecl ]* ')'
;;   |   [ LATERAL ] '(' query ')'
;;   |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
;;   |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]* ')' ')'

;; columnDecl:
;;       column type [ NOT NULL ]

;; hint:
;;       hintName
;;   |   hintName '(' hintOptions ')'

;; hintOptions:
;;       hintKVOption [, hintKVOption]*
;;   |   optionName [, optionName]*
;;   |   optionValue [, optionValue]*

;; hintKVOption:
;;       optionName '=' stringLiteral
;;   |   stringLiteral '=' stringLiteral

;; optionValue:
;;       stringLiteral
;;   |   numericLiteral

;; values:
;;       VALUES expression [, expression ]*

;; groupItem:
;;       expression
;;   |   '(' ')'
;;   |   '(' expression [, expression ]* ')'
;;   |   CUBE '(' expression [, expression ]* ')'
;;   |   ROLLUP '(' expression [, expression ]* ')'
;;   |   GROUPING SETS '(' groupItem [, groupItem ]* ')'

;; window:
;;       windowName
;;   |   windowSpec

;; windowSpec:
;;       '('
;;       [ windowName ]
;;       [ ORDER BY orderItem [, orderItem ]* ]
;;       [ PARTITION BY expression [, expression ]* ]
;;       [
;;           RANGE numericOrIntervalExpression { PRECEDING | FOLLOWING }
;;       |   ROWS numericExpression { PRECEDING | FOLLOWING }
;;       ]
;;       ')'

;; The operator precedence and associativity, highest to lowest.

;; OPERATOR	ASSOCIATIVITY
;; .	left
;; ::	left
;; [ ] (array element)	left
;; + - (unary plus, minus)	right
;; * / % ||	left
;; + -	left
;; BETWEEN, IN, LIKE, SIMILAR, OVERLAPS, CONTAINS etc.	-
;; < > = <= >= <> !=	left
;; IS NULL, IS FALSE, IS NOT TRUE etc.	-
;; NOT	right
;; AND	left
;; OR	left
;; Note that :: is dialect-specific, but is shown in this table for completeness.

;; Comparison operators
;; OPERATOR SYNTAX	DESCRIPTION
;; value1 = value2	Equals
;; value1 <> value2	Not equal
;; value1 != value2	Not equal (only available at some conformance levels)
;; value1 > value2	Greater than
;; value1 >= value2	Greater than or equal
;; value1 < value2	Less than
;; value1 <= value2	Less than or equal
;; value IS NULL	Whether value is null
;; value IS NOT NULL	Whether value is not null
;; value1 IS DISTINCT FROM value2	Whether two values are not equal, treating null values as the same
;; value1 IS NOT DISTINCT FROM value2	Whether two values are equal, treating null values as the same
;; value1 BETWEEN value2 AND value3	Whether value1 is greater than or equal to value2 and less than or equal to value3
;; value1 NOT BETWEEN value2 AND value3	Whether value1 is less than value2 or greater than value3
;; string1 LIKE string2 [ ESCAPE string3 ]	Whether string1 matches pattern string2
;; string1 NOT LIKE string2 [ ESCAPE string3 ]	Whether string1 does not match pattern string2
;; string1 SIMILAR TO string2 [ ESCAPE string3 ]	Whether string1 matches regular expression string2
;; string1 NOT SIMILAR TO string2 [ ESCAPE string3 ]	Whether string1 does not match regular expression string2
;; value IN (value [, value]*)	Whether value is equal to a value in a list
;; value NOT IN (value [, value]*)	Whether value is not equal to every value in a list
;; value IN (sub-query)	Whether value is equal to a row returned by sub-query
;; value NOT IN (sub-query)	Whether value is not equal to every row returned by sub-query
;; value comparison SOME (sub-query)	Whether value comparison at least one row returned by sub-query
;; value comparison ANY (sub-query)	Synonym for SOME
;; value comparison ALL (sub-query)	Whether value comparison every row returned by sub-query
;; EXISTS (sub-query)	Whether sub-query returns at least one row

;; comp:
;;       =
;;   |   <>
;;   |   >
;;   |   >=
;;   |   <
;;   |   <=

;; Logical operators
;; OPERATOR SYNTAX	DESCRIPTION
;; boolean1 OR boolean2	Whether boolean1 is TRUE or boolean2 is TRUE
;; boolean1 AND boolean2	Whether boolean1 and boolean2 are both TRUE
;; NOT boolean	Whether boolean is not TRUE; returns UNKNOWN if boolean is UNKNOWN
;; boolean IS FALSE	Whether boolean is FALSE; returns FALSE if boolean is UNKNOWN
;; boolean IS NOT FALSE	Whether boolean is not FALSE; returns TRUE if boolean is UNKNOWN
;; boolean IS TRUE	Whether boolean is TRUE; returns FALSE if boolean is UNKNOWN
;; boolean IS NOT TRUE	Whether boolean is not TRUE; returns TRUE if boolean is UNKNOWN
;; boolean IS UNKNOWN	Whether boolean is UNKNOWN
;; boolean IS NOT UNKNOWN	Whether boolean is not UNKNOWN
