(ns core2.sql.annotate-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(t/deftest test-annotate-query-scopes
  (let [tree (sql/parse "WITH RECURSIVE foo AS (SELECT 1 FROM foo AS bar)
SELECT t1.d-t1.e AS a, SUM(t1.a) AS b
  FROM t1, foo AS baz
 WHERE EXISTS (SELECT 1 FROM t1 AS x WHERE x.b < t1.c AND x.b > (SELECT t1.b FROM (SELECT 1 AS b FROM boz) AS t2 WHERE t1.b = t2.b))
   AND t1.a > t1.b
 GROUP BY t1.d, t1.e
 ORDER BY b, t1.c")]
    (t/is (= {:scopes
              [{:id 1,
                :dependent-columns #{},
                :projected-columns
                [{:identifier "a", :index 0} {:identifier "b", :index 1}],
                :type :query-expression,
                :ctes
                {"foo"
                 {:query-name "foo", :id 2, :scope-id 1, :subquery-scope-id 3}},
                :order-by-indexes [1 nil]}
               {:id 3,
                :dependent-columns #{},
                :projected-columns [{:index 0}],
                :parent-id 1,
                :type :query-expression,
                :ctes {}}
               {:id 4,
                :dependent-columns #{},
                :projected-columns [{:index 0}],
                :parent-id 3,
                :tables
                {"bar"
                 {:correlation-name "bar",
                  :id 5,
                  :scope-id 4,
                  :table-or-query-name "foo",
                  :cte-id 2,
                  :cte-scope-id 1,
                  :used-columns #{}}},
                :columns #{},
                :type :query-specification}
               {:id 6,
                :dependent-columns #{},
                :projected-columns
                [{:identifier "a", :index 0} {:identifier "b", :index 1}],
                :parent-id 1,
                :tables
                {"t1"
                 {:correlation-name "t1",
                  :id 8,
                  :scope-id 6,
                  :table-or-query-name "t1",
                  :used-columns
                  #{["t1" "e"] ["t1" "a"] ["t1" "b"] ["t1" "c"] ["t1" "d"]}},
                 "baz"
                 {:correlation-name "baz",
                  :id 9,
                  :scope-id 6,
                  :table-or-query-name "foo",
                  :cte-id 2,
                  :cte-scope-id 1,
                  :used-columns #{}}},
                :columns
                #{{:identifiers ["t1" "d"],
                   :type :ordinary,
                   :scope-id 6,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t1" "a"],
                   :type :within-group-varying,
                   :scope-id 6,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t1" "e"],
                   :type :group-invariant,
                   :scope-id 6,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t1" "b"],
                   :type :ordinary,
                   :scope-id 6,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t1" "a"],
                   :type :ordinary,
                   :scope-id 6,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t1" "e"],
                   :type :ordinary,
                   :scope-id 6,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t1" "d"],
                   :type :group-invariant,
                   :scope-id 6,
                   :table-id 8,
                   :table-scope-id 6}},
                :type :query-specification,
                :grouping-columns [["t1" "d"] ["t1" "e"]]}
               {:id 10,
                :dependent-columns
                #{{:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 14,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t1" "c"],
                   :type :outer,
                   :scope-id 11,
                   :table-id 8,
                   :table-scope-id 6}},
                :projected-columns [{:index 0}],
                :parent-id 6,
                :type :query-expression,
                :ctes {}}
               {:id 11,
                :dependent-columns
                #{{:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 14,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t1" "c"],
                   :type :outer,
                   :scope-id 11,
                   :table-id 8,
                   :table-scope-id 6}},
                :projected-columns [{:index 0}],
                :parent-id 10,
                :tables
                {"x"
                 {:correlation-name "x",
                  :id 12,
                  :scope-id 11,
                  :table-or-query-name "t1",
                  :used-columns #{["x" "b"]}}},
                :columns
                #{{:identifiers ["x" "b"],
                   :type :ordinary,
                   :scope-id 11,
                   :table-id 12,
                   :table-scope-id 11}
                  {:identifiers ["t1" "c"],
                   :type :outer,
                   :scope-id 11,
                   :table-id 8,
                   :table-scope-id 6}},
                :type :query-specification}
               {:id 13,
                :dependent-columns
                #{{:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 14,
                   :table-id 8,
                   :table-scope-id 6}},
                :projected-columns
                [{:identifier "b", :qualified-column ["t1" "b"], :index 0}],
                :parent-id 11,
                :type :query-expression,
                :ctes {}}
               {:id 14,
                :dependent-columns
                #{{:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 14,
                   :table-id 8,
                   :table-scope-id 6}},
                :projected-columns
                [{:identifier "b", :qualified-column ["t1" "b"], :index 0}],
                :parent-id 13,
                :tables
                {"t2"
                 {:correlation-name "t2",
                  :id 15,
                  :scope-id 14,
                  :subquery-scope-id 16,
                  :used-columns #{["t2" "b"]}}},
                :columns
                #{{:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 14,
                   :table-id 8,
                   :table-scope-id 6}
                  {:identifiers ["t2" "b"],
                   :type :ordinary,
                   :scope-id 14,
                   :table-id 15,
                   :table-scope-id 14}},
                :type :query-specification}
               {:id 16,
                :dependent-columns #{},
                :projected-columns [{:identifier "b", :index 0}],
                :parent-id 14,
                :type :query-expression,
                :ctes {}}
               {:id 17,
                :dependent-columns #{},
                :projected-columns [{:identifier "b", :index 0}],
                :parent-id 16,
                :tables
                {"boz"
                 {:correlation-name "boz",
                  :id 18,
                  :scope-id 17,
                  :table-or-query-name "boz",
                  :used-columns #{}}},
                :columns #{},
                :type :query-specification}],
              :errs []}
             (sql/analyze-query tree)))))

(defmacro ^:private invalid? [re q]
  `(let [[err# :as errs#] (:errs (sql/analyze-query (sql/parse ~q)))
         err# (or err# "")]
     (t/is (= 1 (count errs#)) (pr-str errs#))
     (t/is (re-find ~re err#))))

(defmacro ^:private valid? [q]
  `(let [{errs# :errs scopes# :scopes} (sql/analyze-query (sql/parse ~q))]
     (t/is (empty? errs#))
     scopes#))

(t/deftest test-parsing-errors-are-reported
  (invalid? #"Parse error at line 1, column 1:\nSELEC\n"
            "SELEC"))

(t/deftest test-scope-rules
  (invalid? #"XTDB requires fully-qualified columns: a at line 1, column 8"
            "SELECT a FROM foo")
  (invalid? #"Table not in scope: bar at line 1, column 8"
            "SELECT bar.a FROM foo")

  (invalid? #"Table not in scope: bar"
            "SELECT bar.a FROM bar AS foo")

  (valid? "SELECT bar.a FROM bar")
  (valid? "SELECT bar.a FROM foo AS bar")

  (valid? "SELECT bar.a FROM foo AS bar ORDER BY bar.y")
  (valid? "SELECT bar.a AS a FROM foo AS bar ORDER BY a")
  (valid? "SELECT bar.x, bar.a + 1 FROM foo AS bar ORDER BY bar.a + 1")

  (invalid? #"Table not in scope: baz"
            "SELECT bar.a FROM foo AS bar ORDER BY baz.y")

  (valid? "SELECT t1.b FROM t1 WHERE EXISTS (SELECT x.b FROM t1 AS x WHERE x.b < t1.b)")
  (valid? "SELECT t1.a FROM t1 WHERE EXISTS (SELECT t1.a, COUNT(x.a) FROM t1 AS x WHERE x.b < t1.b GROUP BY x.a HAVING x.a = 1) GROUP BY t1.a")

  (valid? "SELECT t1.b FROM t1, LATERAL (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2")

  (invalid? #"Table not in scope: t1"
            "SELECT * FROM t1, (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2")
  (invalid? #"Table not in scope: t2"
            "SELECT * FROM t1, LATERAL (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, (SELECT x.b FROM t1 AS x WHERE x.b < t2.b) AS t3")

  (valid? "SELECT t1.b FROM t1 JOIN t2 USING (x)")
  (valid? "SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y)")
  (valid? "SELECT * FROM foo, LATERAL (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2")
  (valid? "SELECT * FROM foo WHERE foo.x = (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x))")
  (valid? "SELECT t1.b FROM foo, t1 JOIN t2 ON (t1.x = foo.y)")
  (valid? "SELECT t1.b FROM bar, (t1 INNER JOIN t2 USING (x)) JOIN t3 ON (t1.x = t3.y)")

  (invalid? #"Table not in scope: foo"
            "SELECT * FROM (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2")
  (invalid? #"Table not in scope: t1"
            "SELECT t2.b FROM (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, t1")
  (invalid? #"Table not in scope: bar"
            "SELECT bar.a FROM foo WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)")
  (invalid? #"Table not in scope: foo"
            "SELECT 1 FROM foo AS baz WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)")

  (valid? "SELECT foo.a FROM bar AS foo")
  (valid? "SELECT foo.a FROM bar AS foo (a)")
  (valid? "SELECT 1 FROM bar AS foo ORDER BY (foo.a)")
  (valid? "SELECT foo.a FROM bar AS foo ORDER BY a")
  (valid? "SELECT foo.a FROM (SELECT x.b FROM x) AS foo (a)")
  (valid? "SELECT foo.b FROM (SELECT x.b FROM x UNION SELECT y.a FROM y) AS foo")
  (valid? "SELECT foo.a FROM (SELECT x.b FROM x UNION SELECT y.a FROM y) AS foo (a)")
  (invalid? #"Column not in scope: foo.a"
            "SELECT foo.a FROM (SELECT x.b FROM x UNION SELECT y.a FROM y) AS foo")
  (invalid? #"Column not in scope: foo.a"
            "SELECT foo.a FROM bar AS foo (b)")
  (invalid? #"Column not in scope: foo.a"
            "SELECT foo.a FROM (SELECT x.b FROM x) AS foo"))

(t/deftest test-variable-duplication
  (invalid? #"Table variable duplicated: baz"
            "SELECT 1 FROM foo AS baz, baz")
  (invalid? #"CTE query name duplicated: foo"
            "WITH foo AS (SELECT 1 FROM foo), foo AS (SELECT 1 FROM foo) SELECT 1 FROM foo")
  (invalid? #"Column name duplicated: bar"
            "WITH foo (bar, bar) AS (SELECT 1, 2 FROM foo) SELECT * FROM foo")
  (invalid? #"Column name duplicated: foo"
            "SELECT 1 FROM (SELECT 1, 2 FROM foo) AS bar (foo, foo)")
  (invalid? #"Column name ambiguous: x.a"
            "SELECT x.a FROM (SELECT * FROM t1 AS t1(a), t2 AS t2(a)) AS x"))

(t/deftest test-grouping-columns
  (invalid? #"Column reference is not a grouping column: t1.a"
            "SELECT t1.a FROM t1 GROUP BY t1.b")
  (invalid? #"Column reference is not a grouping column: t1.a"
            "SELECT t1.b FROM t1 GROUP BY t1.b HAVING t1.a")
  (valid? "SELECT t1.b, COUNT(t1.a) FROM t1 GROUP BY t1.b")
  (invalid? #"Column reference is not a grouping column: t1.a"
            "SELECT t1.a, COUNT(t1.b) FROM t1")

  (invalid? #"Outer column reference is not an outer grouping column: t1.b"
            "SELECT t1.b FROM t1 WHERE EXISTS (SELECT t1.b, COUNT(*) FROM t2)")
  (invalid? #"Within group varying column reference is an outer column: t1.b"
            "SELECT t1.b FROM t1 WHERE 1 = (SELECT COUNT(t1.b) FROM t2)")
  (valid? "SELECT t1.b FROM t1 WHERE 1 = (SELECT COUNT(t2.a) FROM t2) GROUP BY t1.b"))

(t/deftest test-clauses-not-allowed-to-contain-aggregates-or-queries
  (invalid? #"Aggregate functions cannot contain aggregate functions"
            "SELECT COUNT(SUM(t1.b)) FROM t1")
  (invalid? #"Aggregate functions cannot contain nested queries"
            "SELECT COUNT((SELECT 1 FROM foo)) FROM t1")

  (invalid? #"Sort specifications cannot contain aggregate functions"
            "SELECT 1 FROM t1 ORDER BY COUNT(t1.a)")
  (invalid? #"Sort specifications cannot contain nested queries"
            "SELECT 1 FROM t1 ORDER BY (SELECT 1 FROM foo)")

  (invalid? #"WHERE clause cannot contain aggregate functions"
            "SELECT 1 FROM t1 WHERE COUNT(t1.a)")
  (valid? "SELECT 1 FROM t1 WHERE 1 = (SELECT COUNT(t1.a) FROM t1)"))

(t/deftest test-fetch-and-offset-type
  (invalid? #"Fetch first row count must be an integer"
            "SELECT 1 FROM t1 FETCH FIRST 'foo' ROWS ONLY")
  (valid? "SELECT 1 FROM t1 FETCH FIRST 1 ROWS ONLY")
  (valid? "SELECT 1 FROM t1 FETCH FIRST :foo ROWS ONLY")

  (invalid? #"Offset row count must be an integer"
            "SELECT 1 FROM t1 OFFSET 'foo' ROWS")
  (valid? "SELECT 1 FROM t1 OFFSET 1 ROWS")
  (valid? "SELECT 1 FROM t1 OFFSET :foo ROWS"))

(t/deftest test-projection
  (t/is (= [[{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t2" "b"]}]]
           (->> (valid? "SELECT t1.b FROM t1 UNION SELECT t2.b FROM t2")
                (map :projected-columns))))

  (t/is (= [[{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "a", :qualified-column ["t2" "a"]}]]
           (->> (valid? "SELECT t1.b FROM t1 EXCEPT SELECT t2.a FROM t2")
                (map :projected-columns))))

  (t/is (= [[{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t2" "b"]}]
            [{:index 0, :identifier "c", :qualified-column ["t3" "c"]}]]
           (->> (valid? "SELECT t1.b FROM t1 INTERSECT SELECT t2.b FROM t2 UNION SELECT t3.c FROM t3")
                (map :projected-columns))))

  (invalid? #"EXCEPT requires tables to have same degree"
            "SELECT t1.b FROM t1 EXCEPT SELECT t2.a, t2.b FROM t2")
  (invalid? #"UNION requires tables to have same degree"
            "SELECT t1.b FROM t1 UNION SELECT t2.b, t2.c FROM t2")
  (invalid? #"EXCEPT requires tables to have same degree"
            "SELECT t1.b FROM t1 UNION SELECT t2.b FROM t2 EXCEPT SELECT t3.c, t3.d FROM t3")

  (t/is (= [[{:index 0} {:index 1} {:index 2}]]
           (->> (valid? "VALUES (1, 2, 3), (4, 5, 6)")
                (map :projected-columns))))

  (t/is (= [[{:index 0}]]
           (->> (valid? "VALUES 1, 2")
                (map :projected-columns))))

  (t/is (= [[{:index 0}]]
           (->> (valid? "VALUES 1, (2)")
                (map :projected-columns))))

  (invalid? #"VALUES requires rows to have same degree"
            "VALUES (1, 2), (3, 4, 5)")

  (valid? "VALUES (1, 2), (SELECT t1.a, t1.b FROM t1)")
  (valid? "VALUES 1, (SELECT t1.a FROM t1)")
  (invalid? #"VALUES requires rows to have same degree"
            "VALUES (1, 2), (SELECT t1.a FROM t1)")
  (invalid? #"VALUES requires rows to have same degree"
            "VALUES (1), (SELECT t1.a, t1.b FROM t1)")

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["t1" "a"]} {:index 1 :identifier "b" :qualified-column ["t1" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["t1" "a"]} {:index 1 :identifier "b" :qualified-column ["t1" "b"]}]]
           (->> (valid? "SELECT x.a FROM (SELECT t1.a, t1.b FROM t1) AS x (a, b)")
                (map :projected-columns))))
  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0}]]
           (->> (valid? "SELECT x.a FROM (VALUES (1)) AS x (a)")
                (map :projected-columns))))
  (t/is (= [[{:index 0 :identifier "a"}]
            [{:index 0 :identifier "a"}]]
           (->> (valid? "SELECT :a FROM x")
                (map :projected-columns))))

  (invalid? #"Derived columns has to have same degree as table"
            "SELECT x.a FROM (SELECT t1.a, t1.b FROM t1) AS x (a)")
  (invalid? #"Derived columns has to have same degree as table"
            "SELECT x.a FROM LATERAL (SELECT t1.a FROM t1) AS x (a, b)")
  (invalid? #"Derived columns has to have same degree as table"
            "SELECT x.a FROM (VALUES (1, 2)) AS x (a)")
  (valid? "SELECT x.a FROM y AS x (a, b)")
  (valid? "SELECT x.a FROM y, UNNEST(y.a) AS x (a)")

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["x" "b"]}]]
           (->> (valid? "SELECT * FROM x WHERE x.a = x.b")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]} {:index 1 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]} {:index 1 :identifier "b" :qualified-column ["y" "b"]}]]
           (->> (valid? "SELECT * FROM (SELECT y.a, y.b FROM y) AS x")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "c" :qualified-column ["x" "c"]} {:index 1 :identifier "d" :qualified-column ["x" "d"]}]
            [{:index 0 :identifier "c" :qualified-column ["x" "c"]} {:index 1 :identifier "d" :qualified-column ["x" "d"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]} {:index 1 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]} {:index 1 :identifier "b" :qualified-column ["y" "b"]}]]
           (->> (valid? "SELECT * FROM (SELECT y.a, y.b FROM y) AS x (c, d)")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["z" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["z" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]]
           (->> (valid? "SELECT * FROM (SELECT y.a FROM y WHERE y.z = FALSE) AS x, z WHERE z.b = TRUE")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "b" :qualified-column ["x" "b"]} {:index 1 :identifier "b" :qualified-column ["z" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["x" "b"]} {:index 1 :identifier "b" :qualified-column ["z" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]]
           (->> (valid? "SELECT * FROM (SELECT y.b FROM y) AS x, LATERAL (SELECT y.b FROM y) AS z")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]]
           (->> (valid? "SELECT x.* FROM (SELECT y.b FROM y) AS x, (SELECT y.a FROM y) AS z")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "b" :qualified-column ["x" "b"]}
             {:index 1 :identifier "a" :qualified-column ["z" "a"]}]
            [{:index 0 :identifier "b" :qualified-column ["x" "b"]}
             {:index 1 :identifier "a" :qualified-column ["z" "a"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]]
           (->> (valid? "SELECT x.*, z.* FROM (SELECT y.b FROM y) AS x, (SELECT y.a FROM y) AS z")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]]
           (->> (valid? "SELECT * FROM x WHERE x.a = x.b GROUP BY x.a")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "c" :qualified-column ["x" "c"]}
             {:index 2}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "c" :qualified-column ["x" "c"]}
             {:index 2}]]
           (->> (valid? "SELECT x.*, COUNT(x.b) FROM x WHERE x.a = x.b GROUP BY x.a, x.c")
                (map :projected-columns))))

  (invalid? #"Query does not select any columns"
            "SELECT * FROM foo")
  (invalid? #"Table not in scope: baz"
            "SELECT foo.x, baz.* FROM foo")

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["x" "a"]}]]
           (->> (valid? "SELECT x.a, x.* FROM x WHERE x.a IS NOT NULL")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["x" "a"]}]]
           (->> (valid? "SELECT x.a, x.a FROM x WHERE x.a IS NOT NULL")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["y" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["y" "a"]}]]
           (->> (valid? "SELECT * FROM x, y WHERE x.a = y.a")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["foo" "a"]}
             {:index 1 :identifier "b" :qualified-column ["foo" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["foo" "a"]}
             {:index 1 :identifier "b" :qualified-column ["foo" "b"]}]]
           (->> (valid? "WITH foo AS (SELECT * FROM x WHERE x.a = x.b) SELECT * FROM foo")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "c" :qualified-column ["foo" "c"]}
             {:index 1 :identifier "d" :qualified-column ["foo" "d"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "c" :qualified-column ["foo" "c"]}
             {:index 1 :identifier "d" :qualified-column ["foo" "d"]}]]
           (->> (valid? "WITH foo (c, d) AS (SELECT * FROM x WHERE x.a = x.b) SELECT * FROM foo")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "b" :qualified-column ["foo" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "b" :qualified-column ["foo" "b"]}]]
           (->> (valid? "WITH foo (c) AS (SELECT * FROM x AS x (a)) SELECT * FROM foo AS foo (b)")
                (map :projected-columns))))

  (t/is (= [[{:identifier "a", :qualified-column ["x" "a"], :index 0}
             {:identifier "a", :qualified-column ["foo" "a"], :index 1}]
            [{:identifier "a", :qualified-column ["x" "a"], :index 0}
             {:identifier "a", :qualified-column ["foo" "a"], :index 1}]]
           (->> (valid? "SELECT * FROM x, UNNEST(x.a) AS foo (a)")
                (map :projected-columns))))

  (t/is (= [[{:identifier "a", :qualified-column ["foo" "a"], :index 0}
             {:identifier "b", :qualified-column ["foo" "b"], :index 1}]
            [{:identifier "a", :qualified-column ["foo" "a"], :index 0}
             {:identifier "b", :qualified-column ["foo" "b"], :index 1}]]
           (->> (valid? "SELECT foo.* FROM x, UNNEST(x.a) WITH ORDINALITY AS foo (a, b)")
                (map :projected-columns))))

  (invalid? #"Derived columns has to have same degree as table"
            "SELECT * FROM x, UNNEST(x.a) WITH ORDINALITY AS foo (a)")
  (invalid? #"Derived columns has to have same degree as table"
            "SELECT * FROM x, UNNEST(x.a) AS foo (a, b)")

  (invalid? #"Subquery does not select single column"
            "SELECT t1.b FROM t1 WHERE (1, 1) = (SELECT t1.b, t1.c FROM t2)")
  (valid? "SELECT t1.b FROM t1 WHERE (1, 1) IN (SELECT t1.b, t1.c FROM t2)")

  (invalid? #"Left side does not contain all join columns"
            "SELECT y.x FROM (SELECT x.y FROM x) AS x INNER JOIN y USING(x)")
  (invalid? #"Right side does not contain all join columns"
            "SELECT x.x FROM x INNER JOIN (SELECT y.y FROM y) AS y USING(x)")
  (valid? "SELECT x.x, y.x FROM x INNER JOIN y USING(x)")
  (invalid? #"Left side contains ambiguous join columns"
            "SELECT y.x FROM (SELECT t1.x, t1.x FROM t1) AS x INNER JOIN y USING(x)")
  (invalid? #"Right side contains ambiguous join columns"
            "SELECT x.x FROM x INNER JOIN (SELECT t1.x, t1.x FROM t1) AS y USING(x)")
  (valid? "SELECT x.x, y.x, x.y, y.y FROM (SELECT t1.x, t1.y FROM t1) AS x INNER JOIN (SELECT t1.x, t1.y FROM t1) AS y USING(x, y)"))
