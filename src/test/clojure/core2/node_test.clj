(ns core2.node-test
  (:require [clojure.test :as t]
            [core2.datalog :as c2.d]
            [core2.sql :as c2.sql]
            [core2.test-util :as tu]
            [core2.util :as util]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-multi-value-insert-423
  (letfn [(expected [tt]
            {[(util/->zdt #inst "2024-01-01") (util/->zdt util/end-of-time), tt tt]
             "Happy 2024!",

             ;; weird? zero-width sys-time so won't normally show up
             [(util/->zdt #inst "2024-01-01") (util/->zdt #inst "2026-01-01"), tt tt]
             "Happy 2024!",

             [(util/->zdt #inst "2024-01-01") (util/->zdt #inst "2025-01-01"), tt (util/->zdt util/end-of-time)]
             "Happy 2024!"

             [(util/->zdt #inst "2025-01-01") (util/->zdt util/end-of-time), tt tt]
             "Happy 2025!",

             [(util/->zdt #inst "2025-01-01") (util/->zdt #inst "2026-01-01"), tt (util/->zdt util/end-of-time)]
             "Happy 2025!",

             [(util/->zdt #inst "2026-01-01") (util/->zdt util/end-of-time), tt (util/->zdt util/end-of-time)]
             "Happy 2026!"})

          (q [table tx]
            (->> (c2.sql/q tu/*node* (format "
SELECT p.id, p.text,
       p.application_time_start, p.application_time_end,
       p.system_time_start, p.system_time_end
FROM %s FOR ALL SYSTEM_TIME AS p"
                                             table)
                           {:basis {:tx tx}})
                 (into {} (map (juxt (juxt :application_time_start :application_time_end
                                           :system_time_start :system_time_end)
                                     :text)))))]

    (let [tx (c2.sql/submit-tx tu/*node* [[:sql "
INSERT INTO posts (id, text, application_time_start)
VALUES (1, 'Happy 2024!', DATE '2024-01-01'),
       (1, 'Happy 2025!', DATE '2025-01-01'),
       (1, 'Happy 2026!', DATE '2026-01-01')"]])]

      (t/is (= (expected (util/->zdt #inst "2020-01-01"))
               (q "posts" tx))))

    (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO posts2 (id, text, application_time_start) VALUES (1, 'Happy 2024!', DATE '2024-01-01')"]
                                          [:sql "INSERT INTO posts2 (id, text, application_time_start) VALUES (1, 'Happy 2025!', DATE '2025-01-01')"]
                                          [:sql "INSERT INTO posts2 (id, text, application_time_start) VALUES (1, 'Happy 2026!', DATE '2026-01-01')"]])]

      (t/is (= (expected (util/->zdt #inst "2020-01-02"))
               (q "posts2" tx))))))

(t/deftest test-dml-sees-in-tx-docs
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, v) VALUES ('foo', 0)"]
                                        [:sql "UPDATE foo SET v = 1"]])]
    (t/is (= [{:id "foo", :v 1}]
             (c2.sql/q tu/*node* "SELECT foo.id, foo.v FROM foo"
                       {:basis {:tx tx}})))))

(t/deftest test-delete-without-search-315
  (letfn [(q [tx]
            (c2.sql/q tu/*node* "SELECT foo.id, foo.application_time_start, foo.application_time_end FROM foo"
                      {:basis {:tx tx}}))]
    (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id) VALUES ('foo')"]])]
      (t/is (= [{:id "foo",
                 :application_time_start (util/->zdt #inst "2020")
                 :application_time_end (util/->zdt util/end-of-time)}]
               (q tx))))

    (let [tx (c2.sql/submit-tx tu/*node* [[:sql "DELETE FROM foo"]]
                               {:app-time-as-of-now? true})]
      (t/is (= [{:id "foo"
                 :application_time_start (util/->zdt #inst "2020")
                 :application_time_end (util/->zdt #inst "2020-01-02")}]
               (q tx))))

    (let [tx (c2.sql/submit-tx tu/*node* [[:sql "DELETE FROM foo"]])]
      (t/is (= []
               (q tx))))))

(t/deftest test-update-set-field-from-param-328
  (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO users (id, first_name, last_name) VALUES (?, ?, ?)"
                                [["susan", "Susan", "Smith"]]]])

  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "UPDATE users FOR PORTION OF APP_TIME FROM ? TO ? AS u SET first_name = ? WHERE u.id = ?"
                                         [[#inst "2021", util/end-of-time, "sue", "susan"]]]])]

    (t/is (= #{["Susan" "Smith", (util/->zdt #inst "2020") (util/->zdt #inst "2021")]
               ["sue" "Smith", (util/->zdt #inst "2021") (util/->zdt util/end-of-time)]}
             (->> (c2.sql/q tu/*node* "SELECT u.first_name, u.last_name, u.application_time_start, u.application_time_end FROM users u"
                            {:basis {:tx tx}})
                  (into #{} (map (juxt :first_name :last_name :application_time_start :application_time_end))))))))

(t/deftest test-can-submit-same-id-into-multiple-tables-338
  (let [tx1 (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO t1 (id, foo) VALUES ('thing', 't1-foo')"]
                                         [:sql "INSERT INTO t2 (id, foo) VALUES ('thing', 't2-foo')"]])
        tx2 (c2.sql/submit-tx tu/*node* [[:sql "UPDATE t2 SET foo = 't2-foo-v2' WHERE t2.id = 'thing'"]])]

    (t/is (= [{:id "thing", :foo "t1-foo"}]
             (c2.sql/q tu/*node* "SELECT t1.id, t1.foo FROM t1"
                       {:basis {:tx tx1}})))

    (t/is (= [{:id "thing", :foo "t1-foo"}]
             (c2.sql/q tu/*node* "SELECT t1.id, t1.foo FROM t1"
                       {:basis {:tx tx2}})))

    (t/is (= [{:id "thing", :foo "t2-foo"}]
             (c2.sql/q tu/*node* "SELECT t2.id, t2.foo FROM t2"
                       {:basis {:tx tx1}})))

    (t/is (= [{:id "thing", :foo "t2-foo-v2"}]
             (c2.sql/q tu/*node* "SELECT t2.id, t2.foo FROM t2"
                       {:basis {:tx tx2}})))))

(t/deftest test-put-delete-with-implicit-tables-338
  (letfn [(foos [tx]
            (->> (for [[tk tn] {:xt "xt_docs"
                                :t1 "explicit_table1"
                                :t2 "explicit_table2"}]
                   [tk (->> (c2.sql/q tu/*node* (format "SELECT t.id, t.v FROM %s t WHERE t.application_time_end = ?" tn)
                                      {:basis {:tx tx}
                                       :? [util/end-of-time]})
                            (into #{} (map :v)))])
                 (into {})))]
    (let [tx1 (c2.d/submit-tx tu/*node*
                              [[:put {:id :foo, :v "implicit table"}]
                               [:put {:id :foo, :_table "explicit_table1", :v "explicit table 1"}]
                               [:put {:id :foo, :_table "explicit_table2", :v "explicit table 2"}]])]
      (t/is (= {:xt #{"implicit table"}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
               (foos tx1))))

    (let [tx2 (c2.d/submit-tx tu/*node* [[:delete :foo]])]
      (t/is (= {:xt #{}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
               (foos tx2))))

    (let [tx3 (c2.d/submit-tx tu/*node* [[:delete "explicit_table1" :foo]])]
      (t/is (= {:xt #{}, :t1 #{}, :t2 #{"explicit table 2"}}
               (foos tx3))))))

(t/deftest test-array-element-reference-is-one-based-336
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, arr) VALUES ('foo', ARRAY[9, 8, 7, 6])"]])]
    (t/is (= [{:id "foo", :arr [9 8 7 6], :fst 9, :snd 8, :lst 6}]
             (c2.sql/q tu/*node* "SELECT foo.id, foo.arr, foo.arr[1] AS fst, foo.arr[2] AS snd, foo.arr[4] AS lst FROM foo"
                       {:basis {:tx tx}})))))

(t/deftest test-interval-literal-cce-271
  (t/is (= [{:a #c2/interval-ym "P12M"}]
           (c2.sql/q tu/*node* "select a.a from (values (1 year)) a (a)" {}))))

(t/deftest test-overrides-range
  (c2.sql/submit-tx tu/*node* [[:sql "
INSERT INTO foo (id, v, application_time_start, application_time_end)
VALUES (1, 1, DATE '1998-01-01', DATE '2000-01-01')"]])

  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "
INSERT INTO foo (id, v, application_time_start, application_time_end)
VALUES (1, 2, DATE '1997-01-01', DATE '2001-01-01')"]])]

    (t/is (= [{:id 1, :v 1,
               :application_time_start (util/->zdt #inst "1998")
               :application_time_end (util/->zdt #inst "2000")
               :system_time_start (util/->zdt #inst "2020-01-01")
               :system_time_end (util/->zdt #inst "2020-01-02")}
              {:id 1, :v 2,
               :application_time_start (util/->zdt #inst "1997")
               :application_time_end (util/->zdt #inst "2001")
               :system_time_start (util/->zdt #inst "2020-01-02")
               :system_time_end (util/->zdt util/end-of-time)}]

             (c2.sql/q tu/*node* "
SELECT foo.id, foo.v,
       foo.application_time_start, foo.application_time_end,
       foo.system_time_start, foo.system_time_end
FROM foo FOR ALL SYSTEM_TIME FOR ALL APPLICATION_TIME"
                       {:basis {:tx tx}})))))

(t/deftest test-current-timestamp-in-temporal-constraint-409
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "
INSERT INTO foo (id, v)
VALUES (1, 1)"]])]

    (t/is (= [{:id 1, :v 1,
               :application_time_start (util/->zdt #inst "2020")
               :application_time_end (util/->zdt util/end-of-time)}]
             (c2.sql/q tu/*node* "SELECT foo.id, foo.v, foo.application_time_start, foo.application_time_end FROM foo"
                       {:basis {:tx tx}})))

    (t/is (= []
             (c2.sql/q tu/*node* "
SELECT foo.id, foo.v, foo.application_time_start, foo.application_time_end
FROM foo FOR APPLICATION_TIME AS OF DATE '1999-01-01'"
                       {:basis {:tx tx, :current-time (util/->instant #inst "1999")}})))

    (t/is (= []
             (c2.sql/q tu/*node* "
SELECT foo.id, foo.v, foo.application_time_start, foo.application_time_end
FROM foo FOR APPLICATION_TIME AS OF CURRENT_TIMESTAMP"
                       {:basis {:tx tx, :current-time (util/->instant #inst "1999")}})))))

(t/deftest test-repeated-row-id-scan-bug-also-409
  (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, v) VALUES (1, 1)"]])

  (let [tx1 (c2.sql/submit-tx tu/*node* [[:sql "
UPDATE foo
FOR PORTION OF APP_TIME FROM DATE '2022-01-01' TO DATE '2024-01-01'
SET v = 2
WHERE foo.id = 1"]])

        tx2 (c2.sql/submit-tx tu/*node* [[:sql "
DELETE FROM foo
FOR PORTION OF APP_TIME FROM DATE '2023-01-01' TO DATE '2025-01-01'
WHERE foo.id = 1"]])]

    (letfn [(q1 [opts]
              (c2.sql/q tu/*node* "
SELECT foo.id, foo.v, foo.application_time_start, foo.application_time_end
FROM foo
ORDER BY foo.application_time_start"
                        opts))
            (q2 [opts]
              (frequencies
               (c2.sql/q tu/*node* "SELECT foo.id, foo.v FROM foo" opts)))]

      (t/is (= [{:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2020")
                 :application_time_end (util/->zdt #inst "2022")}
                {:id 1, :v 2
                 :application_time_start (util/->zdt #inst "2022")
                 :application_time_end (util/->zdt #inst "2024")}
                {:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2024")
                 :application_time_end (util/->zdt util/end-of-time)}]

               (q1 {:basis {:tx tx1}})))

      (t/is (= {{:id 1, :v 1} 2, {:id 1, :v 2} 1}
               (q2 {:basis {:tx tx1}})))

      (t/is (= [{:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2020")
                 :application_time_end (util/->zdt #inst "2022")}
                {:id 1, :v 2
                 :application_time_start (util/->zdt #inst "2022")
                 :application_time_end (util/->zdt #inst "2023")}
                {:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2025")
                 :application_time_end (util/->zdt util/end-of-time)}]

               (q1 {:basis {:tx tx2}})))

      (t/is (= [{:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2025")
                 :application_time_end (util/->zdt util/end-of-time)}]

               (q1 {:basis {:tx tx2, :current-time (util/->instant #inst "2026")}
                    :app-time-as-of-now? true})))

      (t/is (= {{:id 1, :v 1} 2, {:id 1, :v 2} 1}
               (q2 {:basis {:tx tx2}}))))))

(t/deftest test-error-handling-inserting-strings-into-app-time-cols-397
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, application_time_start) VALUES (1, '2018-01-01')"]])]
    ;; TODO check the rollback error when it's available, #401
    (t/is (= [] (c2.sql/q tu/*node* "SELECT foo.id FROM foo" {:basis {:tx tx}})))))

(t/deftest test-vector-type-mismatch-245
  (t/is (= [{:a [4 "2"]}]
           (c2.sql/q tu/*node* "SELECT a.a FROM (VALUES (ARRAY [4, '2'])) a (a)" {})))

  (t/is (= [{:a [["hello"] "world"]}]
           (c2.sql/q tu/*node* "SELECT a.a FROM (VALUES (ARRAY [['hello'], 'world'])) a (a)" {}))))

(t/deftest test-double-quoted-col-refs
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, \"kebab-case-col\") VALUES (1, 'kebab-case-value')"]])]
    (t/is (= [{:id 1, :kebab-case-col "kebab-case-value"}]
             (c2.sql/q tu/*node* "SELECT foo.id, foo.\"kebab-case-col\" FROM foo WHERE foo.\"kebab-case-col\" = 'kebab-case-value'"
                       {:basis {:tx tx}})))))

(t/deftest test-select-left-join-471
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, x) VALUES (1, 1), (2, 2)"]
                                        [:sql "INSERT INTO bar (id, x) VALUES (1, 1), (2, 3)"]])]
    (t/is (= [{:foo 1, :x 1}, {:foo 2, :x 2}]
             (c2.sql/q tu/*node* "SELECT foo.id foo, foo.x FROM foo LEFT JOIN bar USING (id, x)"
                       {:basis {:tx tx}})))

    #_ ; FIXME #471
    (t/is (= []
             (c2.sql/q tu/*node* "SELECT foo.id foo, foo.x FROM foo LEFT JOIN bar USING (id) WHERE foo.x = bar.x"
                       {:basis {:tx tx}})))))

(t/deftest test-c1-importer-abort-op
  (let [_tx0 (c2.d/submit-tx tu/*node* [[:put {:id :foo}]])
        tx1 (c2.d/submit-tx tu/*node* [[:put {:id :bar}]
                                       [:abort]
                                       [:put {:id :baz}]])]
    (t/is (= [{:id :foo}]
             (c2.d/q tu/*node*
                     (-> '{:find [id]
                           :where [[id :id]]}
                         (assoc :basis {:tx tx1})))))))

(t/deftest test-list-round-trip-546
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO t3(id, data) VALUES (1, [2, 3])"]
                                        [:sql "INSERT INTO t3(id, data) VALUES (2, [6, 7])"]])]
    #_ ; FIXME #546
    (t/is (= [{:data [2 3], :data_1 [2 3]}
              {:data [2 3], :data_1 [6 7]}
              {:data [6 7], :data_1 [2 3]}
              {:data [6 7], :data_1 [6 7]}]
             (c2.sql/q tu/*node* "SELECT t3.data, t2.data FROM t3, t3 AS t2"
                       {:basis {:tx tx}})))))

(t/deftest test-mutable-data-buffer-bug
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO t1(id) VALUES(1)"]])]
    (t/is (= [{:$column_1$ [{:foo 5} {:foo 5}]}]
             (c2.sql/q tu/*node* "SELECT ARRAY [OBJECT('foo': 5), OBJECT('foo': 5)] FROM t1"
                       {:basis {:tx tx}})))))

(t/deftest test-differing-length-lists-441
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO t1(id, data) VALUES (1, [2, 3])"]
                                        [:sql "INSERT INTO t1(id, data) VALUES (2, [5, 6, 7])"]])]
    (t/is (= [{:data [2 3]} {:data [5 6 7]}]
             (c2.sql/q tu/*node* "SELECT t1.data FROM t1"
                       {:basis {:tx tx}}))))

  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "INSERT INTO t2(id, data) VALUES (1, [2, 3])"]
                                        [:sql "INSERT INTO t2(id, data) VALUES (2, ['dog', 'cat'])"]])]
    (t/is (= [{:data [2 3]} {:data ["dog" "cat"]}]
             (c2.sql/q tu/*node* "SELECT t2.data FROM t2"
                       {:basis {:tx tx}})))))

(t/deftest test-cross-join-ioobe-547
  (let [tx (c2.sql/submit-tx tu/*node* [[:sql "
INSERT INTO t2(id, data)
VALUES(2, OBJECT ('foo': OBJECT('bibble': true), 'bar': OBJECT('baz': 1001)))"]

                                        [:sql "
INSERT INTO t1(id, data)
VALUES(1, OBJECT ('foo': OBJECT('bibble': true), 'bar': OBJECT('baz': 1001)))"]])]

    #_ ; FIXME
    (t/is (= [{:t2d {:bibble true}, :t1d {:baz 1001}}]
             (c2.sql/q tu/*node* "SELECT t2.data t2d, t1.data t1d FROM t2, t1"
                       {:basis {:tx tx}})))))
