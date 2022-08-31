(ns core2.api-test
  (:require [clojure.test :as t :refer [deftest]]
            [core2.api :as c2]
            [core2.client :as client]
            [core2.test-util :as tu :refer [*node*]]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [core2.local-node :as node])
  (:import (java.time Duration ZoneId)
           java.util.concurrent.ExecutionException))

(defn- with-mock-clock [f]
  (tu/with-opts {:core2.log/memory-log {:instant-src (tu/->mock-clock)}}
    f))

(defn- with-client [f]
  (let [port (tu/free-port)
        sys (-> {:core2/server {:node *node*, :port port}}
                ig/prep
                (doto ig/load-namespaces)
                ig/init)]
    (try
      (binding [*node* (client/start-client (str "http://localhost:" port))]
        (f))
      (finally
        (ig/halt! sys)))))

(def api-implementations
  (-> {:in-memory (t/join-fixtures [with-mock-clock tu/with-node])
       :remote (t/join-fixtures [with-mock-clock tu/with-node with-client])}

      #_(select-keys [:in-memory])
      #_(select-keys [:remote])))

(def ^:dynamic *node-type*)

(defn with-each-api-implementation [f]
  (doseq [[node-type run-tests] api-implementations]
    (binding [*node-type* node-type]
      (t/testing (str node-type)
        (run-tests f)))))

(t/use-fixtures :each with-each-api-implementation)

(t/deftest test-status
  (t/is (map? (c2/status *node*))))

(t/deftest test-simple-query
  (let [!tx (c2/submit-tx *node* [[:put {:id :foo, :inst #inst "2021"}]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= [{:e :foo, :inst (util/->zdt #inst "2021")}]
             (->> (c2/plan-datalog *node*
                                   (-> '{:find [?e ?inst]
                                         :where [[?e :inst ?inst]]}
                                       (assoc :basis {:tx !tx}
                                              :basis-timeout (Duration/ofSeconds 1))))
                  (into []))))))

(t/deftest test-validation-errors
  (t/is (thrown? IllegalArgumentException
                 (try
                   @(c2/submit-tx *node* [[:pot {:id :foo}]])
                   (catch ExecutionException e
                     (throw (.getCause e))))))

  (t/is (thrown? IllegalArgumentException
                 (try
                   @(c2/submit-tx *node* [[:put {}]])
                   (catch ExecutionException e
                     (throw (.getCause e)))))))

(t/deftest round-trips-lists
  (let [!tx (c2/submit-tx *node* [[:put {:id :foo, :list [1 2 ["foo" "bar"]] :_table "bar"}]
                                  [:sql "INSERT INTO bar (id, list) VALUES ('bar', ARRAY[?, 2, 3 + 5])"
                                   [[4]]]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= [{:id :foo, :list [1 2 ["foo" "bar"]]}
              {:id "bar", :list [4 2 8]}]
             (c2/datalog-query *node*
                               (-> '{:find [?id ?list]
                                     :where [[?id :list ?list]]}
                                   (assoc :basis {:tx !tx}
                                          :basis-timeout (Duration/ofSeconds 1))))))

    (t/is (= [{:id :foo, :list [1 2 ["foo" "bar"]]}
              {:id "bar", :list [4 2 8]}]
             (c2/sql-query *node*
                           "SELECT b.id, b.list FROM bar b"
                           {:basis {:tx !tx}
                            :basis-timeout (Duration/ofSeconds 1)})))))

(t/deftest round-trips-structs
  (let [!tx (c2/submit-tx *node* [[:put {:id :foo, :struct {:a 1, :b {:c "bar"}}}]
                                  [:put {:id :bar, :struct {:a true, :d 42.0}}]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= #{{:id :foo, :struct {:a 1, :b {:c "bar"}}}
               {:id :bar, :struct {:a true, :d 42.0}}}
             (set (c2/datalog-query *node*
                                    (-> '{:find [?id ?struct]
                                          :where [[?id :struct ?struct]]}
                                        (assoc :basis {:tx !tx}
                                               :basis-timeout (Duration/ofSeconds 1)))))))))

(t/deftest round-trips-temporal
  (let [vs {:dt #time/date "2022-08-01"
            :ts #time/date-time "2022-08-01T14:34"
            :tstz #time/zoned-date-time "2022-08-01T14:34+01:00"
            :tm #time/time "13:21:14.932254"
            ;; :tmtz #time/offset-time "11:21:14.932254-08:00" ; TODO #323
            }
        !tx (c2/submit-tx *node* [[:sql "INSERT INTO foo (id, dt, ts, tstz, tm) VALUES ('foo', ?, ?, ?, ?)"
                                   [(mapv vs [:dt :ts :tstz :tm])]]])]

    (t/is (= [(assoc vs :id "foo")]
             (c2/sql-query *node* "SELECT f.id, f.dt, f.ts, f.tstz, f.tm FROM foo f"
                           {:basis {:tx !tx}, :basis-timeout (Duration/ofMillis 100)
                            :default-tz (ZoneId/of "Europe/London")})))

    (let [lits [[:dt "DATE '2022-08-01'"]
                [:ts "TIMESTAMP '2022-08-01 14:34:00'"]
                [:tstz "TIMESTAMP '2022-08-01 14:34:00+01:00'"]
                [:tm "TIME '13:21:14.932254'"]

                #_ ; FIXME #323
                [:tmtz "TIME '11:21:14.932254-08:00'"]]
          !tx (c2/submit-tx *node* (vec (for [[t lit] lits]
                                          [:sql (format "INSERT INTO bar (id, v) VALUES (?, %s)" lit)
                                           [[(name t)]]])))]
      (t/is (= (set (for [[t _lit] lits]
                      {:id (name t), :v (get vs t)}))
               (set (c2/sql-query *node* "SELECT b.id, b.v FROM bar b"
                                  {:basis {:tx !tx}, :basis-timeout (Duration/ofMillis 100)
                                   :default-tz (ZoneId/of "Europe/London")})))))))

(t/deftest can-manually-specify-sys-time-47
  (let [tx1 @(c2/submit-tx *node* [[:put {:id :foo}]]
                           {:sys-time #inst "2012"})

        _invalid-tx @(c2/submit-tx *node* [[:put {:id :bar}]]
                                   {:sys-time #inst "2011"})

        tx3 @(c2/submit-tx *node* [[:put {:id :baz}]])]

    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2012")})
             tx1))

    (letfn [(q-at [tx]
              (->> (c2/datalog-query *node*
                                     (-> '{:find [?id]
                                           :where [[?e :id ?id]]}
                                         (assoc :basis {:tx tx}
                                                :basis-timeout (Duration/ofSeconds 1))))
                   (into #{} (map :id))))]

      (t/is (= #{:foo} (q-at tx1)))
      (t/is (= #{:foo :baz} (q-at tx3))))))

(def ^:private devs
  [[:put {:id :jms, :name "James" :_table "users"}]
   [:put {:id :hak, :name "Håkan" :_table "users"}]
   [:put {:id :mat, :name "Matt" :_table "users"}]
   [:put {:id :wot, :name "Dan" :_table "users"}]])

(t/deftest test-sql-roundtrip
  (let [!tx (c2/submit-tx *node* devs)]

    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= [{:name "James"}]
             (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.name = 'James'"
                           {:basis {:tx !tx}})))))

(t/deftest test-sql-dynamic-params-103
  (let [!tx (c2/submit-tx *node* devs)]

    (t/is (= [{:name "James"} {:name "Matt"}]
             (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.name IN (?, ?)"
                           {:basis {:tx !tx}
                            :? ["James", "Matt"]})))))

(t/deftest start-and-query-empty-node-re-231-test
  (with-open [n (node/start-node {})]
    (t/is (= [] (c2/sql-query n "select a.a from a a" {})))))

(t/deftest test-basic-sql-dml
  (letfn [(all-users [!tx]
            (->> (c2/sql-query *node* "SELECT u.first_name, u.last_name, u.application_time_start, u.application_time_end FROM users u"
                               {:basis {:tx !tx}})
                 (into #{} (map (juxt :first_name :last_name :application_time_start :application_time_end)))))]

    (let [!tx1 (c2/submit-tx *node* [[:sql "INSERT INTO users (id, first_name, last_name, application_time_start) VALUES (?, ?, ?, ?)"
                                      [["dave", "Dave", "Davis", #inst "2018"]
                                       ["claire", "Claire", "Cooper", #inst "2019"]
                                       ["alan", "Alan", "Andrews", #inst "2020"]
                                       ["susan", "Susan", "Smith", #inst "2021"]]]])
          tx1-expected #{["Dave" "Davis", (util/->zdt #inst "2018"), (util/->zdt util/end-of-time)]
                         ["Claire" "Cooper", (util/->zdt #inst "2019"), (util/->zdt util/end-of-time)]
                         ["Alan" "Andrews", (util/->zdt #inst "2020"), (util/->zdt util/end-of-time)]
                         ["Susan" "Smith", (util/->zdt #inst "2021") (util/->zdt util/end-of-time)]}]

      (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx1))

      (t/is (= tx1-expected (all-users !tx1)))

      ;; HACK: we're passing `util/end-of-time` as a param here - in practice the user'll likely want a built-in literal?
      (let [!tx2 (c2/submit-tx *node* [[:sql "DELETE FROM users FOR PORTION OF APP_TIME FROM ? TO ? AS u WHERE u.id = ?"
                                        [[#inst "2020-05-01", util/end-of-time, "dave"]]]])
            tx2-expected #{["Dave" "Davis", (util/->zdt #inst "2018"), (util/->zdt #inst "2020-05-01")]
                           ["Claire" "Cooper", (util/->zdt #inst "2019"), (util/->zdt util/end-of-time)]
                           ["Alan" "Andrews", (util/->zdt #inst "2020"), (util/->zdt util/end-of-time)]
                           ["Susan" "Smith", (util/->zdt #inst "2021") (util/->zdt util/end-of-time)]}]

        (t/is (= tx2-expected (all-users !tx2)))
        (t/is (= tx1-expected (all-users !tx1)))

        (let [!tx3 (c2/submit-tx *node* [[:sql "UPDATE users FOR PORTION OF APPLICATION_TIME FROM ? TO ? AS u SET first_name = 'Sue' WHERE u.id = ?"
                                          [[#inst "2021-07-01", util/end-of-time, "susan"]]]])

              tx3-expected #{["Dave" "Davis", (util/->zdt #inst "2018"), (util/->zdt #inst "2020-05-01")]
                             ["Claire" "Cooper", (util/->zdt #inst "2019"), (util/->zdt util/end-of-time)]
                             ["Alan" "Andrews", (util/->zdt #inst "2020"), (util/->zdt util/end-of-time)]
                             ["Susan" "Smith", (util/->zdt #inst "2021") (util/->zdt #inst "2021-07-01")]
                             ["Sue" "Smith", (util/->zdt #inst "2021-07-01") (util/->zdt util/end-of-time)]}]

          (t/is (= tx3-expected (all-users !tx3)))
          (t/is (= tx2-expected (all-users !tx2)))
          (t/is (= tx1-expected (all-users !tx1))))))))

(deftest test-sql-insert
  (let [!tx1 (c2/submit-tx *node*
                           [[:sql "INSERT INTO users (id, name, application_time_start) VALUES (?, ?, ?)"
                             [["dave", "Dave", #inst "2018"]
                              ["claire", "Claire", #inst "2019"]]]])

        _ (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx1))

        !tx2 (c2/submit-tx *node*
                           [[:sql "INSERT INTO people (id, renamed_name, application_time_start)
                                   SELECT users.id, users.name, users.application_time_start FROM users
                                   WHERE users.APP_TIME CONTAINS TIMESTAMP '2019-06-01 00:00:00' AND users.name = 'Dave'"]])]

    (t/is (= [{:renamed_name "Dave"}]
             (c2/sql-query *node* "SELECT people.renamed_name FROM people
                                   WHERE people.APP_TIME CONTAINS TIMESTAMP '2019-06-01 00:00:00'"
                           {:basis {:tx !tx2}})))))

(deftest test-dml-as-of-now-flag-339
  (let [tt1 (util/->zdt #inst "2020-01-01")
        tt2 (util/->zdt #inst "2020-01-02")
        tt3 (util/->zdt #inst "2020-01-03")
        tt4 (util/->zdt #inst "2020-01-04")
        tt5 (util/->zdt #inst "2020-01-05")
        eot (util/->zdt util/end-of-time)]
    (letfn [(q [!tx]
              (set (c2/sql-query *node*
                                 "SELECT foo.version, foo.application_time_start, foo.application_time_end FROM foo"
                                 {:basis {:tx !tx}})))]
      (let [!tx (c2/submit-tx *node*
                              [[:sql "INSERT INTO foo (id, version) VALUES (?, ?)"
                                [["foo", 0]]]])]
        (t/is (= #{{:version 0, :application_time_start tt1, :application_time_end eot}}
                 (q !tx))))

      (let [!tx (c2/submit-tx *node*
                              [[:sql "UPDATE foo SET version = 1 WHERE foo.id = 'foo'"
                                [[]]]]
                              {:app-time-as-of-now? true})]
        (t/is (= #{{:version 0, :application_time_start tt1, :application_time_end tt2}
                   {:version 1, :application_time_start tt2, :application_time_end eot}}
                 (q !tx))))

      (t/testing "`FOR PORTION OF` means flag is ignored"
        (let [!tx (c2/submit-tx *node*
                                [[:sql (str "UPDATE foo "
                                            "FOR PORTION OF APP_TIME FROM ? TO ? "
                                            "SET version = 2 WHERE foo.id = 'foo'")
                                  [[tt1 tt2]]]]
                                {:app-time-as-of-now? true})]
          (t/is (= #{{:version 2, :application_time_start tt1, :application_time_end tt2}
                     {:version 2, :application_time_start tt2, :application_time_end tt2} ; hmm...
                     {:version 1, :application_time_start tt2, :application_time_end eot}}
                   (q !tx)))))

      (let [!tx (c2/submit-tx *node*
                              [[:sql "UPDATE foo SET version = 3 WHERE foo.id = 'foo'"
                                [[]]]])]

        (t/is (= #{{:version 3, :application_time_start tt1, :application_time_end tt2}
                   {:version 2, :application_time_start tt2, :application_time_end tt2} ; hmm...
                   {:version 3, :application_time_start tt2, :application_time_end tt2} ; hmm...
                   {:version 3, :application_time_start tt2, :application_time_end eot}}
                 (q !tx))))

      (let [!tx (c2/submit-tx *node*
                              [[:sql "DELETE FROM foo WHERE foo.id = 'foo'"
                                [[]]]]
                              {:app-time-as-of-now? true})]

        (t/is (= #{{:version 3, :application_time_start tt1, :application_time_end tt2}
                   {:version 2, :application_time_start tt2, :application_time_end tt2} ; hmm...
                   {:version 3, :application_time_start tt2, :application_time_end tt2} ; hmm...
                   {:version 3, :application_time_start tt2, :application_time_end tt5}}
                 (q !tx)))))))
