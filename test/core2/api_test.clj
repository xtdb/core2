(ns core2.api-test
  (:require [clojure.test :as t :refer [deftest]]
            [core2.api :as c2]
            [core2.client :as client]
            [core2.test-util :as tu :refer [*node*]]
            [core2.sql.plan-test :as pt]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [core2.local-node :as node])
  (:import java.time.Duration
           java.util.concurrent.ExecutionException))

(defn- with-mock-clocks [f]
  (tu/with-opts {:core2.log/memory-log {:clock (tu/->mock-clock)}
                 :core2.tx-producer/tx-producer {:clock (tu/->mock-clock)}}
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
  (-> {:in-memory (t/join-fixtures [with-mock-clocks tu/with-node])
       :remote (t/join-fixtures [with-mock-clocks tu/with-node with-client])}

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
  (let [!tx (c2/submit-tx *node* [[:put {:id :foo, :list [1 2 ["foo" "bar"]]}]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= [{:id :foo
               :list [1 2 ["foo" "bar"]]}]
             (c2/datalog-query *node*
                               (-> '{:find [?id ?list]
                                     :where [[?id :list ?list]]}
                                   (assoc :basis {:tx !tx}
                                          :basis-timeout (Duration/ofSeconds 1))))))))

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
  [[:put {:id :jms, :name "James"}]
   [:put {:id :hak, :name "Håkan"}]
   [:put {:id :mat, :name "Matt"}]
   [:put {:id :wot, :name "Dan"}]])

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
  (let [!tx1 (c2/submit-tx *node* [[:sql
                                    (pt/plan-sql "INSERT INTO users (id, name, application_time_start) VALUES (?, ?, ?)")
                                    [["dave", "Dave", #inst "2018"]
                                     ["claire", "Claire", #inst "2019"]
                                     ["alan", "Alan", #inst "2020"]
                                     ["susan", "Susan", #inst "2021"]]]])]

    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx1))

    (t/is (= [{:name "Dave"} {:name "Claire"}]
             (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.APP_TIME CONTAINS TIMESTAMP '2019-06-01 00:00:00'"
                           {:basis {:tx !tx1}})))

    (t/is (= [{:name "Dave"} {:name "Claire"} {:name "Alan"}]
             (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.APP_TIME CONTAINS TIMESTAMP '2020-06-01 00:00:00'"
                           {:basis {:tx !tx1}})))

    (let [!tx2 (c2/submit-tx *node* [[:sql '[:delete {:table "users"
                                                      :app-time-start #inst "2020-05-01"}
                                             [:scan [_iid
                                                     {_table (= _table "users")}
                                                     {id (= id ?_0)}
                                                     application_time_start
                                                     {application_time_end (>= application_time_end #inst "2020-05-01")}]]]
                                      [["dave"]]]])]

      (t/is (= [{:name "Claire"} {:name "Alan"}]
               (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.APP_TIME CONTAINS TIMESTAMP '2020-06-01 00:00:00'"
                             {:basis {:tx !tx2}})))

      (t/is (= [{:name "Dave"} {:name "Claire"} {:name "Alan"}]
               (c2/sql-query *node* "SELECT users.name FROM users WHERE users.APP_TIME CONTAINS TIMESTAMP '2020-06-01 00:00:00'"
                             {:basis {:tx !tx1}})))

      (t/is (= [{:name "Dave"} {:name "Claire"}]
               (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.APP_TIME CONTAINS TIMESTAMP '2019-06-01 00:00:00'"
                             {:basis {:tx !tx2}}))))

    (let [!tx3 (c2/submit-tx *node* [[:sql '[:update {:table "users"
                                                      :app-time-start #inst "2021-07-01"}
                                             [:map [{name "Sue"}]
                                              [:scan [_iid
                                                      {id (= id ?_0)}
                                                      {_table (= _table "users")}
                                                      application_time_start
                                                      {application_time_end (>= application_time_end #inst "2021-07-01")}]]]]
                                      [["susan"]]]])]

      ;; TODO when we can return `application_time_start` etc from `:scan` without errors we can probably coalesce these tests
      (t/is (= [{:name "Susan"} {:name "Sue"}]
               (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.name IN ('Susan', 'Sue')"
                             {:basis {:tx !tx3}})))

      (t/is (= [{:name "Susan"}]
               (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.name IN ('Susan', 'Sue') AND u.APP_TIME CONTAINS TIMESTAMP '2021-05-01 00:00:00'"
                             {:basis {:tx !tx3}})))

      (t/is (= [{:name "Susan"}]
               (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.name IN ('Susan', 'Sue') AND u.APP_TIME CONTAINS TIMESTAMP '2021-08-01 00:00:00'"
                             {:basis {:tx !tx1}})))

      (t/is (= [{:name "Sue"}]
               (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.name IN ('Susan', 'Sue') AND u.APP_TIME CONTAINS TIMESTAMP '2021-08-01 00:00:00'"
                             {:basis {:tx !tx3}}))))))

(deftest test-sql-insert
  (let [!tx1 (c2/submit-tx *node*
                           [[:sql (pt/plan-sql "INSERT INTO users (id, name, application_time_start) VALUES (?, ?, ?)")
                             [["dave", "Dave", #inst "2018"]
                              ["claire", "Claire", #inst "2019"]]]])

        _ (t/is (= (c2/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) @!tx1))

        !tx2 (c2/submit-tx *node*
                           [[:sql (pt/plan-sql "INSERT INTO people (id, renamed_name, application_time_start)
                                               SELECT users.id, users.name, users.application_time_start FROM users
                                               WHERE users.APP_TIME CONTAINS TIMESTAMP '2019-06-01 00:00:00' AND users.name = 'Dave'")]])]

    (t/is (= [{:renamed_name "Dave"}]
             (c2/sql-query *node* "SELECT people.renamed_name FROM people
                                  WHERE people.APP_TIME CONTAINS TIMESTAMP '2019-06-01 00:00:00'"
                           {:basis {:tx !tx2}})))))
