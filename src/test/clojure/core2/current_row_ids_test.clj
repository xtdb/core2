(ns core2.current-row-ids-test
  (:require [clojure.test :as t :refer [deftest]]
            [core2.datalog :as c2]
            [core2.temporal :as temporal]
            [core2.test-util :as tu]
            )
  (:import org.roaringbitmap.longlong.Roaring64Bitmap))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(deftest test-current-row-ids
  (c2/submit-tx
    tu/*node*
    '[[:put xt_docs {:id :ivan, :first-name "Ivan"}]
      [:put xt_docs {:id :petr, :first-name "Petr"}
       {:app-time-start #inst "2020-01-02T12:00:00Z"}]
      [:put xt_docs {:id :susie, :first-name "Susie"}
       {:app-time-end #inst "2020-01-02T13:00:00Z"}]
      [:put xt_docs {:id :sam, :first-name "Sam"}]
      [:put xt_docs{:id :petr, :first-name "Petr"}
       {:app-time-start #inst "2020-01-04T12:00:00Z"}]
      [:put xt_docs {:id :jen, :first-name "Jen"}
       {:app-time-end #inst "2020-01-04T13:00:00Z"}]
      [:put xt_docs {:id :james, :first-name "James"}
       {:app-time-start #inst "2020-01-01T12:00:00Z"}]
      [:put xt_docs {:id :jon, :first-name "Jon"}
       {:app-time-end #inst "2020-01-01T12:00:00Z"}]
      [:put xt_docs {:id :lucy :first-name "Lucy"}]])

  (c2/submit-tx
    tu/*node*
    '[[:put xt_docs {:id :ivan, :first-name "Ivan-2"}
       {:app-time-start #inst "2020-01-02T14:00:00Z"}]
      [:put xt_docs {:id :ben, :first-name "Ben"}
       {:app-time-start #inst "2020-01-02T14:00:00Z"
        :app-time-end #inst "2020-01-02T15:00:00Z"}]
      [:evict xt_docs :lucy]])

  (t/is (= [{:name "Ivan-2"}
            {:name "James"}
            {:name "Jen"}
            {:name "Petr"}
            {:name "Sam"}]
           (c2/q
             tu/*node*
             (-> '{:find [name]
                   :where [(match xt_docs {:first-name name})]
                   :order-by [[name :asc]]}
                 (assoc :basis {:current-time #time/instant "2020-01-03T00:00:00Z"})))))) ;; timing

(defn valid-ids-at [current-time]
  (c2/q
    tu/*node*
    (-> '{:find [id]
          :where [(match xt_docs {:id id})]}
        (assoc :basis {:current-time current-time}))))

(deftest test-current-row-ids-app-time-start-inclusivity
  (t/testing "app-time-start"
    (c2/submit-tx
      tu/*node*
      '[[:put xt_docs {:id 1}
         {:app-time-start #inst "2020-01-01T00:00:02Z"}]])

    (t/is (= []
             (valid-ids-at #time/instant "2020-01-01T00:00:01Z")))
    (t/is (= [{:id 1}]
             (valid-ids-at #time/instant "2020-01-01T00:00:02Z")))
    (t/is (= [{:id 1}]
             (valid-ids-at #time/instant "2020-01-01T00:00:03Z")))))

(deftest test-current-row-ids-app-time-end-inclusivity
  (t/testing "app-time-start"
    (c2/submit-tx
      tu/*node*
      '[[:put xt_docs {:id 1}
         {:app-time-end #inst "2020-01-01T00:00:02Z"}]])

    (t/is (= [{:id 1}]
             (valid-ids-at #time/instant "2020-01-01T00:00:01Z")))
    (t/is (= []
             (valid-ids-at #time/instant "2020-01-01T00:00:02Z")))
    (t/is (= []
             (valid-ids-at #time/instant "2020-01-01T00:00:03Z")))))


(deftest remove-evicted-row-ids-test
  (t/is
    (= #{1 3}
       (temporal/remove-evicted-row-ids
         #{1 2 3}
         (doto
           (Roaring64Bitmap.)
           (.addLong (long 2)))))))
