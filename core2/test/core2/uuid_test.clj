(ns core2.uuid-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.test-util :as tu])
  (:import java.time.Duration))

(t/use-fixtures :each tu/with-node)

(t/deftest round-trips-uuids
  ;; https://xkcd.com/221/
  (let [random-uuid #uuid "4589bf8e-f301-42eb-ab28-402b4fe27869"]
    (c2/with-db [db tu/*node*
                 {:tx (c2/submit-tx tu/*node* [{:op :put, :doc {:_id random-uuid}}])
                  :timeout (Duration/ofMillis 250)}]
      (t/is (= [{:_id random-uuid}]
               (into [] (c2/plan-ra [:scan '[_id]] db)))))))
