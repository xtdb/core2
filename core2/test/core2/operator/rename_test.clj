(ns core2.operator.rename-test
  (:require [clojure.test :as t]
            [core2.operator.rename :as rename]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-rename
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                    [[{:a 12, :b 10}, {:a 0, :b 15}]
                                     [{:a 100, :b 83}]])
                rename-cursor (rename/->rename-cursor tu/*allocator* cursor
                                                      {"a" "c"}
                                                      nil)]
      (t/is (= [[{:c 12, :b 10}, {:c 0, :b 15}]
                [{:c 100, :b 83}]]
               (tu/<-cursor rename-cursor))))

    (t/testing "prefix"
      (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                      [[{:a 12, :b 10}, {:a 0, :b 15}]
                                       [{:a 100, :b 83}]])
                  rename-cursor (rename/->rename-cursor tu/*allocator* cursor
                                                        {"a" "c"}
                                                        "R")]
        (t/is (= [[{:R_c 12, :R_b 10}, {:R_c 0, :R_b 15}]
                  [{:R_c 100, :R_b 83}]]
                 (tu/<-cursor rename-cursor)))))

    (t/testing "prefix only"
      (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                      [[{:a 12, :b 10}, {:a 0, :b 15}]
                                       [{:a 100, :b 83}]])
                  rename-cursor (rename/->rename-cursor tu/*allocator* cursor
                                                        {}
                                                        "R")]
        (t/is (= [[{:R_a 12, :R_b 10}, {:R_a 0, :R_b 15}]
                  [{:R_a 100, :R_b 83}]]
                 (tu/<-cursor rename-cursor)))))))
