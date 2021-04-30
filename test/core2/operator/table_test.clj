(ns core2.operator.table-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-table
  (with-open [table-cursor (c2/open-q {'table [{:a 12, :b "foo" :c 1.2 :d nil :e true}
                                               {:a 100, :b "bar" :c 3.14 :d #inst "2020" :e 10}]}
                                      '[:table table])]
    (t/is (= [[{:a 12, :b "foo" :c 1.2 :d nil :e true}
               {:a 100, :b "bar" :c 3.14 :d #inst "2020" :e 10}]]
             (tu/<-cursor table-cursor))))

  (t/testing "inline table"
    (with-open [table-cursor (c2/open-q {}
                                        '[:table [{:a 12, :b "foo" :c 1.2 :d nil :e true}
                                                  {:a 100, :b "bar" :c 3.14 :d #inst "2020" :e 10}]])]
      (t/is (= [[{:a 12, :b "foo" :c 1.2 :d nil :e true}
                 {:a 100, :b "bar" :c 3.14 :d #inst "2020" :e 10}]]
               (tu/<-cursor table-cursor)))))

  (t/testing "empty"
    (with-open [table-cursor (c2/open-q {'table []}
                                        '[:table table])]
      (t/is (empty? (tu/<-cursor table-cursor)))))

  (t/testing "requires same columns"
    (t/is (thrown? IllegalArgumentException
                   (with-open [_cursor (c2/open-q {'table [{:a 12, :b "foo"}
                                                           {:a 100}]}
                                                  '[:table table])])))))
