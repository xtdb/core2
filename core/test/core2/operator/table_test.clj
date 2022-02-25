(ns core2.operator.table-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.operator :as op]
            [core2.util :as util])
  (:import java.time.Duration))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-table
  (t/is (= [{:a 12, :b "foo" :c 1.2, :d nil, :e true, :f (Duration/ofHours 1)}
            {:a 100, :b "bar", :c 3.14, :d (util/->zdt #inst "2020"), :e 10, :f (Duration/ofMinutes 1)}]
           (op/query-ra '[:table #{a b c d e f} $table]
                        {'$table [{:a 12, :b "foo" :c 1.2 :d nil :e true :f (Duration/ofHours 1)}
                                  {:a 100, :b "bar", :c 3.14, :d #inst "2020", :e 10, :f (Duration/ofMinutes 1)}]})))

  (t/is (= [{:a 12, :b "foo" :c 1.2 :d nil :e true}
            {:a 100, :b "bar" :c 3.14 :d (util/->zdt #inst "2020") :e 10}]
           (op/query-ra '[:table [{:a 12, :b "foo", :c 1.2, :d nil, :e true}
                                  {:a 100, :b "bar", :c 3.14, :d #inst "2020", :e 10}]]
                        {}))
        "inline table")

  (t/is (empty? (op/query-ra '[:table $table]
                             {'$table []}))
        "empty")

  (t/is (thrown? IllegalArgumentException
                 (op/query-ra '[:table $table]
                              {'$table [{:a 12, :b "foo"}
                                        {:a 100}]}))
        "requires same columns")

  (t/is (thrown? IllegalArgumentException
                 (op/query-ra '[:table #{'a} $table]
                              {'$table [{:a 12, :b "foo"}]}))
        "doesn't match provided col-names"))
