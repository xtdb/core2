(ns core2.operator.unwind-test
  (:require [clojure.test :as t]
            [core2.operator :as op]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-unwind
  (let [in-vals [[{:a 1 :b [1 2]} {:a 2 :b [3 4 5]}]
                 [{:a 3 :b []}]
                 [{:a 4 :b [6 7 8]} {:a 5 :b []}]]]

    (with-open [res (op/open-ra [:unwind '{b* b}
                                 [::tu/blocks '{a :i64, b [:list :i64]} in-vals]])]
      (t/is (= '{a :i64, b [:list :i64], b* :i64}
               (.columnTypes res)))

      (t/is (= [[{:a 1, :b [1 2], :b* 1}
                 {:a 1, :b [1 2], :b* 2}
                 {:a 2, :b [3 4 5], :b* 3}
                 {:a 2, :b [3 4 5], :b* 4}
                 {:a 2, :b [3 4 5], :b* 5}]
                [{:a 4, :b [6 7 8], :b* 6}
                 {:a 4, :b [6 7 8], :b* 7}
                 {:a 4, :b [6 7 8], :b* 8}]]
               (tu/<-cursor res))))

    (with-open [res (op/open-ra [:unwind '{b* b} '{:ordinality-column $ordinal}
                                 [::tu/blocks '{a :i64, b [:list :i64]} in-vals]])]
      (t/is (= '{a :i64, b [:list :i64], b* :i64, $ordinal :i32}
               (.columnTypes res)))

      (t/is (= [[{:a 1, :b [1 2], :b* 1, :$ordinal 1}
                 {:a 1, :b [1 2], :b* 2, :$ordinal 2}
                 {:a 2, :b [3 4 5], :b* 3, :$ordinal 1}
                 {:a 2, :b [3 4 5], :b* 4, :$ordinal 2}
                 {:a 2, :b [3 4 5], :b* 5, :$ordinal 3}]
                [{:a 4, :b [6 7 8], :b* 6, :$ordinal 1}
                 {:a 4, :b [6 7 8], :b* 7, :$ordinal 2}
                 {:a 4, :b [6 7 8], :b* 8, :$ordinal 3}]]
               (tu/<-cursor res))))))
