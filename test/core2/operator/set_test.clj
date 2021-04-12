(ns core2.operator.set-test
  (:require [clojure.test :as t]
            [core2.operator.project :as project]
            [core2.operator.select :as select]
            [core2.operator.set :as set-op]
            [core2.expression :as expr]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import core2.select.IVectorSchemaRootSelector
           core2.operator.project.ProjectionSpec
           org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType
           org.roaringbitmap.RoaringBitmap))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-union
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 83 :b 3}]])
                union-cursor (set-op/->union-cursor tu/*allocator* left-cursor right-cursor)]

      (t/is (= [#{{:a 0, :b 15}
                  {:a 12, :b 10}}
                #{{:a 100, :b 15}}
                #{{:a 10, :b 1}
                  {:a 15, :b 2}}
                #{{:a 83, :b 3}}]
               (mapv set (tu/<-cursor union-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  union-cursor (set-op/->union-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor union-cursor)))))))

(t/deftest test-intersection
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 0 :b 15}]])
                intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

      (t/is (= [#{{:a 0, :b 15}}]
               (mapv set (tu/<-cursor intersection-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 10}, {:a 15}]])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 10}, {:a 15}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 10}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 20}]])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor)))))))

(t/deftest test-difference
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 0 :b 15}]])
                difference-cursor (set-op/->difference-cursor tu/*allocator* left-cursor right-cursor)]

      (t/is (= [#{{:a 12, :b 10}}
                #{{:a 100 :b 15}}]
               (mapv set (tu/<-cursor difference-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor difference-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 10}, {:a 15}]])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor difference-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 10}, {:a 15}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (= [#{{:a 10} {:a 15}}] (mapv set (tu/<-cursor difference-cursor))))))))

(t/deftest test-distinct
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]

    (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                       [[{:a 12 :b 10}, {:a 0 :b 15}]
                                        [{:a 100 :b 15} {:a 0 :b 15}]
                                        [{:a 100 :b 15}]
                                        [{:a 10 :b 15} {:a 10 :b 15}]])
                distinct-cursor (set-op/->distinct-cursor tu/*allocator* in-cursor)]

        (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
                  #{{:a 100, :b 15}}
                  #{{:a 10, :b 15}}]
                 (mapv set (tu/<-cursor distinct-cursor)))))


    (t/testing "already distinct"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                  distinct-cursor (set-op/->distinct-cursor tu/*allocator* in-cursor)]

        (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
                  #{{:a 100, :b 15}}]
                 (mapv set (tu/<-cursor distinct-cursor))))))

    (t/testing "empty input and output"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field])
                                          [])
                  distinct-cursor (set-op/->distinct-cursor tu/*allocator* in-cursor)]

        (t/is (empty? (tu/<-cursor distinct-cursor)))))))

(t/deftest test-fixpoint
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]

    (with-open [factorial-cursor (set-op/->fixpoint-cursor
                                  tu/*allocator*
                                  (reify core2.operator.set.IFixpointCursorFactory
                                    (createCursor [_ fixpoint-cursor-factory]
                                      (set-op/->union-cursor
                                       tu/*allocator*
                                       (tu/->cursor (Schema. [a-field b-field])
                                                    [[{:a 0 :b 1}]])
                                       (select/->select-cursor
                                        tu/*allocator*
                                        (project/->project-cursor
                                         tu/*allocator*
                                         (.createCursor fixpoint-cursor-factory)
                                         [(reify ProjectionSpec
                                            (project [_ in-root allocator]
                                              (let [^BigIntVector a-vec (.getVector in-root a-field)
                                                    ^BigIntVector out-vec (.createVector (ty/->field "a" (.getType Types$MinorType/BIGINT) false) tu/*allocator*)
                                                    row-count (.getRowCount in-root)]
                                                (.setValueCount out-vec row-count)
                                                (dotimes [idx row-count]
                                                  (.set out-vec idx (+ (.get a-vec idx) 1)))
                                                out-vec)))
                                          (reify ProjectionSpec
                                            (project [_ in-root allocator]
                                              (let [^BigIntVector a-vec (.getVector in-root a-field)
                                                    ^BigIntVector b-vec (.getVector in-root b-field)
                                                    ^BigIntVector out-vec (.createVector (ty/->field "b" (.getType Types$MinorType/BIGINT) false) tu/*allocator*)
                                                    row-count (.getRowCount in-root)]
                                                (.setValueCount out-vec row-count)
                                                (dotimes [idx row-count]
                                                  (.set out-vec idx (* (+ (.get a-vec idx) 1) (.get b-vec idx))))
                                                out-vec)))])
                                        (reify IVectorSchemaRootSelector
                                          (select [_ in-root]
                                            (let [idx-bitmap (RoaringBitmap.)]

                                              (dotimes [idx (.getRowCount in-root)]
                                                (when (<= (.get ^BigIntVector (.getVector in-root a-field) idx) 8)
                                                  (.add idx-bitmap idx)))

                                              idx-bitmap))))))))]

      (t/is (= [[{:a 0, :b 1}
                 {:a 1, :b 1}
                 {:a 2, :b 2}
                 {:a 3, :b 6}
                 {:a 4, :b 24}
                 {:a 5, :b 120}
                 {:a 6, :b 720}
                 {:a 7, :b 5040}
                 {:a 8, :b 40320}]]
               (tu/<-cursor factorial-cursor))))))
