(ns core2.expression-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.test-util :as test-util]
            [core2.util :as util])
  (:import org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector BigIntVector BitVector FieldVector Float8Vector ValueVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.util.Text))

(t/deftest can-compile-simple-expression
  (with-open [allocator (RootAllocator.)
              a (Float8Vector. "a" allocator)
              b (Float8Vector. "b" allocator)
              d (BigIntVector. "d" allocator)
              e (VarCharVector. "e" allocator)]
    (let [rows 1000]
      (.setValueCount a rows)
      (.setValueCount b rows)
      (.setValueCount d rows)
      (.setValueCount e rows)
      (dotimes [n rows]
        (.set a n (double n))
        (.set b n (double n))
        (.set d n n)
        (.setSafe e n (Text. (format "%04d" n)))))

    (with-open [in (VectorSchemaRoot/of (into-array FieldVector [a b d]))]
      (let [expr (expr/form->expr '(+ a b))
            expr-projection-spec (expr/->expression-projection-spec "c" expr)]
        (t/is (= '[a b] (expr/variables expr)))
        (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
          (let [v (util/maybe-single-child-dense-union acc)
                xs (test-util/->list v)]
            (t/is (instance? Float8Vector v))
            (t/is (= (mapv (comp double +) (range 1000) (range 1000))
                     xs)))))

      (let [expr (expr/form->expr '(- a (* 2.0 b)))
            expr-projection-spec (expr/->expression-projection-spec "c" expr)]
        (t/is (= '[a b] (expr/variables expr)))
        (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
          (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
            (t/is (= (mapv (comp double -) (range 1000) (map (partial * 2) (range 1000)))
                     xs)))))

      (t/testing "support keyword and vectors"
        (let [expr (expr/form->expr '[:+ a [:+ b 2]])
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (= '[a b] (expr/variables expr)))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
              (t/is (= (mapv (comp double +) (range 1000) (range 1000) (repeat 2))
                       xs))))))

      (t/testing "mixing types"
        (let [expr (expr/form->expr '(+ 2 d))
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (= '[d] (expr/variables expr)))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list (util/maybe-single-child-dense-union acc))]
              (t/is (instance? BigIntVector v))
              (t/is (= (mapv + (repeat 2) (range 1000))
                       xs)))))

        (let [expr (expr/form->expr '(+ 2.0 d))
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (= '[d] (expr/variables expr)))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list (util/maybe-single-child-dense-union acc))]
              (t/is (instance? Float8Vector v))
              (t/is (= (mapv (comp double +) (repeat 2) (range 1000))
                       xs))))))

      (t/testing "predicate"
        (let [expr (expr/form->expr '(= a d))
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (= '[a d] (expr/variables expr)))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list v)]
              (t/is (instance? BitVector v))
              (t/is (= (repeat 1000 true) xs))))))

      (t/testing "math"
        (let [expr (expr/form->expr '(sin a))
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (= '[a] (expr/variables expr)))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list v)]
              (t/is (instance? Float8Vector v))
              (t/is (= (mapv #(Math/sin ^double %) (range 1000)) xs))))))

      (t/testing "if"
        (let [expr (expr/form->expr '(if false a 0))
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (= '[a] (expr/variables expr)))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list v)]
              (t/is (instance? Float8Vector v))
              (t/is (= (repeat 1000 0.0) xs))))))

      (t/testing "cannot call arbitrary functions"
        (let [expr (expr/form->expr '(vec a))
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (thrown? IllegalArgumentException (.project expr-projection-spec in allocator)))))

      (t/testing "selector"
        (let [expr (expr/form->expr '(>= a 500))
              selector (expr/->expression-root-selector expr)]
          (t/is (= '[a] (expr/variables expr)))
          (t/is (= 500 (.getCardinality (.select selector in)))))

        (let [expr (expr/form->expr '(>= a 500))
              selector (expr/->expression-vector-selector expr)]
          (t/is (= '[a] (expr/variables expr)))
          (t/is (= 500 (.getCardinality (.select selector a)))))

        (let [expr (expr/form->expr '(>= e "0500"))
              selector (expr/->expression-vector-selector expr)]
          (t/is (= '[e] (expr/variables expr)))
          (t/is (= 500 (.getCardinality (.select selector e)))))))))
