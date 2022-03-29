(ns core2.expression-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.expression.temporal :as expr.temp]
            [core2.local-node :as node]
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.types.LegType
           [java.time Duration ZonedDateTime ZoneId LocalDate]
           [org.apache.arrow.vector DurationVector TimeStampVector ValueVector]
           [org.apache.arrow.vector.types.pojo ArrowType$Duration ArrowType$FixedSizeList ArrowType$Timestamp ArrowType$Union FieldType]
           org.apache.arrow.vector.types.TimeUnit))

(t/use-fixtures :each tu/with-allocator)

(defn ->data-vecs []
  [(tu/->mono-vec "a" types/float8-type (map double (range 1000)))
   (tu/->mono-vec "b" types/float8-type (map double (range 1000)))
   (tu/->mono-vec "d" types/bigint-type (range 1000))
   (tu/->mono-vec "e" types/varchar-type (map #(format "%04d" %) (range 1000)))])

(defn- open-rel ^core2.vector.IIndirectRelation [vecs]
  (iv/->indirect-rel (map iv/->direct-vec vecs)))

(t/deftest test-simple-projection
  (with-open [in-rel (open-rel (->data-vecs))]
    (letfn [(project [form]
              (with-open [project-col (.project (expr/->expression-projection-spec "c" form {})
                                                tu/*allocator* in-rel)]
                (tu/<-column project-col)))]

      (t/is (= (mapv (comp double +) (range 1000) (range 1000))
               (project '(+ a b))))

      (t/is (= (mapv (comp double -) (range 1000) (map (partial * 2) (range 1000)))
               (project '(- a (* 2.0 b)))))

      (t/is (= (mapv (comp double +) (range 1000) (range 1000) (repeat 2))
               (project '(:+ a (:+ b 2))))
            "support keywords")

      (t/is (= (mapv + (repeat 2) (range 1000))
               (project '(+ 2 d)))
            "mixing types")

      (t/is (= (repeat 1000 true)
               (project '(= a d)))
            "predicate")

      (t/is (= (mapv #(Math/sin ^double %) (range 1000))
               (project '(sin a)))
            "math")

      (t/is (= (interleave (map float (range)) (repeat 500 0))
               (project '(if (= 0 (% a 2)) (/ a 2) 0)))
            "if")

      (t/is (thrown? IllegalArgumentException (project '(vec a)))
            "cannot call arbitrary functions"))))

(t/deftest can-compile-simple-expression
  (with-open [in-rel (open-rel (->data-vecs))]
    (letfn [(select-relation [form params]
              (alength (.select (expr/->expression-relation-selector form params)
                                tu/*allocator*
                                in-rel)))]

      (t/testing "selector"
        (t/is (= 500 (select-relation '(>= a 500) {})))
        (t/is (= 500 (select-relation '(>= e "0500") {}))))

      (t/testing "parameter"
        (t/is (= 500 (select-relation '(>= a ?a) {'?a 500})))
        (t/is (= 500 (select-relation '(>= e ?e) {'?e "0500"})))))))

(t/deftest can-extract-min-max-range-from-expression
  (let [μs-2018 (util/instant->micros (util/->instant #inst "2018"))
        μs-2019 (util/instant->micros (util/->instant #inst "2019"))]
    (letfn [(transpose [[mins maxs]]
              (->> (map vector mins maxs)
                   (zipmap [:tt-end :id :tt-start :row-id :vt-start :vt-end])
                   (into {} (remove (comp #{[Long/MIN_VALUE Long/MAX_VALUE]} val)))))]
      (t/is (= {:vt-start [Long/MIN_VALUE μs-2019]
                :vt-end [(inc μs-2019) Long/MAX_VALUE]}
               (transpose (expr.temp/->temporal-min-max-range
                           {"_valid-time-start" '(<= _vt-time-start #inst "2019")
                            "_valid-time-end" '(> _vt-time-end #inst "2019")}
                           {}))))

      (t/is (= {:vt-start [μs-2019 μs-2019]}
               (transpose (expr.temp/->temporal-min-max-range
                           {"_valid-time-start" '(= _vt-time-start #inst "2019")}
                           {}))))

      (t/testing "symbol column name"
        (t/is (= {:vt-start [μs-2019 μs-2019]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {'_valid-time-start '(= _vt-time-start #inst "2019")}
                             {})))))

      (t/testing "conjunction"
        (t/is (= {:vt-start [Long/MIN_VALUE μs-2019]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"_valid-time-start" '(and (<= _vt-time-start #inst "2019")
                                                        (<= _vt-time-start #inst "2020"))}
                             {})))))

      (t/testing "disjunction not supported"
        (t/is (= {}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"_valid-time-start" '(or (= _vt-time-start #inst "2019")
                                                       (= _vt-time-start #inst "2020"))}
                             {})))))

      (t/testing "parameters"
        (t/is (= {:vt-start [μs-2018 Long/MAX_VALUE]
                  :vt-end [Long/MIN_VALUE (dec μs-2018)]
                  :tt-start [Long/MIN_VALUE μs-2019]
                  :tt-end [(inc μs-2019) Long/MAX_VALUE]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"_tx-time-start" '(>= ?tt _tx-time-start)
                              "_tx-time-end" '(< ?tt _tx-time-end)
                              "_valid-time-start" '(<= ?vt _vt-time-start)
                              "_valid-time-end" '(> ?vt _vt-time-end)}
                             {'?tt (util/->instant #inst "2019",) '?vt (util/->instant #inst "2018")}))))))))

(defn project
  "Use to test an expression on some example documents. See also, project1.

  Usage: (project '(+ a b) [{:a 1, :b 2}, {:a 3, :b 4}]) ;; => [3, 7]"
  [expr docs]
  (with-open [node (node/start-node {})]
    (let [docs (map-indexed #(assoc %2 :_id %1) docs)
          db (snap/snapshot (tu/component node ::snap/snapshot-factory))
          lp [:project [{'ret expr}] [:table docs]]]
      (mapv :ret (op/query-ra lp db)))))

(defn project1 [expr doc] (first (project expr [doc])))

(t/deftest test-date-trunc
  (let [test-doc {:_id  :foo,
                  :date (util/->instant #inst "2021-10-21T12:34:56Z")
                  :zdt  (-> (util/->zdt #inst "2021-08-21T12:34:56Z")
                            (.withZoneSameLocal (ZoneId/of "Europe/London")))}]
    (letfn [(simple-trunc [time-unit] (project1 (list 'date-trunc time-unit 'date) test-doc))]

      (t/is (= (util/->zdt #inst "2021-10-21") (simple-trunc "DAY")))
      (t/is (= (util/->zdt #inst "2021-10-21T12:34") (simple-trunc "MINUTE")))
      (t/is (= (util/->zdt #inst "2021-10-01") (simple-trunc "MONTH")))
      (t/is (= (util/->zdt #inst "2021-01-01") (simple-trunc "YEAR"))))

    (t/is (= (-> (util/->zdt #inst "2021-08-21")
                 (.withZoneSameLocal (ZoneId/of "Europe/London")))
             (project1 '(date-trunc "DAY" zdt) test-doc))
          "timezone aware")

    (t/is (= (util/->zdt #inst "2021-10-21") (project1 '(date-trunc "DAY" date) test-doc)))

    (t/is (= (util/->zdt #inst "2021-10-21") (project1 '(date-trunc "DAY" (date-trunc "MINUTE" date)) test-doc)))

    (t/testing "java.time.LocalDate"
      (let [ld (LocalDate/of 2022 3 29)
            trunc #(project1 (list 'date-trunc % 'date) {:date ld})]
        (t/is (= (LocalDate/of 2022 3 29) (trunc "DAY")))
        (t/is (= (LocalDate/of 2022 3 1) (trunc "MONTH")))
        (t/is (= (LocalDate/of 2022 1 1) (trunc "YEAR")))
        (t/is (= (LocalDate/of 2022 1 1) (project1 '(date-trunc "YEAR" (date-trunc "MONTH" date)) {:date ld})))))))

(t/deftest test-date-extract
  ;; todo units below minute are not yet implemented for any type
  (letfn [(extract [part date-like] (project1 (list 'extract part 'date) {:date date-like}))
          (extract-all [part date-likes] (project (list 'extract part 'date) (map (partial array-map :date) date-likes)))]
    (t/testing "java.time.Instant"
      (let [inst (util/->instant #inst "2022-03-21T13:44:52.344")]
        (t/is (= 44 (extract "MINUTE" inst)))
        (t/is (= 13 (extract "HOUR" inst)))
        (t/is (= 21 (extract "DAY" inst)))
        (t/is (= 3 (extract "MONTH" inst)))
        (t/is (= 2022 (extract "YEAR" inst)))))

    (t/testing "java.time.ZonedDateTime"
      (let [zdt (-> (util/->zdt #inst "2022-03-21T13:44:52.344")
                    (.withZoneSameLocal (ZoneId/of "Europe/London")))]
        (t/is (= 44 (extract "MINUTE" zdt)))
        (t/is (= 13 (extract "HOUR" zdt)))
        (t/is (= 21 (extract "DAY" zdt)))
        (t/is (= 3 (extract "MONTH" zdt)))
        (t/is (= 2022 (extract "YEAR" zdt)))))

    (t/testing "java.time.LocalDate"
      (let [ld (LocalDate/of 2022 03 21)]
        (t/is (= 0 (extract "MINUTE" ld)))
        (t/is (= 0 (extract "HOUR" ld)))
        (t/is (= 21 (extract "DAY" ld)))
        (t/is (= 3 (extract "MONTH" ld)))
        (t/is (= 2022 (extract "YEAR" ld)))))

    (t/testing "mixed types"
      (let [dates [(util/->instant #inst "2022-03-22T13:44:52.344")
                   (-> (util/->zdt #inst "2021-02-23T21:19:10.692")
                       (.withZoneSameLocal (ZoneId/of "Europe/London")))
                   (LocalDate/of 2020 04 18)]]
        (t/is (= [44 19 0] (extract-all "MINUTE" dates)))
        (t/is (= [13 21 0] (extract-all "HOUR" dates)))
        (t/is (= [22 23 18] (extract-all "DAY" dates)))
        (t/is (= [3 2 4] (extract-all "MONTH" dates)))
        (t/is (= [2022 2021 2020] (extract-all "YEAR" dates)))))))

(defn- run-projection [rel form]
  (with-open [out-ivec (.project (expr/->expression-projection-spec "out" form {})
                                 tu/*allocator*
                                 rel)]
    {:res (tu/<-column out-ivec)
     :leg-type (let [out-field (.getField (.getVector out-ivec))]
                 (if (instance? ArrowType$Union (.getType out-field))
                   (->> (.getChildren out-field)
                        (into #{} (map types/field->leg-type)))
                   (types/field->leg-type out-field)))
     :nullable? (.isNullable (.getField (.getVector out-ivec)))}))

(t/deftest test-variadics
  (letfn [(run-test [f x y z]
            (with-open [rel (open-rel [(tu/->mono-vec "x" types/bigint-type [x])
                                       (tu/->mono-vec "y" types/bigint-type [y])
                                       (tu/->mono-vec "z" types/bigint-type [z])])]
              (-> (run-projection rel (list f 'x 'y 'z))
                  :res first)))]

    (t/is (= 6 (run-test '+ 1 2 3)))
    (t/is (= 1 (run-test '- 4 2 1)))
    (t/is (true? (run-test '< 1 2 4)))
    (t/is (false? (run-test '> 4 1 2)))))

(defn- project-mono-value [f-sym val vector-type]
  (with-open [rel (open-rel [(tu/->mono-vec "s" vector-type [val])])]
    (-> (run-projection rel (list f-sym 's))
        :res
        first)))

(t/deftest test-character-length
  (letfn [(len [s vec-type] (project-mono-value 'character-length s vec-type))]
    (t/are [s]
      (= (count s) (len s types/varchar-type))
      ""
      "a"
      "hello"
      "😀")))

(t/deftest test-octet-length
  (letfn [(len [s vec-type] (project-mono-value 'octet-length s vec-type))]
    (t/are [s]
      (= (alength (.getBytes s "utf-8")) (len s types/varchar-type) (len s types/varbinary-type))
      ""
      "a"
      "hello"
      "😀")))

(t/deftest test-min-max
  (letfn [(run-test [form vecs]
            (with-open [rel (open-rel vecs)]
              (-> (run-projection rel form)
                  :res first)))]
    (t/is (= 9 (run-test '(max x y)
                         [(tu/->mono-vec "x" types/bigint-type [1])
                          (tu/->mono-vec "y" types/bigint-type [9])])))
    (t/is (= 1.0 (run-test '(min x y)
                           [(tu/->mono-vec "x" types/float8-type [1.0])
                            (tu/->mono-vec "y" types/float8-type [9.0])])))))

(t/deftest can-return-string-multiple-times
  (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable types/bigint-type) [1 2 3])])]
    (t/is (= {:res ["foo" "foo" "foo"]
              :leg-type LegType/UTF8
              :nullable? false}
             (run-projection rel "foo")))))

(t/deftest test-cond
  (letfn [(run-test [expr xs]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable types/bigint-type) xs)])]
              (run-projection rel expr)))]

    (t/is (= {:res ["big" "small" "tiny" "tiny"]
              :leg-type LegType/UTF8
              :nullable? false}
             (run-test '(cond (> x 100) "big", (> x 10) "small", "tiny")
                       [500 50 5 nil])))

    (t/is (= {:res ["big" "small" nil nil]
              :leg-type LegType/UTF8
              :nullable? true}
             (run-test '(cond (> x 100) "big", (> x 10) "small")
                       [500 50 5 nil])))))

(t/deftest test-let
  (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable types/bigint-type) [1 2 3 nil])])]
    (t/is (= {:res [6 9 12 nil]
              :leg-type LegType/BIGINT
              :nullable? true}
             (run-projection rel '(let [y (* x 2)
                                        y (+ y 3)]
                                    (+ x y)))))))

(t/deftest test-case
  (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable types/bigint-type) [1 2 3 nil])])]
    (t/is (= {:res ["x=1" "x=2" "none of the above" "none of the above"]
              :leg-type LegType/UTF8
              :nullable? false}
             (run-projection rel '(case (* x 2)
                                    2 "x=1"
                                    (+ x 2) "x=2"
                                    "none of the above"))))))

(t/deftest test-coalesce
  (letfn [(run-test [expr]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable types/varchar-type) ["x" nil nil])
                                       (tu/->mono-vec "y" (FieldType/nullable types/varchar-type) ["y" "y" nil])])]
              (run-projection rel expr)))]

    (t/is (= {:res ["x" "y" nil]
              :leg-type LegType/UTF8
              :nullable? true}
             (run-test '(coalesce x y))))

    (t/is (= {:res ["x" "lit" "lit"]
              :leg-type LegType/UTF8
              :nullable? false}
             (run-test '(coalesce x "lit" y))))

    (t/is (= {:res ["x" "y" "default"]
              :leg-type LegType/UTF8
              :nullable? false}
             (run-test '(coalesce x y "default"))))))

(t/deftest test-mixing-numeric-types
  (letfn [(run-test [f x y]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (.arrowType (types/value->leg-type x)) [x])
                                       (tu/->mono-vec "y" (.arrowType (types/value->leg-type y)) [y])])]
              (-> (run-projection rel (list f 'x 'y))
                  (update :res first)
                  (dissoc :nullable?))))]

    (t/is (= {:res 6, :leg-type LegType/INT}
             (run-test '+ (int 4) (int 2))))

    (t/is (= {:res 6, :leg-type LegType/BIGINT}
             (run-test '+ (int 2) (long 4))))

    (t/is (= {:res 6, :leg-type LegType/SMALLINT}
             (run-test '+ (short 2) (short 4))))

    (t/is (= {:res 6.5, :leg-type LegType/FLOAT4}
             (run-test '+ (byte 2) (float 4.5))))

    (t/is (= {:res 6.5, :leg-type LegType/FLOAT4}
             (run-test '+ (float 2) (float 4.5))))

    (t/is (= {:res 6.5, :leg-type LegType/FLOAT8}
             (run-test '+ (float 2) (double 4.5))))

    (t/is (= {:res 6.5, :leg-type LegType/FLOAT8}
             (run-test '+ (int 2) (double 4.5))))

    (t/is (= {:res -2, :leg-type LegType/INT}
             (run-test '- (short 2) (int 4))))

    (t/is (= {:res 8, :leg-type LegType/SMALLINT}
             (run-test '* (byte 2) (short 4))))

    (t/is (= {:res 2, :leg-type LegType/SMALLINT}
             (run-test '/ (short 4) (byte 2))))

    (t/is (= {:res 2.0, :leg-type LegType/FLOAT4}
             (run-test '/ (float 4) (int 2))))))

(t/deftest test-throws-on-overflow
  (letfn [(run-unary-test [f x]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (.arrowType (types/value->leg-type x)) [x])])]
              (-> (run-projection rel (list f 'x))
                  (update :res first))))

          (run-binary-test [f x y]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (.arrowType (types/value->leg-type x)) [x])
                                       (tu/->mono-vec "y" (.arrowType (types/value->leg-type y)) [y])])]
              (-> (run-projection rel (list f 'x 'y))
                  (update :res first))))]

    (t/is (thrown? ArithmeticException
                   (run-binary-test '+ (Integer/MAX_VALUE) (int 4))))

    (t/is (thrown? ArithmeticException
                   (run-binary-test '- (Integer/MIN_VALUE) (int 4))))

    (t/is (thrown? ArithmeticException
                   (run-unary-test '- (Integer/MIN_VALUE))))

    (t/is (thrown? ArithmeticException
                   (run-binary-test '* (Integer/MIN_VALUE) (int 2))))

    #_ ; TODO this one throws IAE because that's what clojure.lang.Numbers/shortCast throws
    ;; the others are thrown by java.lang.Math/*Exact, which throw ArithmeticException
    (t/is (thrown? ArithmeticException
                   (run-unary-test '- (Short/MIN_VALUE))))))

(t/deftest test-polymorphic-columns
  (t/is (= {:res [1.2 1 3.4]
            :leg-type #{LegType/FLOAT8 LegType/BIGINT}
            :nullable? false}
           (with-open [rel (open-rel [(tu/->duv "x" [1.2 1 3.4])])]
             (run-projection rel 'x))))

  (t/is (= {:res [4.4 9.75]
            :leg-type #{LegType/FLOAT4 LegType/FLOAT8}
            :nullable? false}
           (with-open [rel (open-rel [(tu/->duv "x" [1 1.5])
                                      (tu/->duv "y" [3.4 (float 8.25)])])]
             (run-projection rel '(+ x y)))))

  (t/is (= {:res [(float 4.4) nil nil nil]
            :leg-type #{LegType/NULL LegType/FLOAT4 LegType/FLOAT8}
            :nullable? false}
           (with-open [rel (open-rel [(tu/->duv "x" [1 12 nil nil])
                                      (tu/->duv "y" [(float 3.4) nil 4.8 nil])])]
             (run-projection rel '(+ x y))))))

(t/deftest test-ternary-booleans
  (t/is (= [{:res [true false nil false false false nil false nil]
             :leg-type LegType/BOOL, :nullable? true}
            {:res [true true true true false nil true nil nil]
             :leg-type LegType/BOOL, :nullable? true}]
           (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType. true types/bool-type nil)
                                                     [true true true false false false nil nil nil])
                                      (tu/->duv "y" [true false nil true false nil true false nil])])]
             [(run-projection rel '(and x y))
              (run-projection rel '(or x y))])))

  (t/is (= [{:res [false true nil]
             :leg-type LegType/BOOL, :nullable? true}
            {:res [true false false]
             :leg-type LegType/BOOL, :nullable? false}
            {:res [false true false]
             :leg-type LegType/BOOL, :nullable? false}
            {:res [false false true]
             :leg-type LegType/BOOL, :nullable? false}]
           (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType. true types/bool-type nil) [true false nil])])]
             [(run-projection rel '(not x))
              (run-projection rel '(true? x))
              (run-projection rel '(false? x))
              (run-projection rel '(nil? x))]))))

(t/deftest test-mixing-timestamp-types
  (letfn [(->ts-vec [col-name time-unit, ^long value]
            (doto ^TimeStampVector (.createVector (types/->field col-name (ArrowType$Timestamp. time-unit "UTC") false) tu/*allocator*)
              (.setValueCount 1)
              (.set 0 value)))

          (->dur-vec [col-name ^TimeUnit time-unit, ^long value]
            (doto (DurationVector. (types/->field col-name (ArrowType$Duration. time-unit) false) tu/*allocator*)
              (.setValueCount 1)
              (.set 0 value)))

          (test-projection [f-sym ->x-vec ->y-vec]
            (with-open [^ValueVector x-vec (->x-vec)
                        ^ValueVector y-vec (->y-vec)]
              (-> (run-projection (iv/->indirect-rel [(iv/->direct-vec x-vec)
                                                      (iv/->direct-vec y-vec)])
                                  (list f-sym 'x 'y))
                  (dissoc :nullable?))))]

    (t/testing "ts/dur"
      (t/is (= {:res [(util/->zdt #inst "2021-01-01T00:02:03Z")]
                :leg-type (LegType. (ArrowType$Timestamp. TimeUnit/SECOND "UTC"))}
               (test-projection '+
                                #(->ts-vec "x" TimeUnit/SECOND (.getEpochSecond (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/SECOND 123))))

      (t/is (= {:res [(util/->zdt #inst "2021-01-01T00:00:00.123Z")]
                :leg-type (LegType. (ArrowType$Timestamp. TimeUnit/MILLISECOND "UTC"))}
               (test-projection '+
                                #(->ts-vec "x" TimeUnit/SECOND (.getEpochSecond (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/MILLISECOND 123))))

      (t/is (= {:res [(ZonedDateTime/parse "1970-01-01T00:02:34.000001234Z[UTC]")]
                :leg-type (LegType. (ArrowType$Timestamp. TimeUnit/NANOSECOND "UTC"))}
               (test-projection '+
                                #(->dur-vec "x" TimeUnit/SECOND 154)
                                #(->ts-vec "y" TimeUnit/NANOSECOND 1234))))

      (t/is (thrown? ArithmeticException
                     (test-projection '+
                                      #(->ts-vec "x" TimeUnit/MILLISECOND (- Long/MAX_VALUE 500))
                                      #(->dur-vec "y" TimeUnit/SECOND 1))))

      (t/is (= {:res [(util/->zdt #inst "2020-12-31T23:59:59.998Z")]
                :leg-type (LegType. (ArrowType$Timestamp. TimeUnit/MICROSECOND "UTC"))}
               (test-projection '-
                                #(->ts-vec "x" TimeUnit/MICROSECOND (util/instant->micros (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/MILLISECOND 2)))))

    (t/is (t/is (= {:res [(Duration/parse "PT23H59M59.999S")]
                    :leg-type (LegType. (ArrowType$Duration. TimeUnit/MILLISECOND))}
                   (test-projection '-
                                    #(->ts-vec "x" TimeUnit/MILLISECOND (.toEpochMilli (util/->instant #inst "2021-01-02")))
                                    #(->ts-vec "y" TimeUnit/MILLISECOND (.toEpochMilli (util/->instant #inst "2021-01-01T00:00:00.001Z")))))))

    (t/testing "durations"
      (letfn [(->bigint-vec [^String col-name, ^long value]
                (tu/->mono-vec col-name types/bigint-type [value]))

              (->float8-vec [^String col-name, ^double value]
                (tu/->mono-vec col-name types/float8-type [value]))]

        (t/is (= {:res [(Duration/parse "PT0.002001S")]
                  :leg-type LegType/DURATIONMICRO}
                 (test-projection '+
                                  #(->dur-vec "x" TimeUnit/MICROSECOND 1)
                                  #(->dur-vec "y" TimeUnit/MILLISECOND 2))))

        (t/is (= {:res [(Duration/parse "PT-1.999S")]
                  :leg-type (LegType. (ArrowType$Duration. TimeUnit/MILLISECOND))}
                 (test-projection '-
                                  #(->dur-vec "x" TimeUnit/MILLISECOND 1)
                                  #(->dur-vec "y" TimeUnit/SECOND 2))))

        (t/is (= {:res [(Duration/parse "PT0.002S")]
                  :leg-type (LegType. (ArrowType$Duration. TimeUnit/MILLISECOND))}
                 (test-projection '*
                                  #(->dur-vec "x" TimeUnit/MILLISECOND 1)
                                  #(->bigint-vec "y" 2))))

        (t/is (= {:res [(Duration/parse "PT10S")]
                  :leg-type (LegType. (ArrowType$Duration. TimeUnit/SECOND))}
                 (test-projection '*
                                  #(->bigint-vec "x" 2)
                                  #(->dur-vec "y" TimeUnit/SECOND 5))))

        (t/is (= {:res [(Duration/parse "PT0.000012S")]
                  :leg-type LegType/DURATIONMICRO}
                 (test-projection '*
                                  #(->float8-vec "x" 2.4)
                                  #(->dur-vec "y" TimeUnit/MICROSECOND 5))))

        (t/is (= {:res [(Duration/parse "PT3S")]
                  :leg-type (LegType. (ArrowType$Duration. TimeUnit/SECOND))}
                 (test-projection '/
                                  #(->dur-vec "x" TimeUnit/SECOND 10)
                                  #(->bigint-vec "y" 3))))))))

(t/deftest test-struct-literals
  (with-open [rel (open-rel [(tu/->mono-vec "x" types/float8-type [1.2 3.4])
                             (tu/->mono-vec "y" types/float8-type [3.4 8.25])])]
    (t/is (= {:res [{:x 1.2, :y 3.4}
                    {:x 3.4, :y 8.25}]
              :leg-type (LegType/structOfKeys #{"x" "y"})
              :nullable? false}
             (run-projection rel '{:x x, :y y})))

    (t/is (= {:res [3.4 8.25]
              :leg-type #{LegType/FLOAT8}
              :nullable? false}
             (run-projection rel '(. {:x x, :y y} y))))

    (t/is (= {:res [nil nil]
              :leg-type #{LegType/NULL}
              :nullable? false}
             (run-projection rel '(. {:x x, :y y} z))))))

(t/deftest test-nested-structs
  (with-open [rel (open-rel [(tu/->mono-vec "y" types/float8-type [1.2 3.4])])]
    (t/is (= {:res [{:x {:y 1.2}}
                    {:x {:y 3.4}}]
              :leg-type (LegType/structOfKeys #{"x"})
              :nullable? false}
             (run-projection rel '{:x {:y y}})))

    (t/is (= {:res [{:y 1.2} {:y 3.4}]
              :leg-type #{(LegType/structOfKeys #{"y"})}
              :nullable? false}
             (run-projection rel '(. {:x {:y y}} x))))

    (t/is (= {:res [1.2 3.4]
              :leg-type #{LegType/FLOAT8}
              :nullable? false}
             (run-projection rel '(.. {:x {:y y}} x y))))))

(t/deftest test-lists
  (t/testing "simple lists"
    (with-open [rel (open-rel [(tu/->mono-vec "x" types/float8-type [1.2 3.4])
                               (tu/->mono-vec "y" types/float8-type [3.4 8.25])])]
      (t/is (= {:res [[1.2 3.4 10.0]
                      [3.4 8.25 10.0]]
                :leg-type (LegType. (ArrowType$FixedSizeList. 3))
                :nullable? false}
               (run-projection rel '[x y 10.0])))

      (t/is (= {:res [[1.2 3.4 nil] [3.4 8.25 nil]]
                :leg-type (LegType. (ArrowType$FixedSizeList. 3))
                :nullable? false}
               (run-projection rel '[(nth [x y] 0)
                                     (nth [x y] 1)
                                     (nth [x y] 2)])))))

  (t/testing "might not be lists"
    (with-open [rel (open-rel [(tu/->duv "x"
                                         [12.0
                                          [1 2 3]
                                          [4 5]
                                          "foo"])])]
      (t/is (= {:res [nil 3 nil nil]
                :leg-type #{LegType/BIGINT LegType/NULL}
                :nullable? false}
               (run-projection rel '(nth x 2)))))))

(t/deftest test-mixing-prims-with-non-prims
  (with-open [rel (open-rel [(tu/->mono-vec "x" types/struct-type
                                            [{:a 42, :b 8}
                                             {:a 12, :b 5}])])]
    (t/is (= {:res [{:a 42, :b 8, :sum 50}
                    {:a 12, :b 5, :sum 17}]
              :leg-type (LegType/structOfKeys #{"a", "b", "sum"})
              :nullable? false}
             (run-projection rel '{:a (. x a)
                                   :b (. x b)
                                   :sum (+ (. x a) (. x b))})))))

(t/deftest test-multiple-struct-legs
  (with-open [rel (open-rel [(tu/->duv "x"
                                       [{:a 42}
                                        {:a 12, :b 5}
                                        {:b 10}
                                        {:a 15, :b 25}
                                        10.0])])]
    (t/is (= {:res [{:a 42}
                    {:a 12, :b 5}
                    {:b 10}
                    {:a 15, :b 25}
                    10.0]
              :leg-type #{(LegType/structOfKeys #{"a"})
                          (LegType/structOfKeys #{"a" "b"})
                          (LegType/structOfKeys #{"b"})
                          LegType/FLOAT8}
              :nullable? false}
             (run-projection rel 'x)))

    (t/is (= {:res [42 12 nil 15 nil]
              ;; TODO: this could be a nullable BigIntVector, rather than a DUV
              :leg-type #{LegType/BIGINT LegType/NULL}
              :nullable? false}
             (run-projection rel '(. x a))))))
