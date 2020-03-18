(ns crux.datalog.arrow-test
  (:require [clojure.test :as t]
            [crux.datalog :as d]
            [crux.datalog.arrow :as da])
  (:import org.apache.arrow.memory.RootAllocator))

(declare try-reclaim-memory)

(t/deftest test-memory-consumption
  (binding [d/*tuple-relation-factory* #'da/new-arrow-struct-relation
            da/*allocator* (RootAllocator.)]
    (t/is (zero? (.getAllocatedMemory da/*allocator*)))

    (t/testing "db memory usage"
      (let [edge '[edge(1, 2).
                   edge(2, 3).

                   path(X, Y) :- edge(X, Y).
                   path(X, Z) :- path(X, Y), edge(Y, Z).]
            db (d/execute edge)]
        (t/is (pos? (.getAllocatedMemory da/*allocator*)))
        (d/close-db db)
        (t/is (zero? (.getAllocatedMemory da/*allocator*)))))

    ;; TODO: Fix memory leaks during query.
    (t/testing "query memory usage"
      (let [edge '[edge(1, 2).
                   edge(2, 3).

                   path(X, Y) :- edge(X, Y).
                   path(X, Z) :- path(X, Y), edge(Y, Z).]
            db (d/execute edge)
            db-memory (.getAllocatedMemory da/*allocator*)
            result (set (d/query-by-name db 'path))
            query-memory (.getAllocatedMemory da/*allocator*)]
        (t/is (>= query-memory db-memory))
        (t/is (= #{[1 2] [2 3] [1 3]} result))

        (t/testing "reclaiming memory"
          (try-reclaim-memory)
          (t/is (= db-memory (.getAllocatedMemory da/*allocator*))))

        (t/testing "closing db frees remaining memory"
          (d/close-db db)
          (t/is (zero? (.getAllocatedMemory da/*allocator*))))))

    (t/testing "can cleanly close allocator"
      (.close da/*allocator*))))

(defn try-reclaim-memory []
  (loop [memory (.getAllocatedMemory da/*allocator*)]
    (System/gc)
    (Thread/sleep 10)
    (let [new-memory (.getAllocatedMemory da/*allocator*)]
      (when (< new-memory memory)
        (recur new-memory)))))
