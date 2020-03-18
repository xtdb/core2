(ns crux.datalog.storage-test
  (:require [clojure.test :as t]
            [crux.io :as cio]
            [crux.datalog :as d]
            [crux.datalog.arrow :as da]
            [crux.datalog.storage :as ds]
            [crux.datalog.arrow-test])
  (:import org.apache.arrow.memory.RootAllocator))

(def ^:dynamic *dir*)
(def ^:dynamic *buffer-pool-factory*)
(def ^:dynamic *z-index?*)
(declare with-tmp-dir with-each-buffer-pool with-and-without-z-index)
(t/use-fixtures :each #'with-each-buffer-pool #'with-and-without-z-index #'with-tmp-dir)

(t/deftest test-restore-db
  (let [triangle-edb '[r(1, 3).
                       r(1, 4).
                       r(1, 5).
                       r(3, 5).

                       t(1, 2).
                       t(1, 4).
                       t(1, 5).
                       t(1, 6).
                       t(1, 8).
                       t(1, 9).
                       t(3, 2).

                       s(3, 4).
                       s(3, 5).
                       s(4, 6).
                       s(4, 8).
                       s(4, 9).
                       s(5, 2).]
        triangle-idb '[q(A, B, C) :- r(A, B), s(B, C), t(A, C).]
        db-opts {:crux.datalog.storage/root-dir *dir*
                 :crux.datalog.storage/z-index? *z-index?*
                 :crux.datalog.hquad-tree/leaf-size 4
                 :buffer-pool-factory *buffer-pool-factory*}]
    (binding [da/*allocator* (RootAllocator.)]
      (t/testing "in memory"
        (with-open [db (ds/new-arrow-db db-opts)]
          (t/testing "IDB"
            (t/is (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]}
                     (-> db
                         (d/execute triangle-edb)
                         (d/execute triangle-idb)
                         (d/query-by-name 'q)
                         (set)))))

          (t/testing "Relations are stored in Z order"
            (t/is (= (sort ds/z-comparator [[1 3] [1 4] [1 5] [3 5]])
                     (d/query-by-name db 'r)))
            (t/is (= (sort ds/z-comparator [[1 2] [1 4] [1 5] [1 6] [1 8] [1 9] [3 2]])
                     (d/query-by-name db 't)))
            (t/is (= (sort ds/z-comparator [[3 4] [3 5] [4 6] [4 8] [4 9] [5 2]])
                     (d/query-by-name db 's))))))

      (t/testing "persistence"
        (with-open [db (ds/new-arrow-db db-opts)]
          (t/testing "EDB is persisted"
            (t/testing "Relations are stored in Z order"
              (t/is (= (sort ds/z-comparator [[1 3] [1 4] [1 5] [3 5]])
                       (d/query-by-name db 'r)))
              (t/is (= (sort ds/z-comparator [[1 2] [1 4] [1 5] [1 6] [1 8] [1 9] [3 2]])
                       (d/query-by-name db 't)))
              (t/is (= (sort ds/z-comparator [[3 4] [3 5] [4 6] [4 8] [4 9] [5 2]])
                       (d/query-by-name db 's)))))

          (t/testing "IDB is persisted"
            (t/is (d/relation-by-name db 'q))
            (t/is (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]}
                     (-> db
                         (d/query-by-name 'q)
                         (set)))))))

      #_(t/testing "can cleanly close allocator"
          (crux.datalog.arrow-test/try-reclaim-memory)
          (.close da/*allocator*)))))

(defn with-tmp-dir [f]
  (binding [*dir* (cio/create-tmpdir (str *ns*))]
    (try
      (f)
      (finally
        (cio/delete-dir *dir*)))))

(defn with-each-buffer-pool [f]
  (doseq [factory [#'ds/new-in-memory-buffer-pool-factory
                   #'ds/new-mmap-buffer-pool-factory]]
    (binding [*buffer-pool-factory* factory]
      (t/testing (:name (meta factory))
        (f)))))

(defn- with-and-without-z-index [f]
  (doseq [z-index? [true false]]
    (t/testing (str (if z-index?
                      "with"
                      "without") "-z-index")
      (binding [*z-index?* z-index?]
        (f)))))
