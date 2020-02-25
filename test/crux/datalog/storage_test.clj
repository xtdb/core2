(ns crux.datalog.storage-test
  (:require [clojure.test :as t]
            [crux.io :as cio]
            [crux.datalog :as d]
            [crux.datalog.hquad-tree :as dhq]
            [crux.datalog.storage :as ds]))

(def ^:dynamic *dir*)
(def ^:dynamic *buffer-pool-factory*)
(declare with-tmp-dir with-each-buffer-pool)
(t/use-fixtures :each #'with-each-buffer-pool #'with-tmp-dir)

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
                 :crux.datalog.hquad-tree/leaf-size 4
                 :buffer-pool-factory *buffer-pool-factory*}]
    (t/testing "in memory"
      (with-open [db (ds/new-arrow-db db-opts)]
        (t/is (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]}
                 (-> db
                     (d/execute triangle-edb)
                     (d/execute triangle-idb)
                     (d/query-by-name 'q)
                     (set))))))

    (t/testing "persistence"
      (with-open [db (ds/new-arrow-db db-opts)]
        (t/testing "IDB is not persisted"
          (t/is (nil? (d/relation-by-name db 'q))))

        (t/testing "EDB is persisted"
          (t/testing "Relations are stored in Z order"
            (t/is (= (sort dhq/z-comparator [[1 3] [1 4] [1 5] [3 5]])
                     (d/query-by-name db 'r)))
            (t/is (= (sort dhq/z-comparator [[1 2] [1 4] [1 5] [1 6] [1 8] [1 9] [3 2]])
                     (d/query-by-name db 't)))
            ;; TODO: is this an issue or not?
            #_(t/is (= (sort dhq/z-comparator [[3 4] [3 5] [4 6] [4 8] [4 9] [5 2]])
                       (d/query-by-name db 's))))

          (t/testing "reasserting IDB"
            (t/is (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]}
                     (-> db
                         (d/execute triangle-idb)
                         (d/query-by-name 'q)
                         (set))))))))))

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
