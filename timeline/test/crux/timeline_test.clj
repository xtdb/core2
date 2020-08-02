(ns crux.timeline-test
  (:require [clojure.test :as t])
  (:import [io.airlift.tpch TpchColumn TpchColumnType TpchColumnType$Base TpchEntity TpchTable]
           java.util.Date))

(defn uniform-long ^long [^long start ^long end]
  (+ start (long (rand (- (inc end) start)))))

(defn uniform-date ^java.util.Date [^Date start ^Date end]
  (let [start-ms (.getTime start)]
    (Date. (+ start-ms (uniform-long start-ms (.getTime end))))))

(def table->pkey
  {"part" ["p_partkey"]
   "supplier" ["s_suppkey"]
   "partsupp" ["ps_partkey" "ps_suppkey"]
   "customer" ["c_custkey"]
   "lineitem" ["l_orderkey" "l_partkey"]
   "orders" ["o_orderkey"]
   "nation" ["n_nationkey"]
   "region" ["r_regionkey"]})

(defn tpch-column->clj [^TpchColumn c ^TpchEntity e]
  (condp identical? (.getBase (.getType c))
    TpchColumnType$Base/IDENTIFIER
    (.getIdentifier c e)
    TpchColumnType$Base/INTEGER
    (.getInteger c e)
    TpchColumnType$Base/VARCHAR
    (.getString c e)
    TpchColumnType$Base/DOUBLE
    (.getDouble c e)
    TpchColumnType$Base/DATE
    (.getDate c e)))

(defn tpch-doc->pkey [doc]
  (select-keys doc (get table->pkey (get (meta doc) :table))))

(defn tpch-entity->doc [^TpchTable t ^TpchEntity e]
  (let [doc (persistent!
             (reduce
              (fn [acc ^TpchColumn c]
                (assoc! acc (.getColumnName c) (tpch-column->clj c e)))
              (transient {})
              (.getColumns t)))]
    (with-meta doc {:table (.getTableName t)})))

(defn tpch-table->docs [^TpchTable t scale-factor]
  (for [e (.createGenerator ^TpchTable t scale-factor 1 1)]
    (tpch-entity->doc t e)))

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region
;; first happens to be customers (;; 150000)
(defn tpch-dbgen
  ([]
   (tpch-dbgen 0.05))
  ([scale-factor]
   (for [^TpchTable t (TpchTable/getTables)
         doc (tpch-table->docs t scale-factor)]
     doc)))
