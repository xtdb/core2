(ns crux.timeline-test
  (:require [clojure.test :as t])
  (:import [io.airlift.tpch TpchColumn TpchColumnType TpchColumnType$Base TpchEntity TpchTable]
           java.util.UUID))

(defn tpch-column->clj [^TpchColumn c ^TpchEntity e]
  (condp = (.getBase (.getType c))
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

(defn tpch-entity->doc [^TpchTable t ^TpchEntity e]
  (persistent!
   (reduce
    (fn [acc ^TpchColumn c]
      (assoc! acc (.getColumnName c) (tpch-column->clj c e)))
    (transient {})
    (.getColumns t))))

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
