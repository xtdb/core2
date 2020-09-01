(ns crux.tpch
  (:require [clojure.instant :as i]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import [io.airlift.tpch GenerateUtils TpchColumn TpchColumnType TpchColumnType$Base TpchEntity TpchTable]
           [java.util Comparator Date]
           java.util.function.Function))

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

(defn tpch-column->type [^TpchColumn c]
  (condp identical? (.getBase (.getType c))
    TpchColumnType$Base/IDENTIFIER
    Long
    TpchColumnType$Base/INTEGER
    Long
    TpchColumnType$Base/VARCHAR
    String
    TpchColumnType$Base/DOUBLE
    Double
    TpchColumnType$Base/DATE
    Date))

(defn tpch-column->clj [^TpchColumn c ^TpchEntity e]
  (condp identical? (.getBase (.getType c))
    TpchColumnType$Base/IDENTIFIER
    (.getIdentifier c e)
    TpchColumnType$Base/INTEGER
    (long (.getInteger c e))
    TpchColumnType$Base/VARCHAR
    (.getString c e)
    TpchColumnType$Base/DOUBLE
    (.getDouble c e)
    TpchColumnType$Base/DATE
    (i/read-instant-date (GenerateUtils/formatDate (.getDate c e)))))

(defn tpch-doc->pkey [table doc]
  (select-keys doc (get table->pkey table)))

(defn tpch-table->columns [^TpchTable table]
  (->> (for [^TpchColumn c (.getColumns table)]
         [(keyword (.getColumnName c))
          (tpch-column->type c)])
       (into {})))

(def ^:dynamic *key-fn* (fn [table column]
                          (keyword column)))

(defn tpch-entity->doc [^TpchTable t ^TpchEntity e]
  (persistent!
   (reduce
    (fn [acc ^TpchColumn c]
      (assoc! acc (*key-fn* (.getTableName t)
                            (.getColumnName c)) (tpch-column->clj c e)))
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
   (->> (for [^TpchTable t (TpchTable/getTables)
              :let [table-name (keyword (.getTableName t))]]
          [table-name
           (with-meta (set (for [doc (tpch-table->docs t scale-factor)]
                             doc))
             {:name table-name
              :columns (tpch-table->columns t)})])
        (into {}))))

(defn tpch-dbgen-csv
  ([]
   (tpch-dbgen-csv 0.05))
  ([scale-factor]
   (->> (for [^TpchTable t (TpchTable/getTables)]
          [(.getTableName t)
           (str/join "\n"
                     (cons (str/join "|" (for [^TpchColumn c (.getColumns t)]
                                           (.getColumnName c)))
                           (for [^TpchEntity e (.createGenerator ^TpchTable t scale-factor 1 1)]
                             (-> (.toLine e)
                                 (str/replace #"[|]$" "")))))])
        (into {}))))

(def tpch-schema
  ["CREATE TABLE region (r_regionkey INT NOT NULL,r_name VARCHAR(25) NOT NULL,r_comment VARCHAR(152) NOT NULL)"
   "CREATE TABLE nation (n_nationkey INT NOT NULL,n_name VARCHAR(25) NOT NULL,n_regionkey INT NOT NULL,n_comment VARCHAR(152) NOT NULL)"
   "CREATE TABLE supplier (s_suppkey INT NOT NULL,s_name VARCHAR(25) NOT NULL,s_address VARCHAR(40) NOT NULL,s_nationkey INT NOT NULL,s_phone VARCHAR(15) NOT NULL,s_acctbal DECIMAL(15,2) NOT NULL,s_comment VARCHAR(101) NOT NULL)"
   "CREATE TABLE customer (c_custkey INT NOT NULL,c_name VARCHAR(25) NOT NULL,c_address VARCHAR(40) NOT NULL,c_nationkey INT NOT NULL,c_phone VARCHAR(15) NOT NULL,c_acctbal DECIMAL(15,2) NOT NULL,c_mktsegment VARCHAR(10) NOT NULL,c_comment VARCHAR(117) NOT NULL)"
   "CREATE TABLE part (p_partkey INT NOT NULL,p_name VARCHAR(55) NOT NULL,p_mfgr VARCHAR(25) NOT NULL,p_brand VARCHAR(10) NOT NULL,p_type VARCHAR(25) NOT NULL,p_size INT NOT NULL,p_container VARCHAR(10) NOT NULL,p_retailprice DECIMAL(15,2) NOT NULL,p_comment VARCHAR(23) NOT NULL)"
   "CREATE TABLE partsupp (ps_partkey INT NOT NULL,ps_suppkey INT NOT NULL,ps_availqty INT NOT NULL,ps_supplycost DECIMAL(15,2) NOT NULL,ps_comment VARCHAR(199) NOT NULL)"
   "CREATE TABLE orders (o_orderkey INT NOT NULL,o_custkey INT NOT NULL,o_orderstatus VARCHAR(1) NOT NULL,o_totalprice DECIMAL(15,2) NOT NULL,o_orderdate DATE NOT NULL,o_orderpriority VARCHAR(15) NOT NULL,o_clerk VARCHAR(15) NOT NULL,o_shippriority INT NOT NULL,o_comment VARCHAR(79) NOT NULL)"
   "CREATE TABLE lineitem (l_orderkey INT NOT NULL,l_partkey INT NOT NULL,l_suppkey INT NOT NULL,l_linenumber INT NOT NULL,l_quantity INTEGER NOT NULL,l_extendedprice DECIMAL(15,2) NOT NULL,l_discount DECIMAL(15,2) NOT NULL,l_tax DECIMAL(15,2) NOT NULL,l_returnflag VARCHAR(1) NOT NULL,l_linestatus VARCHAR(1) NOT NULL,l_shipdate DATE NOT NULL,l_commitdate DATE NOT NULL,l_receiptdate DATE NOT NULL,l_shipinstruct VARCHAR(25) NOT NULL,l_shipmode VARCHAR(10) NOT NULL,l_comment VARCHAR(44) NOT NULL)"])

(comment
  (require '[next.jdbc :as jdbc])
  (def db {:dbtype "h2:mem" :dbname "tpch"})
  (def ds (jdbc/get-datasource db))

  (doseq [s crux.tpch/tpch-schema]
    (jdbc/execute! ds [s]))

  (doseq [[k v] (crux.tpch/tpch-dbgen-csv 0.01)
          :let [f (format "target/%s.csv" k)]]
    (spit f v)
    (jdbc/execute! ds [(format "INSERT INTO %s SELECT * FROM CSVREAD('%s', null, 'fieldSeparator=|')" k f)]))

  (jdbc/execute! db [(slurp (clojure.java.io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))]))

;; See https://github.com/cwida/duckdb/tree/master/third_party/dbgen/answers

;; https://db.in.tum.de/teaching/ws2021/queryopt/?lang=en

(defonce db-sf-0_01 (tpch-dbgen 0.01))

(comment
  (defonce db-sf-0_1 (tpch-dbgen 0.1))
  (defonce db-sf-1 (tpch-dbgen 1)))

(comment

  ;; TPC-H as Crux Datalog, extended with aggregates and expressions
  ;; in find and rules.

  ;; 01
  '{:find [l_returnflag
           l_linestatus
           (sum l_quantity)
           (sum l_extendedprice)
           (sum (* l_extendedprice (- 1 l_discount)))
           (sum (* l_extendedprice (- 1 l_discount) (+ 1 l_tax)))
           (avg l_quantity)
           (avg l_extendedprice)
           (avg l_discount)
           (count l)]
    :where [[l :l_shipdate l_shipdate]
            [l :l_quantity l_quantity]
            [l :l_extendedprice l_extendedprice]
            [l :l_discount l_discount]
            [l :l_tax l_tax]
            [l :l_returnflag l_returnflag]
            [l :l_linestatus l_linestatus]
            [(<= l_shipdate #inst "1998-09-02")]]
    :order-by [[l_returnflag :asc]
               [l_linestatus :asc]]}

  ;; 02
  '{:find [s_acctbal
           s_name
           n_name
           p_partkey
           p_mfgr
           s_address
           s_phone
           s_comment]
    :where [[p_partkey :p_size 15]
            [p_partkey :p_type p_type]
            [(re-find #"^.*BRASS$" p_type)]
            [p_partkey :p_mfgr p_mfgr]
            [ps :ps_partkey p_partkey]
            [ps :ps_suppkey s]
            [s :s_acctbal s_acctbal]
            [s :s_name s_name]
            [s :s_address s_address]
            [s :s_phone s_phone]
            [s :s_nationkey n]
            [n :n_regionkey r]
            [n :n_name n_name]
            [r :r_name "EUROPE"]
            [ps :ps_supplycost ps_supplycost]
            (sub-query p_partkey ps_supplycost)]
    :order-by [[s_acctbal :desc]
               [n_name :asc]
               [s_name :asc]
               [p_partkey :asc]]
    :limit 100
    :rules [[(sub-query [p_partkey] (min ps_supplycost))
             [ps :ps_partkey p_partkey]
             [ps :ps_supplycost ps_supplycost]
             [ps :ps_suppkey s]
             [s :s_nationkey n]
             [n :r_regionkey r]
             [r :r_name "EUROPE"]]]}

  ;; 02 Alt. q is a built-in, binding full tuples and like or-join
  ;; defines the required vars. This shows inline version for single
  ;; valued tuple, expands to [(q ...) [ps_supplycost]]
  '{:find [s_acctbal
           s_name
           n_name
           p_partkey
           p_mfgr
           s_address
           s_phone
           s_comment]
    :where [{:ps_partkey {:p_partkey p_partkey
                          :p_size 15
                          :p_type #"^.*BRASS$"
                          :p_mfgr p_mfgr}
             :ps_suppkey {:s_acctbal s_acctbal
                          :s_name s_name
                          :s_address s_address
                          :s_phone s_phone
                          :s_comment s_comment
                          :s_nationkey {:n_name n_name
                                        :n_regionkey {:r_name "EUROPE"}}}
             :ps_supplycost (q [p_partkey]
                               {:find [(min ps_supplycost)]
                                :where {:ps_partkey p_partkey
                                        :ps_supplycost ps_supplycost
                                        :ps_suppkey {:s_nationkey {:n_regionkey {:r_name "EUROPE"}}}}})}]
    :order-by [[s_acctbal :desc]
               [n_name :asc]
               [s_name :asc]
               [p_partkey :asc]]
    :limit 100}

  ;; 03
  '{:find [o
           [(sum (* l_extendedprice (- 1 l_discount))) revenue]
           o_orderdate
           o_shippriority]
    :where [[c :c_mktsegment "BUILDING"]
            [o :o_custkey c]
            [o :o_shippriority o_shippriority]
            [o :o_orderdate o_orderdate]
            [(< o_orderdate #inst "1995-03-15")]
            [l :l_orderkey o]
            [l :l_discount l_discount]
            [l :l_extendedprice l_extendedprice]
            [l :l_shipdate l_shipdate]
            [(> l_shipdate #inst "1995-03-15")]]
    :order-by [[revenue :desc]
               [o_orderdate :asc]]
    :limit 10})
