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

(defn tpch-01 [{:strs [lineitem] :as db}]
  ;; :select-exp
  (let [result (set/select
                #_[:where
                   [:comp-le
                    [:identifier "l_shipdate"]
                    [:numeric-minus
                     [:date-literal "'1998-12-01'"]
                     [:interval-literal "'90'" [:day]]]]]
                (fn [{:strs [l_shipdate]}]
                  (not (pos? (compare l_shipdate #inst "1998-09-02T00:00:00.000-00:00"))))
                #_[:from [:table-spec [:identifier "lineitem"]]]
                lineitem)
        #_[:group-by
           [:identifier "l_returnflag"]
           [:identifier "l_linestatus"]]
        result (->> (group-by (fn [{:strs [l_returnflag l_linestatus]}]
                                [l_returnflag l_linestatus])
                              result)
                    (vals)
                    (remove empty?)
                    ;; :select
                    (into #{} (map (fn [[{:strs [l_returnflag l_linestatus]} :as group]]
                                     {#_[:select-item [:identifier "l_returnflag"]]
                                      "l_returnflag" l_returnflag
                                      #_[:select-item [:identifier "l_linestatus"]]
                                      "l_linestatus" l_linestatus
                                      #_[:select-item
                                         [:set-function-spec [:sum] [:identifier "l_quantity"]]
                                         [:identifier "sum_qty"]]
                                      "sum_qty" (reduce (fn [acc {:strs [l_quantity]}]
                                                          (+ acc l_quantity))
                                                        0 group)
                                      #_[:select-item
                                         [:set-function-spec [:sum] [:identifier "l_extendedprice"]]
                                         [:identifier "sum_base_price"]]
                                      "sum_base_price" (reduce (fn [acc {:strs [l_extendedprice]}]
                                                                 (+ acc l_extendedprice))
                                                               0 group)
                                      #_[:select-item
                                         [:set-function-spec
                                          [:sum]
                                          [:numeric-multiply
                                           [:identifier "l_extendedprice"]
                                           [:numeric-minus
                                            [:numeric-literal "1"]
                                            [:identifier "l_discount"]]]]
                                         [:identifier "sum_disc_price"]]
                                      "sum_disc_price" (reduce (fn [acc {:strs [l_extendedprice l_discount]}]
                                                                 (+ acc (* l_extendedprice
                                                                           (- 1 l_discount))))
                                                               0 group)
                                      #_[:select-item
                                         [:set-function-spec
                                          [:sum]
                                          [:numeric-multiply
                                           [:numeric-multiply
                                            [:identifier "l_extendedprice"]
                                            [:numeric-minus
                                             [:numeric-literal "1"]
                                             [:identifier "l_discount"]]]
                                           [:numeric-plus [:numeric-literal "1"] [:identifier "l_tax"]]]]
                                         [:identifier "sum_charge"]]
                                      "sum_charge" (reduce (fn [acc  {:strs [l_extendedprice l_discount l_tax]}]
                                                             (+ acc (* l_extendedprice
                                                                       (- 1 l_discount)
                                                                       (+ 1 l_tax))))
                                                           0 group)
                                      #_[:select-item
                                         [:set-function-spec [:avg] [:identifier "l_quantity"]]
                                         [:identifier "avg_qty"]]
                                      "avg_qty" (/ (reduce (fn [acc {:strs [l_quantity]}]
                                                             (+ acc l_quantity))
                                                           0 group)
                                                   (count group))
                                      #_[:select-item
                                         [:set-function-spec [:avg] [:identifier "l_extendedprice"]]
                                         [:identifier "avg_price"]]
                                      "avg_price" (/ (reduce (fn [acc {:strs [l_extendedprice]}]
                                                               (+ acc l_extendedprice))
                                                             0 group)
                                                     (count group))
                                      #_[:select-item
                                         [:set-function-spec [:avg] [:identifier "l_discount"]]
                                         [:identifier "avg_disc"]]
                                      "avg_disc" (/ (reduce (fn [acc {:strs [l_discount]}]
                                                              (+ acc l_discount))
                                                            0 group)
                                                    (count group))
                                      #_[:select-item
                                         [:set-function-spec [:count] [:star]]
                                         [:identifier "count_order"]]
                                      "count_order" (count group)}))))
        #_[:order-by
           [:sort-spec [:identifier "l_returnflag"]]
           [:sort-spec [:identifier "l_linestatus"]]]
        result (sort (-> (Comparator/comparing
                          (reify Function
                            (apply [_ {:strs [l_returnflag]}]
                              l_returnflag)))
                         (.thenComparing
                          (Comparator/comparing
                           (reify Function
                             (apply [_ {:strs [l_linestatus]}]
                               l_linestatus)))))
                     result)]
    result))

(defn tpch-02 [{:strs [part supplier partsupp nation region] :as db}]
  ;; :select-exp
  #_[:where
     [:boolean-and
      [:boolean-and
       [:boolean-and
        [:boolean-and
         [:boolean-and
          [:boolean-and
           [:boolean-and
            [:comp-eq p_partkey ps_partkey]
            [:comp-eq s_suppkey ps_suppkey]]
           [:comp-eq p_size 15]]
          [:like-exp p_type #".*BRASS"]]
         [:comp-eq s_nationkey n_nationkey]]
        [:comp-eq n_regionkey r_regionkey]]
       [:comp-eq r_name "EUROPE"]]
      [:comp-eq
       ps_supplycost
       [:select-exp
        [:select
         [:select-item [:set-function-spec [:min] ps_supplycost]]]
        [:from
         [:table-spec partsupp]
         [:table-spec supplier]
         [:table-spec nation]
         [:table-spec region]]
        [:where
         [:boolean-and
          [:boolean-and
           [:boolean-and
            [:boolean-and
             [:comp-eq p_partkey ps_partkey]
             [:comp-eq s_suppkey ps_suppkey]]
            [:comp-eq s_nationkey n_nationkey]]
           [:comp-eq n_regionkey r_regionkey]]
          [:comp-eq r_name "EUROPE"]]]]]]]
  (let [result (-> (set/select (fn [{:strs [r_name]}]
                                 (= r_name "EUROPE"))
                               region)
                   (set/join nation {"r_regionkey" "n_regionkey"})
                   (set/join supplier {"n_nationkey" "s_nationkey"})
                   (set/join partsupp {"s_suppkey" "ps_suppkey"})
                   (set/join (set/select (fn [{:strs [p_size p_type]}]
                                           (and (= p_size 15)
                                                (boolean (re-find #".*BRASS" p_type))))
                                         part)
                             {"ps_partkey" "p_partkey"}))
        result (set/select (fn [{:strs [ps_supplycost p_partkey]}]
                             (= ps_supplycost
                                (->> ((fn [{:strs [region nation supplier partsupp] :as db}]
                                        (let [result (-> (set/select (fn [{:strs [r_name]}]
                                                                       (= r_name "EUROPE"))
                                                                     region)
                                                         (set/join nation {"r_regionkey" "n_regionkey"})
                                                         (set/join supplier {"n_nationkey" "s_nationkey"})
                                                         (set/join (set/select (fn [{:strs [ps_partkey]}]
                                                                                 (= ps_partkey p_partkey))
                                                                               partsupp)
                                                                   {"s_suppkey" "ps_suppkey"}))
                                              result (->> (group-by (fn [{:strs []}]
                                                                      [])
                                                                    result)
                                                          (vals)
                                                          (remove empty?)
                                                          (into #{} (map (fn [[{:strs []} :as group]]
                                                                           {"min_ps_supplycost"
                                                                            (reduce (fn [acc {:strs [ps_supplycost]}]
                                                                                      (min acc ps_supplycost))
                                                                                    Long/MAX_VALUE group)}))))]
                                          result))
                                      db)
                                     (ffirst)
                                     (val))))
                           result)
        #_[:select
           [:select-item s_acctbal]
           [:select-item s_name]
           [:select-item n_name]
           [:select-item p_partkey]
           [:select-item p_mfgr]
           [:select-item s_address]
           [:select-item s_phone]
           [:select-item s_comment]]
        result (->> (set/project result ["s_acctbal"
                                         "s_name"
                                         "n_name"
                                         "p_partkey"
                                         "p_mfgr"
                                         "s_address"
                                         "s_phone"
                                         "s_comment"])
                    (remove empty?))
        #_[:order-by
           [:sort-spec s_acctbal [:desc]]
           [:sort-spec n_name]
           [:sort-spec s_name]
           [:sort-spec p_partkey]]
        result (sort (-> (.reversed
                          (Comparator/comparing
                           (reify Function
                             (apply [_ {:strs [s_acctbal]}]
                               s_acctbal))))
                         (.thenComparing
                          (Comparator/comparing
                           (reify Function
                             (apply [_ {:strs [n_name]}]
                               n_name))))
                         (.thenComparing
                          (Comparator/comparing
                           (reify Function
                             (apply [_ {:strs [s_name]}]
                               s_name))))
                         (.thenComparing
                          (Comparator/comparing
                           (reify Function
                             (apply [_ {:strs [p_partkey]}]
                               p_partkey)))))
                     result)
        #_[:limit 100]
        result (into [] (take 100) result)]
    result))

(defn tpch-03 [{:strs [customer orders lineitem] :as db}]
  (let [#_[:where
           [:boolean-and
            [:boolean-and
             [:boolean-and
              [:boolean-and
               [:comp-eq c_mktsegment "BUILDING"]
               [:comp-eq c_custkey o_custkey]]
              [:comp-eq l_orderkey o_orderkey]]
             [:comp-lt o_orderdate #inst "1995-03-15T00:00:00.000-00:00"]]
            [:comp-gt l_shipdate #inst "1995-03-15T00:00:00.000-00:00"]]]
        result (-> (set/select (fn [{:strs [l_shipdate]}]
                                 (pos? (compare l_shipdate #inst "1995-03-15T00:00:00.000-00:00")))
                               lineitem)
                   (set/join (set/select (fn [{:strs [o_orderdate]}]
                                           (neg? (compare o_orderdate #inst "1995-03-15T00:00:00.000-00:00")))
                                         orders)
                             {"l_orderkey" "o_orderkey"})
                   (set/join (set/select (fn [{:strs [c_mktsegment]}]
                                           (= c_mktsegment "BUILDING"))
                                         customer)
                             {"o_custkey" "c_custkey"}))
        #_[:group-by l_orderkey o_orderdate o_shippriority]
        result (->> (group-by (fn [{:strs [l_orderkey o_orderdate o_shippriority]}]
                                [l_orderkey o_orderdate o_shippriority])
                              result)
                    (vals)
                    (remove empty?)
                    #_[:select
                       [:select-item l_orderkey]
                       [:select-item
                        [:set-function-spec
                         [:sum]
                         [:numeric-multiply
                          l_extendedprice
                          [:numeric-minus 1 l_discount]]]
                        revenue]
                       [:select-item o_orderdate]
                       [:select-item o_shippriority]]
                    (into #{} (map (fn [[{:strs [l_orderkey o_orderdate o_shippriority]} :as group]]
                                     {"l_orderkey" l_orderkey
                                      "o_orderdate" o_orderdate
                                      "o_shippriority" o_shippriority
                                      "revenue" (reduce (fn [acc {:strs [l_extendedprice l_discount]}]
                                                          (+ acc (* l_extendedprice (- 1 l_discount))))
                                                        0 group)}))))
        #_[:order-by [:sort-spec revenue [:desc]] [:sort-spec o_orderdate]]
        result (sort (-> (.reversed
                          (Comparator/comparing
                           (reify Function
                             (apply [_ {:strs [revenue]}]
                               revenue))))
                         (.thenComparing
                          (Comparator/comparing
                           (reify Function
                             (apply [_ {:strs [o_orderdate]}]
                               o_orderdate)))))
                     result)
        #_[:limit 10]
        result (into [] (take 10) result)]
    result))

(defn tpch-04 [{:strs [orders lineitem] :as db}]
  #_ [:where
      [:boolean-and
       [:boolean-and
        [:comp-ge o_orderdate #inst "1993-07-01T00:00:00.000-00:00"]
        [:comp-lt o_orderdate #inst "1993-10-01T00:00:00.000-00:00"]]
       [:exists-exp
        [:select-exp
         [:select [:star]]
         [:from [:table-spec lineitem]]
         [:where
          [:boolean-and
           [:comp-eq l_orderkey o_orderkey]
           [:comp-lt l_commitdate l_receiptdate]]]]]]]
  (let [result (-> (set/select (fn [{:strs [o_orderdate]}]
                                 (and (not (neg? (compare o_orderdate #inst "1993-07-01T00:00:00.000-00:00")))
                                      (neg? (compare o_orderdate #inst "1993-10-01T00:00:00.000-00:00"))))
                               orders))
        result (set/select (fn [{:strs [o_orderkey]}]
                             (->> ((fn [{:strs [lineitem] :as db}]
                                     (let [result (-> (set/select (fn [{:strs [l_orderkey l_commitdate l_receiptdate]}]
                                                                    (and (= l_orderkey o_orderkey)
                                                                         (neg? (compare l_commitdate l_receiptdate))))
                                                                  lineitem))]
                                       result))
                                   db)
                                  (not-empty)
                                  (boolean)))
                           result)

        ;; using join:

        ;; result (set/join result
        ;;                  (-> (set/select (fn [{:strs [l_commitdate l_receiptdate]}]
        ;;                                    (neg? (compare l_commitdate l_receiptdate)))
        ;;                                  lineitem)
        ;;                      (set/project ["l_orderkey"]))
        ;;                  {"o_orderkey" "l_orderkey"})

        #_[:group-by o_orderpriority]
        result (->> (group-by (fn [{:strs [o_orderpriority]}]
                                [o_orderpriority])
                              result)
                    (vals)
                    (remove empty?)
                    #_[:select
                       [:select-item o_orderpriority]
                       [:select-item [:set-function-spec [:count] [:star]] order_count]]
                    (into #{} (map (fn [[{:strs [o_orderpriority]} :as group]]
                                     {"o_orderpriority" o_orderpriority
                                      "order_count" (count group)}))))

        #_[:order-by [:sort-spec o_orderpriority]]
        result (sort (-> (Comparator/comparing
                          (reify Function
                            (apply [_ {:strs [o_orderpriority]}]
                              o_orderpriority))))
                     result)]
    result))
