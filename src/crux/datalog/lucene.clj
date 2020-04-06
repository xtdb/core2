(ns crux.datalog.lucene
  (:require [clojure.edn :as edn]
            [crux.byte-keys :as cbk]
            [crux.datalog :as d]
            [crux.datalog.parser :as dp])
  (:import java.lang.AutoCloseable
           [org.apache.lucene.document Document BinaryPoint StoredField]
           [org.apache.lucene.index DirectoryReader IndexableField IndexReader IndexWriter IndexWriterConfig]
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery BooleanQuery$Builder
            IndexSearcher MatchAllDocsQuery SimpleCollector Query]
           [org.apache.lucene.store Directory ByteBuffersDirectory]))

(defn- doc->tuple [^Document doc]
  (some->> (.getBinaryValue doc "_source")
           (.utf8ToString)
           (edn/read-string)))

(def ^:private ^"[B" wildcard-min-bytes (byte-array Long/BYTES (byte 0)))
(def ^:private ^"[B" wildcard-max-bytes (byte-array Long/BYTES (byte -1)))


(defn- var-bindings->query ^org.apache.lucene.search.Query [var-bindings]
  (.build
   ^BooleanQuery$Builder
   (reduce
    (fn [^BooleanQuery$Builder builder [idx v]]
      (if (dp/logic-var?)
        (if-let [constraints (:constraints (meta v))]
          (reduce
           (fn [^BooleanQuery$Builder builder [op value]]
             (case op
               > (.add builder
                       (BinaryPoint/newRangeQuery (str idx)
                                                  (if (int? value)
                                                    (cbk/->byte-key (inc ^long value))
                                                    (cbk/inc-unsigned-bytes (cbk/->byte-key value)))
                                                  wildcard-max-bytes)
                       BooleanClause$Occur/MUST)
               >= (.add builder
                        (BinaryPoint/newRangeQuery (str idx)
                                                   (cbk/->byte-key value)
                                                   wildcard-max-bytes)
                        BooleanClause$Occur/MUST)

               < (.add builder
                       (BinaryPoint/newRangeQuery (str idx)
                                                  wildcard-min-bytes
                                                  (if (int? value)
                                                    (cbk/->byte-key (dec ^long value))
                                                    (cbk/dec-unsigned-bytes (cbk/->byte-key value))))
                       BooleanClause$Occur/MUST)
               <= (.add builder
                        (BinaryPoint/newRangeQuery (str idx)
                                                   wildcard-min-bytes
                                                   (cbk/->byte-key value))
                        BooleanClause$Occur/MUST)
               = (.add builder
                       (BinaryPoint/newExactQuery (str idx) (into-array [(cbk/->byte-key v)]))
                       BooleanClause$Occur/MUST)
               != (.add builder
                        (BinaryPoint/newExactQuery (str idx) (into-array [(cbk/->byte-key v)]))
                        BooleanClause$Occur/MUST_NOT))
             builder)
           builder
           constraints)
          builder)
        (.add builder (BinaryPoint/newExactQuery (str idx) (into-array [(cbk/->byte-key v)]))
              BooleanClause$Occur/MUST)))
    (BooleanQuery$Builder.)
    (map-indexed vector var-bindings))))

(defn- search [^IndexReader idx-reader ^Query query]
  (let [searcher (IndexSearcher. idx-reader)
        docs (atom [])]
    (.search searcher query (proxy [SimpleCollector] []
                              (collect [doc]
                                (swap! docs conj (doc->tuple (.doc searcher doc))))))
    @docs))

(deftype LuceneRelation [^Directory directory name]
  d/Relation
  (table-scan [this db]
    (with-open [idx-reader (DirectoryReader/open directory)]
      (search idx-reader (MatchAllDocsQuery.))))

  (table-filter [this db var-bindings]
    (with-open [idx-reader (DirectoryReader/open directory)]
      (search idx-reader (var-bindings->query var-bindings))))

  (insert [this value]
    (with-open [idx-writer (IndexWriter. directory (IndexWriterConfig.))]
      (let [doc (Document.)]
        (.add doc (StoredField. "_source" (.getBytes (pr-str value))))
        (doseq [[idx v] (map-indexed vector value)]
          (.add doc (BinaryPoint. (str idx) (into-array [(cbk/->byte-key v)]))))
        (.addDocument idx-writer doc)))
    this)

  (delete [this value]
    (with-open [idx-writer (IndexWriter. directory (IndexWriterConfig.))]
      (let [query (var-bindings->query value)]
        (.deleteDocuments idx-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query [query]))))
    this)

  (truncate [this]
    (with-open [idx-writer (IndexWriter. directory (IndexWriterConfig.))]
      (.deleteAll idx-writer))
    this)

  (cardinality [this]
    (with-open [dir-reader (DirectoryReader/open directory)]
      (.numDocs dir-reader)))

  (relation-name [this]
    name)

  AutoCloseable
  (close [_]
    (.close directory)))

(defn new-lucene-relation
  (^crux.datalog.lucene.LuceneRelation [relation-name]
   (new-lucene-relation (ByteBuffersDirectory.) relation-name))
  (^crux.datalog.lucene.LuceneRelation [^Directory directory relation-name]
   (->LuceneRelation directory relation-name)))
