(ns crux.datalog.lucene
  (:require [clojure.edn :as edn]
            [crux.datalog :as d]
            [crux.datalog.parser :as dp])
  (:import java.lang.AutoCloseable
           [org.apache.lucene.document Document BinaryPoint DoublePoint Field$Store FloatPoint IntPoint LongPoint StringField StoredField]
           [org.apache.lucene.index DirectoryReader IndexableField IndexReader IndexWriter IndexWriterConfig Term]
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery BooleanQuery$Builder
            IndexSearcher MatchAllDocsQuery SimpleCollector Query TermQuery]
           [org.apache.lucene.store Directory ByteBuffersDirectory]))

(defn- doc->tuple [^Document doc]
  (some->> (.getBinaryValue doc "_stored")
           (.utf8ToString)
           (edn/read-string)))

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
               < (.add builder (LongPoint/newRangeQuery (str idx) Long/MIN_VALUE (dec (long value))))
               <= (.add builder (LongPoint/newRangeQuery (str idx) Long/MIN_VALUE (long value)))
               > (.add builder (LongPoint/newRangeQuery (str idx) (inc (long value)) Long/MAX_VALUE))
               >= (.add builder (LongPoint/newRangeQuery (str idx) (long value) Long/MAX_VALUE))
               = (.add builder (TermQuery. (Term. (str idx) (str value))) BooleanClause$Occur/MUST)
               != (.add builder (TermQuery. (Term. (str idx) (str value))) BooleanClause$Occur/MUST_NOT))
             builder)
           builder
           constraints)
          builder)
        (.add builder
              (TermQuery. (Term. (str idx) (str v)))
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
        (.add doc (StoredField. "_stored" (.getBytes (pr-str value))))
        (doseq [[idx v] (map-indexed vector value)]
          (.add doc (case (symbol (.getSimpleName (class v)))
                      int (IntPoint. (str idx) ^int v)
                      long (LongPoint. (str idx) ^long v)
                      float (FloatPoint. (str idx) ^float v)
                      double (DoublePoint. (str idx) ^double v)
                      String (StringField. (str idx) ^String v Field$Store/NO)
                      (BinaryPoint. (str idx) (into-array [(.getBytes (pr-str v))])))))
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
  ([relation-name]
   (new-lucene-relation (ByteBuffersDirectory.) relation-name))
  ([^Directory directory relation-name]
   (->LuceneRelation directory relation-name)))
