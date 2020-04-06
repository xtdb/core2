(ns crux.datalog.lucene
  (:require [clojure.edn :as edn]
            [crux.datalog :as d])
  (:import [org.apache.lucene.document Document StoredField]
           [org.apache.lucene.index DirectoryReader IndexableField IndexReader IndexWriter IndexWriterConfig Term]
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery BooleanQuery$Builder
            IndexSearcher MatchAllDocsQuery SimpleCollector Query TermQuery]
           [org.apache.lucene.store Directory ByteBuffersDirectory]))

(defn- doc->tuple [^Document doc]
  (vec (for [^IndexableField f doc]
         (or (.stringValue f)
             (.numericValue f)
             (some->> (.binaryValue f) (.utf8ToString) (edn/read-string))))))

(defn- var-bindings->query ^org.apache.lucene.search.Query [var-bindings]
  (.build
   ^BooleanQuery$Builder
   (reduce (fn [^BooleanQuery$Builder builder [idx v]]
             (.add builder
                   (TermQuery. (Term. (str idx) (str v)))
                   BooleanClause$Occur/MUST))
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
        (doseq [[idx v] (map-indexed vector value)]
          (.add doc (case (symbol (.getSimpleName (class v)))
                      int (StoredField. (str idx) ^int v)
                      long (StoredField. (str idx) ^long v)
                      float (StoredField. (str idx) ^float v)
                      double (StoredField. (str idx) ^double v)
                      String (StoredField. (str idx) ^String v)
                      (StoredField. (str idx) (.getBytes (pr-str v))))))
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
    name))

(defn new-lucene-relation
  ([relation-name]
   (new-lucene-relation (ByteBuffersDirectory.) relation-name))
  ([^Directory directory relation-name]
   (->LuceneRelation directory relation-name)))
