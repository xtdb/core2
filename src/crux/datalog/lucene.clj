(ns crux.datalog.lucene
  (:require [clojure.edn :as edn]
            [crux.byte-keys :as cbk]
            [crux.datalog :as d]
            [crux.datalog.parser :as dp])
  (:import java.lang.AutoCloseable
           java.nio.charset.StandardCharsets
           [org.apache.lucene.document Document BinaryPoint Field$Store StringField StoredField]
           [org.apache.lucene.index DirectoryReader IndexNotFoundException IndexReader IndexWriter IndexWriterConfig Term]
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery BooleanQuery$Builder
            ConstantScoreQuery IndexSearcher MatchAllDocsQuery ScoreDoc ScoreMode Query TermRangeQuery TermQuery]
           [org.apache.lucene.store Directory ByteBuffersDirectory]))

(defn- doc->tuple [^Document doc]
  (some->> (.getBinaryValue doc "_source")
           (.utf8ToString)
           (edn/read-string)))

(defn- var-bindings->query ^org.apache.lucene.search.Query [var-bindings]
  (binding [cbk/*use-var-ints?* false]
    (if (empty? var-bindings)
      (MatchAllDocsQuery.)
      (.build
       ^BooleanQuery$Builder
       (reduce
        (fn [^BooleanQuery$Builder builder [idx v]]
          (if (dp/logic-var? v)
            (if-let [constraints (:constraints (meta v))]
              (reduce
               (fn [^BooleanQuery$Builder builder [op value]]
                 (case op
                   > (.add builder
                           (if (number? value)
                             (let [lower (cbk/inc-unsigned-bytes (cbk/->byte-key value))]
                               (BinaryPoint/newRangeQuery (str idx)
                                                          lower
                                                          (byte-array (alength lower) (byte -1))))
                             (TermRangeQuery/newStringRange (str idx) (str value) (str (unchecked-char (byte -1))) false true))
                           BooleanClause$Occur/MUST)
                   >= (.add builder
                            (if (number? value)
                              (let [lower (cbk/->byte-key value)]
                                (BinaryPoint/newRangeQuery (str idx)
                                                           lower
                                                           (byte-array (alength lower) (byte -1))))
                              (TermRangeQuery/newStringRange (str idx) (str value) (str (unchecked-char (byte -1))) true true))
                            BooleanClause$Occur/MUST)

                   < (.add builder
                           (if (number? value)
                             (let [upper (cbk/dec-unsigned-bytes (cbk/->byte-key value))]
                               (BinaryPoint/newRangeQuery (str idx)
                                                          (byte-array (alength upper) (byte 0))
                                                          upper))
                             (TermRangeQuery/newStringRange (str idx) "" (str value) true false))
                           BooleanClause$Occur/MUST)
                   <= (.add builder
                            (if (number? value)
                              (let [upper (cbk/->byte-key value)]
                                (BinaryPoint/newRangeQuery (str idx)
                                                           (byte-array (alength upper) (byte 0))
                                                           upper))
                              (TermRangeQuery/newStringRange (str idx) "" (str value) true true))
                            BooleanClause$Occur/MUST)
                   = (.add builder
                           (if (number? value)
                             (BinaryPoint/newExactQuery (str idx) (cbk/->byte-key value))
                             (TermQuery. (Term. (str idx) (str value))))
                           BooleanClause$Occur/MUST)
                   != (.add builder
                            (if (number? value)
                              (BinaryPoint/newExactQuery (str idx) (cbk/->byte-key value))
                              (TermQuery. (Term. (str idx) (str value))))
                            BooleanClause$Occur/MUST_NOT)))
               (.add builder (MatchAllDocsQuery.) BooleanClause$Occur/MUST)
               constraints)
              (.add builder (MatchAllDocsQuery.) BooleanClause$Occur/MUST))
            (.add builder
                  (if (number? v)
                    (BinaryPoint/newExactQuery (str idx) (cbk/->byte-key v))
                    (TermQuery. (Term. (str idx) (str v))))
                  BooleanClause$Occur/MUST)))
        (BooleanQuery$Builder.)
        (map-indexed vector var-bindings))))))

(def ^:dynamic ^{:tag 'long} *page-size* 128)

(defn- search [^IndexReader idx-reader ^Query query]
  (let [searcher (IndexSearcher. idx-reader)]
    ((fn step [after]
        (lazy-seq
         (when-let [score-docs (seq (.scoreDocs (if after
                                                  (.searchAfter searcher
                                                                after
                                                                (ConstantScoreQuery. query)
                                                                *page-size*)
                                                  (.search searcher
                                                           (ConstantScoreQuery. query)
                                                           *page-size*))))]
           (concat (for [^ScoreDoc score-doc score-docs]
                     (doc->tuple (.doc searcher (.-doc score-doc))))
                   (step (last score-docs)))))) nil)))

(deftype LuceneRelation [^Directory directory name]
  d/Relation
  (table-scan [this db]
    (try
      (with-open [idx-reader (DirectoryReader/open directory)]
        (vec (search idx-reader (MatchAllDocsQuery.))))
      (catch IndexNotFoundException _)))

  (table-filter [this db var-bindings]
    (try
      (with-open [idx-reader (DirectoryReader/open directory)]
        (let [projection (d/projection var-bindings)
              unify-tuple? (d/contains-duplicate-vars? var-bindings)]
          (vec (for [tuple (cond->> (search idx-reader (var-bindings->query var-bindings))
                             unify-tuple? (filter (partial d/unify var-bindings)))]
                 (d/project-tuple projection tuple)))))
      (catch IndexNotFoundException _)))

  (insert [this value]
    (binding [cbk/*use-var-ints?* false]
      (with-open [idx-writer (IndexWriter. directory (IndexWriterConfig.))]
        (let [doc (Document.)]
          (.add doc (StoredField. "_source" (.getBytes (pr-str value) StandardCharsets/UTF_8)))
          (doseq [[idx v] (map-indexed vector value)]
            (.add doc (if (number? v)
                        (BinaryPoint. (str idx) (into-array [(cbk/->byte-key v)]))
                        (StringField. (str idx) (str v) Field$Store/NO))))
          (.addDocument idx-writer doc))))
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
    (try
      (with-open [dir-reader (DirectoryReader/open directory)]
        (.numDocs dir-reader))
      (catch IndexNotFoundException _
        0)))

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
