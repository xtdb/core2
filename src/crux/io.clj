(ns crux.io
  (:require [clojure.java.io :as io])
  (:import java.io.File
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute
           [java.util Comparator PriorityQueue]))

(def ^:private file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn delete-dir [dir]
  (let [dir (io/file dir)]
    (when (.exists dir)
      (Files/walkFileTree (.toPath dir) file-deletion-visitor))))

(defn create-tmpdir
  (^java.io.File []
   (create-tmpdir ""))
  (^java.io.File [dir-name]
   (.toFile (Files/createTempDirectory dir-name (make-array FileAttribute 0)))))

(defn- new-merge-sort-priority-queue ^PriorityQueue [^Comparator comp sorted-seqs]
  (let [pq-comp (reify Comparator
                  (compare [_ [a] [b]]
                    (.compare comp a b)))]
    (doto (PriorityQueue. (count sorted-seqs) pq-comp)
      (.addAll sorted-seqs))))

(defn- merge-sort-priority-queue->seq [^PriorityQueue pq]
  ((fn step []
     (lazy-seq
      (let [[x & xs] (.poll pq)]
        (when x
          (when xs
            (.add pq xs))
          (cons x (step))))))))

(defn merge-sort [comp sorted-seqs]
  (let [sorted-seqs (remove empty? sorted-seqs)]
    (if (< (count sorted-seqs) 2)
      (first sorted-seqs)
      (->> (new-merge-sort-priority-queue comp sorted-seqs)
           (merge-sort-priority-queue->seq)))))
