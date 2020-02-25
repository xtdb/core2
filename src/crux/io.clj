(ns crux.io
  (:require [clojure.java.io :as io])
  (:import java.io.File
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute
           java.util.Comparator))

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

(defn merge-sorted
  ([xs ys]
   (merge-sorted (Comparator/naturalOrder) xs ys))
  ([^Comparator comp xs ys]
   ((fn step [[x & xs* :as xs] [y & ys* :as ys]]
      (cond (nil? x) ys
            (nil? y) xs
            :else
            (lazy-seq
             (if (neg? (.compare comp x y))
               (cons x (step xs* ys))
               (cons y (step xs ys*))))))
    xs ys)))
