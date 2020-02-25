(ns crux.io
  (:require [clojure.java.io :as io])
  (:import java.io.File
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute
           [java.util ArrayList Comparator Iterator]))

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

(defn merge-sorted-lazy
  ([xs ys]
   (merge-sorted-lazy (Comparator/naturalOrder) xs ys))
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

(defn merge-sorted-eager
  ([xs ys]
   (merge-sorted-eager (Comparator/naturalOrder) xs ys))
  ([^Comparator comp xs ys]
   (cond
     (not (and (instance? Iterable xs)
               (instance? Iterable ys)))
     (merge-sorted-lazy comp xs ys)

     (empty? xs)
     ys

     (empty? ys)
     xs

     :else
     (let [acc (ArrayList.)
           xs-it (.iterator ^Iterable xs)
           ys-it (.iterator ^Iterable ys)
           add-all (fn [e ^Iterator i]
                     (.add acc e)
                     (while (.hasNext i)
                       (.add acc (.next i))))]
       (loop [x (.next xs-it)
              y (.next ys-it)]
         (if (<= (.compare comp x y) 0)
           (do (.add acc x)
               (if (.hasNext xs-it)
                 (recur (.next xs-it) y)
                 (add-all y ys-it)))
           (do (.add acc y)
               (if (.hasNext ys-it)
                 (recur x (.next ys-it))
                 (add-all x xs-it)))))
       acc))))
