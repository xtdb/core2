(ns crux.io
  (:require [clojure.java.io :as io])
  (:import java.io.File
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute
           [java.util ArrayList Comparator Iterator]
           java.lang.AutoCloseable))

(defn try-close [c]
  (when (instance? AutoCloseable c)
    (.close ^AutoCloseable c)))

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
             (let [diff (.compare comp x y)]
               (cond
                 (zero? diff)
                 (cons x (step xs* ys*))

                 (neg? diff)
                 (cons x (step xs* ys))

                 (pos? diff)
                 (cons y (step xs ys*)))))))
    xs ys)))

(defn- maybe-next [^Iterator it]
  (when (.hasNext it)
    (.next it)))

(deftype MergeSortedIterator [^Comparator comp
                              ^Iterator xs-it
                              ^Iterator ys-it
                              ^:volatile-mutable x
                              ^:volatile-mutable y]
  Iterator
  (hasNext [this]
    (boolean (or x y)))

  (next [this]
    (cond
      (and x y)
      (let [x* x
            y* y
            diff (.compare comp x y)]
        (cond
          (zero? diff)
          (do (set! (.-y this) (maybe-next ys-it))
              (set! (.-x this) (maybe-next xs-it))
              x*)

          (neg? diff)
          (do (set! (.-x this) (maybe-next xs-it))
              x*)

          (pos? diff)
          (do (set! (.-y this) (maybe-next ys-it))
              y*)))

      x
      (let [x* x]
        (set! (.-x this) (maybe-next xs-it))
        x*)

      y
      (let [y* y]
        (set! (.-y this) (maybe-next ys-it))
        y*))))

(defn merge-sorted-iter
  ([xs ys]
   (merge-sorted-iter (Comparator/naturalOrder) xs ys))
  ([^Comparator comp ^Iterable xs ^Iterable ys]
   (cond
     (empty? xs)
     ys

     (empty? ys)
     xs

     :else
     (let [xs-it (.iterator xs)
           ys-it (.iterator ys)]
       (iterator-seq (->MergeSortedIterator comp xs-it ys-it (maybe-next xs-it) (maybe-next ys-it)))))))
