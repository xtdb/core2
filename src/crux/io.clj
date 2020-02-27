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
             (let [diff (.compare comp x y)]
               (cond
                 (zero? diff)
                 (cons x (step xs* ys*))

                 (neg? diff)
                 (cons x (step xs* ys))

                 (pos? diff)
                 (cons y (step xs ys*)))))))
    xs ys)))

(defn merge-sorted-eager
  ([xs ys]
   (merge-sorted-eager (Comparator/naturalOrder) xs ys))
  ([^Comparator comp ^Iterable xs ^Iterable ys]
   (cond
     (empty? xs)
     ys

     (empty? ys)
     xs

     :else
     (let [acc (ArrayList.)
           xs-it (.iterator xs)
           ys-it (.iterator ys)
           add-all (fn [^Iterator i]
                     (while (.hasNext i)
                       (.add acc (.next i))))]
       (loop [x (.next xs-it)
              y (.next ys-it)]
         (let [diff (.compare comp x y)]
           (cond
             (zero? diff)
             (do (.add acc x)
                 (cond
                   (and (.hasNext xs-it)
                        (.hasNext ys-it))
                   (recur (.next xs-it) (.next ys-it))

                   (.hasNext xs-it)
                   (add-all xs-it)

                   (.hasNext ys-it)
                   (add-all ys-it)))

             (neg? diff)
             (do (.add acc x)
                 (if (.hasNext xs-it)
                   (recur (.next xs-it) y)
                   (do (.add acc y)
                       (add-all ys-it))))

             (pos? diff)
             (do (.add acc y)
                 (if (.hasNext ys-it)
                   (recur x (.next ys-it))
                   (do (.add acc x)
                       (add-all xs-it)))))))
       acc))))

(def ^:dynamic ^{:tag 'long} *merge-chunk-size* 128)

(defn merge-sorted-chunked
  ([xs ys]
   (merge-sorted-chunked (Comparator/naturalOrder) xs ys))
  ([^Comparator comp ^Iterable xs ^Iterable ys]
   (cond
     (empty? xs)
     ys

     (empty? ys)
     xs

     :else
     (let [xs-it (.iterator xs)
           ys-it (.iterator ys)]
       ((fn step [x y ^List acc]
          (if (= (.size acc) *merge-chunk-size*)
            (concat acc (lazy-seq (step x y (ArrayList.))))
            (let [diff (.compare comp x y)]
              (cond
                (zero? diff)
                (do (.add acc x)
                    (cond
                      (and (.hasNext xs-it)
                           (.hasNext ys-it))
                      (recur (.next xs-it) (.next ys-it) acc)

                      (.hasNext xs-it)
                      (concat acc (iterator-seq xs-it))

                      (.hasNext ys-it)
                      (concat acc (iterator-seq ys-it))))

                (neg? diff)
                (do (.add acc x)
                    (if (.hasNext xs-it)
                      (recur (.next xs-it) y acc)
                      (do (.add acc y)
                          (concat acc (iterator-seq ys-it)))))

                (pos? diff)
                (do (.add acc y)
                    (if (.hasNext ys-it)
                      (recur x (.next ys-it) acc)
                      (do (.add acc x)
                          (concat acc (iterator-seq xs-it)))))))))
        (.next xs-it)
        (.next ys-it)
        (ArrayList.))))))
