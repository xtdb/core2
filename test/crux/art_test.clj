(ns crux.art-test
  (:require [crux.art :as art]
            [clojure.java.io :as io]
            [clojure.walk :as w]
            [clojure.test :as t]))

(t/deftest grow-nodes
  (loop [n 0
         tree (art/art-make-tree)]
    (let [key (byte-array [n])
          tree (art/art-insert tree key n)]
      (t/is (= n (art/art-lookup tree key)))
      (t/is (= (condp > n
                 4 "Node4"
                 16 "Node16"
                 48 "Node48"
                 256 "Node256")
               (.getSimpleName (class tree))))
      (when (< n 255)
        (recur (inc n) tree)))))

(defn insert-all [keys]
  (reduce art/art-insert (art/art-make-tree) keys))

(defn lookup-all [tree keys]
  (map (partial art/art-lookup tree) keys))

(t/deftest shared-paths
  (let [keys ["a" "ab" "ac" "aba" "b" "ba"]]
    (t/is (= keys (-> (insert-all keys)
                      (lookup-all keys))))))

(t/deftest non-existent-keys
  (let [keys ["aa" "ab" "aba" "abc" "ad"]]
    (t/is (every? nil?
                  (-> (insert-all keys)
                      (lookup-all ["b" "ac" "aabc" "de" "a"]))))))

(defn read-lines [resource]
  (-> (io/resource resource)
      io/reader
      line-seq))

(t/deftest uuids
  (let [uuids (read-lines "crux/art/uuid.txt")
        tree (insert-all uuids)]
    (t/is (= 100000 (count uuids)))
    (t/is (= uuids (lookup-all tree uuids)))
    (t/is (= "00026bda-e0ea-4cda-8245-522764e9f325" (art/art-minimum tree)))
    (t/is (= "ffffcb46-a92e-4822-82af-a7190f9c1ec5" (art/art-maximum tree)))))

(def object-array-class (class (object-array 0)))

(t/deftest words
  (let [words (read-lines "crux/art/words.txt")
        tree (insert-all words)
        counts (volatile! {})]
    (t/is (= 235886 (count words)))
    (t/is (= words (lookup-all tree words)))

    (t/is (= "A" (art/art-minimum tree)))
    (t/is (= "zythum" (art/art-maximum tree)))

    (w/prewalk #(do (when (record? %)
                      (vswap! counts update (.getSimpleName (class %)) (fnil inc 0)))
                    (cond-> %
                      (instance? object-array-class %) vec))
               tree)

    (t/is (= {"Leaf" 235886
              "Node4" 111616
              "Node16" 12181
              "Node48" 458
              "Node256" 1}
             @counts))))
