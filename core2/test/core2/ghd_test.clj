(ns core2.ghd-test
  (:require [clojure.test :as t]
            [clojure.set :as set]))

;; "A Backtracking-Based Algorithm for Computing Hypertree-Decompositions"
;; https://arxiv.org/abs/cs/0701083

;; Does not implement the backtracking part.

;; Lambda are the edges (relations) and chi the vertices (variables).
(defn- ->htree [lambda chi sub-trees]
  {:lambda lambda
   :chi chi
   :sub-trees sub-trees})

(defn- ->vertice->edges [edge->vertices]
  (reduce
   (fn [acc [k vs]]
     (reduce
      (fn [acc v]
        (update acc v (fnil conj (sorted-set)) k))
      acc
      vs))
   (sorted-map)
   edge->vertices))

(defn- separate [{:keys [edge->vertices] :as h} edges separator]
  (let [edge->vertices (select-keys edge->vertices edges)
        vertice->edges (apply dissoc
                              (->vertice->edges edge->vertices)
                              (mapcat edge->vertices separator))
        edges (set/difference edges separator)]
    (if-let [vertice->edges (not-empty vertice->edges)]
      (let [comp (reduce
                  (fn [acc new-edges]
                    (if (not-empty (set/intersection acc new-edges))
                      (set/union acc new-edges)
                      acc))
                  (vals vertice->edges))]
        (cons comp (separate h edges comp)))
      (when (not-empty edges)
        (list edges)))))

(defn- guess-separator [{:keys [vertice->edges edge->vertices]} k]
  (let [edges (vec (keys edge->vertices))]
    (repeatedly (fn []
                  (->> (repeatedly #(rand-nth edges))
                       (distinct)
                       (take (inc (rand-int k)))
                       (into (sorted-set)))))))

(defn htree->tree-seq [ht]
  (tree-seq map? :sub-trees ht))

(defn htree-decomp-width [ht]
  (->> (htree->tree-seq ht)
       (map (comp count :lambda))
       (reduce max 0)))

(defn htree-width [hts]
  (reduce min (map htree-decomp-width hts)))

(defn ->hgraph [h]
  (let [edge->vertices (->> (for [[relation & vars] h]
                              [relation (vec vars)])
                            (into (sorted-map)))]
    {:edge->vertices edge->vertices
     :vertice->edges (->vertice->edges edge->vertices)}))

(defn k-decomposable
  ([h k]
   (let [h (->hgraph h)
         edges (into (sorted-set) (keys (:edge->vertices h)))]
     (k-decomposable h k edges (sorted-set))))
  ([{:keys [edge->vertices] :as h} k edges old-sep]
   (first (for [separator (guess-separator h k)
                :when (and (set/subset? (set/intersection edges old-sep) separator)
                           (not-empty (set/intersection separator edges)))
                :let [sub-trees (reduce
                                 (fn [sub-trees comp]
                                   (if-let [h-tree (k-decomposable h k comp separator)]
                                     (conj sub-trees h-tree)
                                     (reduced nil)))
                                 #{}
                                 (separate h edges separator))
                      chi (->> (set/union (set/intersection edges old-sep)
                                          (set/intersection separator edges))
                               (map edge->vertices)
                               (reduce into (sorted-set)))]]
            (with-meta (->htree separator chi sub-trees) h)))))


;; NOTE: this is not necessarily correct, but need some test as its a
;; test ns.
(t/deftest can-separate-components
  (let [h (->hgraph '[[:A a b c]
                      [:B d e f]
                      [:C c d g]
                      [:D a f i]
                      [:E g i]
                      [:F b e h]
                      [:G e j]
                      [:H a h j]])]
    (t/is (= [#{ :B :C :D :E :F :G :H}]
             (separate h #{:A :B :C :D :E :F :G :H} #{:A})))

    (t/is (= [#{:C :D :E} #{:F :G :H}]
             (separate h #{:A :B :C :D :E :F :G :H} #{:A :B})))

    (t/is (= [#{:E}]
             (separate h #{:C :D :E} #{:C :D})))

    (t/is (= [#{:F :H}]
             (separate h #{:F :G :H} #{:G})))))
