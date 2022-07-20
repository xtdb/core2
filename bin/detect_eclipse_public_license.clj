#!/usr/bin/env bb

(ns detect-eclipse-public-license
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.xml :as xml]
            [clojure.tools.logging :as log]
            [clojure.java.shell]
            [babashka.fs :as fs]))

(defn root-file-path [file]
  (fs/normalize (fs/path (fs/parent *file*) ".." file)))

(defn get-xml []
  (let [path (root-file-path "THIRD_PARTY_NOTICES.xml")
        file (io/file (str path))
        stream (io/input-stream file)]
    (xml/parse stream)))

(defn get-artifacts [x]
  (->> (:content x)
       (filter #(= (:tag %) :artifacts))
       first
       :content
       vec
       (filter #(= (:tag %) :artifact))))

(defn epl? [a]
  ;; this is the point where a person really wishes he'd bothered to grab a zipper? -sd
  ;; (first (:content (first (filter #(= (:tag %) :license) (:content (first artifacts))))))
  (some->> a :content
           (filter #(= (:tag %) :license))
           first
           :content
           first
           (re-matches #".*Eclipse.*")))

(defn libbify [s]
  (let [parts (clojure.string.split s #":")]
    (str (first parts) "/" (second parts))))

(defn get-epl-libs [as]
  (->> as
       (map (fn [a] (-> a :attrs :id)))
       (map libbify)))

(defn ->grep [s]
  (re-pattern (str "(?s).*"
                   (-> s
                       (clojure.string.replace "/" "\\/")
                       (clojure.string.replace "." "\\."))
                   ".*")))

(defn get-missing-exceptions [libs]
  (let [license (slurp (str (root-file-path "LICENSE")))]
    #_(println "full license:\n" license "\n########")
    (println (first libs) "matches" (->grep (first libs)))
    (println (re-matches (->grep (first libs)) license))

    (println (second libs) "matches" (->grep (second libs)))
    (println (re-matches (->grep (second libs)) license))

    (remove #(re-matches (->grep %) license) libs)))

(defn print-license-list [l]
  (doseq [e l] (println (str "* " e))))

(def ERROR 1)

(defn main [& args]
  (let [x (get-xml)
        artifacts (get-artifacts x)
        epl-artifacts (filter epl? artifacts)
        epl-libs (get-epl-libs epl-artifacts)
        missing-exceptions (get-missing-exceptions epl-libs)]
    (when (seq epl-libs)
      (println "\nLibraries under Eclipse Public License, which require AGPLv3 exceptions:")
      (print-license-list epl-libs))
    (when (seq missing-exceptions)
      (println "\nLICENSE is missing the follow exceptions: ")
      (print-license-list missing-exceptions)
      (System/exit ERROR))))

(defn -main [& args]
  (main args))

(main)
