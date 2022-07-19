#!/usr/bin/env bb

(ns detect-eclipse-public-license
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.xml :as xml]
            [clojure.tools.logging :as log]
            [clojure.java.shell]
            [babashka.fs :as fs]))

(defn get-xml []
  (let [path (fs/normalize (fs/path (fs/parent *file*) ".." "THIRD_PARTY_NOTICES.xml"))
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

(defn main [& args]
  (let [x (get-xml)
        artifacts (get-artifacts x)
        epl-artifacts (filter epl? artifacts)
        epl-libs (get-epl-libs epl-artifacts)]
    (println epl-libs)))

(defn -main [& args]
  (main args))

(main)
