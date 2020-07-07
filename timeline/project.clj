(defproject crux-timeline "0.1.0-SNAPSHOT"
  :description "Crux Timeline Index"
  :url "https://github.com/juxt/crux-rnd/timeline"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.6.0"]
                 [org.slf4j/slf4j-api "1.7.29"]
                 [org.clojure/spec.alpha "0.2.176"]
                 [com.google.flatbuffers/flatbuffers-java "1.12.0"]
                 [org.roaringbitmap/RoaringBitmap "0.9.0"]]
  :profiles {:uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [io.airlift.tpch/tpch "0.10"]]}}
  :java-source-paths ["src"]
  :jvm-opts ["-Xmx2G"
             "-XX:MaxDirectMemorySize=2G"]
  :global-vars {*warn-on-reflection* true})
