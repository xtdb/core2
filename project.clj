(defproject crux-datalog "0.1.0-SNAPSHOT"
  :description "Crux Datalog"
  :url "https://github.com/juxt/crux-rnd"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :aot [crux.datalog.main]
  :main crux.datalog.main
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.6.0"]
                 [org.slf4j/slf4j-api "1.7.29"]
                 [org.clojure/spec.alpha "0.2.176"]
                 [org.apache.arrow/arrow-vector "0.17.1"]
                 [org.apache.lucene/lucene-core "8.5.0"]]
  :profiles {:uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :java-source-paths ["src"]
  :jvm-opts ["-Xmx2G"
             "-XX:MaxDirectMemorySize=2G"
             "-Dio.netty.tryReflectionSetAccessible=true"
             "-Darrow.memory.debug.allocator=true"]
  :global-vars {*warn-on-reflection* true})
