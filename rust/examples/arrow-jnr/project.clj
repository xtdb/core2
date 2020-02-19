(defproject arrow-jnr "0.1.0-SNAPSHOT"
  :description "Parts of Crux re-imagined in Rust"
  :url "https://github.com/juxt/crux-rnd"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :main crux.arrow-jnr
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [com.github.jnr/jnr-ffi "2.1.11"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.apache.arrow/arrow-vector "0.16.0"]]
  :java-source-paths ["src"]
  :jvm-opts ["-Xmx2G" "-XX:MaxDirectMemorySize=2G"
             "-Dio.netty.tryReflectionSetAccessible=true"]
  :global-vars {*warn-on-reflection* true})
