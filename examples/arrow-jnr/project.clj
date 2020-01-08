(defproject arrow-jnr "0.1.0-SNAPSHOT"
  :description "Parts of Crux re-imagined in Rust"
  :url "https://github.com/juxt/crux-rnd"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :main crux.arrow-jnr
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [com.github.jnr/jnr-ffi "2.1.9"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.apache.arrow/arrow-vector "0.15.1"]
                 [org.agrona/agrona "1.1.0"]
                 [org.roaringbitmap/RoaringBitmap "0.8.12"]]
  :java-source-paths ["src"]
  :global-vars {*warn-on-reflection* true})
