(defproject pro.juxt.crux-labs/core2-datasets "<inherited>"
  :description "Core2 Datasets"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../../.."}

  :dependencies [[pro.juxt.crux-labs/core2]
                 [io.airlift.tpch/tpch "0.10"]
                 [org.clojure/data.csv "1.0.0"]
                 [software.amazon.awssdk/s3]]

  :resource-paths ["data"])
