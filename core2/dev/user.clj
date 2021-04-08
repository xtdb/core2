(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as ctn]))

(doseq [ns '[user core2.temporal.simd]]
  (ctn/disable-reload! (create-ns ns)))

(defn reset []
  (ctn/refresh))

(comment
  (def node (time (core2.core/->local-node (core2.util/->path "target/tpch-queries-sf-001"))))
  (with-open [watermark (core2.core/open-watermark node)]
    (let [^core2.core.Node node node]
      (with-bindings {#'core2.tpch-queries/*node* node
                      #'core2.tpch-queries/*op-factory*
                      (core2.operator/->operator-factory (.allocator node)
                                                         (.metadata-manager node)
                                                         (.temporal-manager node)
                                                         (.buffer-pool node))
                      #'core2.tpch-queries/*watermark* watermark}
        (time
         (doseq [q [#'core2.tpch-queries/tpch-q1-pricing-summary-report
                    #'core2.tpch-queries/tpch-q3-shipping-priority
                    #'core2.tpch-queries/tpch-q5-local-supplier-volume
                    #'core2.tpch-queries/tpch-q6-forecasting-revenue-change
                    #'core2.tpch-queries/tpch-q7-volume-shipping
                    #'core2.tpch-queries/tpch-q8-national-market-share
                    #'core2.tpch-queries/tpch-q9-product-type-profit-measure
                    #'core2.tpch-queries/tpch-q10-returned-item-reporting
                    #'core2.tpch-queries/tpch-q12-shipping-modes-and-order-priority
                    #'core2.tpch-queries/tpch-q14-promotion-effect
                    #'core2.tpch-queries/tpch-q19-discounted-revenue]]
           (prn q)
           (prn (time (count (@q))))))))))
