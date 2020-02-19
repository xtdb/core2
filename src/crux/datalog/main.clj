(ns crux.datalog.main
  (:gen-class))

(defn -main [& args]
  (apply (requiring-resolve 'crux.datalog/-main) args))
