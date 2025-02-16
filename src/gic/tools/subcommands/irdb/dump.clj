(ns gic.tools.subcommands.irdb.dump
  (:require [clojure.tools.logging :as log]))

(defn run [input-opts]
  (let [src-irdb-path (get-in input-opts [:opts :input-irdb])
        target-javabin-path (get-in input-opts [:opts :target-javabin])]
    (log/info (format "Source irdb: %s" src-irdb-path))
    (log/info (format "Target javabin %s" target-javabin-path))
    (prn input-opts)
    (log/info "All Done!")))
