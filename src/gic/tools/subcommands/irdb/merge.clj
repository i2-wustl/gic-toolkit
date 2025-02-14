(ns gic.tools.subcommands.irdb.merge
  (:require [clojure.tools.logging :as log]
            [gic.tools.utils.fs :as utils]
            [gic.tools.utils.os :as os]))

(defn ensure-minor-irdb-args [minor-irdb-paths]
  (if (empty? minor-irdb-paths)
    (os/exit 1 "[err] Found no minor irdb to subsume")
    (log/info (format "Found %d irdbs to subsume." (count minor-irdb-paths)))))

(defn validate-minor-irdbs [minor-irdb-paths]
  (letfn [(ensure-irdb [irdb-path]
               (when (not (utils/file-exists? irdb-path))
                 (let [msg "[err] Did not find irdb on file system: %s"]
                  (os/exit 1 (format msg irdb-path)))))]
    (run! #(ensure-irdb %) minor-irdb-paths)))

(defn run [input-opts]
  (let [main-irdb-path (get-in input-opts [:opts :main-irdb])
        minor-irdb-paths (:args input-opts)]
    (ensure-minor-irdb-args minor-irdb-paths)
    (comment log/info (format "Master irdb: %s" (main-irdb-path)))
    (comment log/info (format "Logging new patient ids into allIds table in irdb"))
    (validate-minor-irdbs minor-irdb-paths)
    (prn input-opts)
    (log/info "All Done!")))
