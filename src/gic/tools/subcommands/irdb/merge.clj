(ns gic.tools.subcommands.irdb.merge
  (:require [clojure.tools.logging :as log]
            [gic.tools.utils.fs :as utils]
            [gic.tools.utils.os :as os]
            [gic.tools.utils.db :as u]))

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

(defn merge-irdb! [main-irdb minor-irdb]
  (let [sql-1 (format "ATTACH '%s' as child (READ_ONLY)" minor-irdb)
        sql-2 (str "INSERT INTO pheno_cubes "
                   "SELECT * from child.pheno_cubes "
                   "ON CONFLICT "
                   "DO UPDATE SET cube = EXCLUDED.cube")
        sql-3 (str "INSERT INTO allids "
                   "SELECT * from child.allids "
                   "ON CONFLICT "
                   "DO UPDATE SET patient_id = EXCLUDED.patient_id")]
    (with-open [conn (u/duckdb-connect-rw main-irdb)]
      (run! #(jdbc/execute-one! conn [%]) [sql-1 sql-2 sql-3]))))

(defn merge-irdbs! [main-irdb minor-irdbs]
  (let [total (count minor-irdbs)]
    (doseq [[i irdb] (map-indexed vector minor-irdbs)]
      (log/info (format "[ %d | %d ] Merging irdb: %s" (+ i 1) total irdb))
      (merge-irdb! main-irdb irdb)
      (log/info (format "[ %d | %d ] Finished merging" (+ i 1) total)))))

(defn run [input-opts]
  (let [main-irdb-path (get-in input-opts [:opts :main-irdb])
        minor-irdb-paths (:args input-opts)]
    (ensure-minor-irdb-args minor-irdb-paths)
    (comment log/info (format "Master irdb: %s" (main-irdb-path)))
    (comment log/info (format "Logging new patient ids into allIds table in irdb"))
    (validate-minor-irdbs minor-irdb-paths)
    (comment merge-irdbs! main-irdb-path minor-irdb-paths)
    (prn input-opts)
    (log/info "All Done!")))
