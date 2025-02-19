(ns gic.tools.subcommands.irdb.merge
  (:require [next.jdbc :as jdbc]
            [clojure.tools.logging :as log]
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

(defn merge-irdb [main-irdb minor-irdb]
  (let [sql-1 (format "ATTACH '%s' as child (READ_ONLY)" minor-irdb)
        sql-2 (str "INSERT INTO pheno_cubes "
                   "(concept_path, partition, is_alpha, observation_count, column_width, loading_map) "
                   "SELECT concept_path, partition, is_alpha, observation_count, column_width, loading_map "
                   "from child.pheno_cubes "
                   "ON CONFLICT (concept_path) "
                   "DO UPDATE SET "
                   "partition = EXCLUDED.partition, "
                   "is_alpha = EXCLUDED.is_alpha, "
                   "observation_count = EXCLUDED.observation_count, "
                   "column_width = EXCLUDED.column_width, "
                   "loading_map = EXCLUDED.loading_map")
        sql-3 (str "INSERT INTO allids "
                   "SELECT t.patient_id FROM child.allids t "
                   "WHERE t.patient_id NOT IN (SELECT patient_id from allids)")]
    (with-open [conn (u/duckdb-connect-rw main-irdb)]
      (run! #(jdbc/execute-one! conn [%]) [sql-1 sql-2 sql-3]))))

(defn merge-irdbs [main-irdb minor-irdbs]
  (let [total (count minor-irdbs)]
    (doseq [[i irdb] (map-indexed vector minor-irdbs)]
      (log/info (format "[ %d | %d ] Merging irdb: %s" (+ i 1) total irdb))
      (merge-irdb main-irdb irdb)
      (log/info (format "[ %d | %d ] Finished merging" (+ i 1) total)))))

(defn run [input-opts]
  (let [main-irdb-path (get-in input-opts [:opts :main-irdb])
        minor-irdb-paths (:args input-opts)]
    (ensure-minor-irdb-args minor-irdb-paths)
    (log/info (format "Master irdb: %s" main-irdb-path))
    (validate-minor-irdbs minor-irdb-paths)
    (merge-irdbs main-irdb-path minor-irdb-paths)
    (comment prn input-opts)
    (log/info "All Done!")))

;;; testing
;;; clj -M:cli irdb init -i data/duckdb/test-child1.duckdb
;;; clj -M:cli irdb init -i data/duckdb/test-child2.duckdb
;;; clj -M:cli irdb init -i data/duckdb/test-merge.duckdb
;;; clj -M:cli irdb add --input-parquet ~/Downloads/age.parquet --target-irdb data/duckdb/test-child1.duckdb
;;; clj -M:cli irdb add --input-parquet ~/Downloads/race.parquet --target-irdb data/duckdb/test-child2.duckdb
;;; clj -M:cli irdb merge --main-irdb data/duckdb/test-merge.duckdb data/duckdb/test-child1.duckdb data/duckdb/test-child2.duckdb
