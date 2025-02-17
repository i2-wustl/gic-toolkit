(ns gic.tools.subcommands.irdb.dump
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [gic.tools.utils.fs :as fs]
            [gic.tools.utils.db :as u]
            [clojure.string :as s]
            [next.jdbc :as jdbc])
  (:import (edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype SequentialLoader)
           (edu.harvard.hms.dbmi.avillach.hpds.crypto Crypto)))

(defn load-encryption-key [keyfile]
  (Crypto/loadKey "DEFAULT" keyfile))

(defn cleanup-store [file-path]
  (when (fs/file-exists? file-path)
      (log/info (format "Deleting existing -- %s" file-path))
      (fs/delete-file file-path)))

(defn ensure-output-store-directory [store-dir]
  (let [io-store-dir (io/file store-dir)]
    (when (not (fs/dir-exists? io-store-dir))
        (log/info (format "Creating output directory: %s" store-dir))
        (.mkdirs io-store-dir))))

(defn copy-encrypt-file-into-store-dir [store-dir encrypt-key-file]
  (let [local-encrypt-file (io/file store-dir "encryption_key")]
     (log/info (format "Copying %s into %s" encrypt-key-file store-dir))
     (io/copy (io/file encrypt-key-file) local-encrypt-file)))

(defn setup-output-store-directory [store-dir encrypt-key-path]
  (let [all-obs-file       (io/file store-dir "allObservationsStore.javabin")
        col-meta-file      (io/file store-dir "columnMeta.javabin")
        local-encrypt-file (io/file store-dir "encryption_key")]
      (ensure-output-store-directory store-dir)
      (run! #(cleanup-store %) [all-obs-file col-meta-file local-encrypt-file])
      (copy-encrypt-file-into-store-dir store-dir encrypt-key-path)))

(defn save-store [loader]
  (-> loader
      .getLoadingStore
      .saveStore))

(defn dump-stats [loader]
  (-> loader
      .getLoadingStore
      .dumpStats))

(defn load-cube [total loader [i row-record]]
  (let [concept (:concept row-record)
        pheno-cube (u/duckdb-blob->object (:cube row-record))
        observations (vec (.sortedByKey pheno-cube))]
    (log/info (format "[ %d | %d ] Loading concept: %s" i total concept))
    (.setLoadingMap pheno-cube observations)
    (-> loader
        (.getLoadingStore)
        (.loadingCache)
        (.put concept pheno-cube))
    (log/info (format "[ %d | %d ] Finished loading concept: %s" i total concept))))

(defn load-cubes-from-irdb [loader irdb-path]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
    (let [total (:total (jdbc/execute-one! conn ["select count(*) as total from pheno_cubes"]))
          sql "select concept_path as concept, cube from pheno_cubes order by concept_path"
          counter (atom 1)]
        (run! #(do (->> (vector @counter %)
                        (load-cube total loader))
                   (swap! counter inc)) (jdbc/plan conn [sql])))))

(defn create-javabins [target-dir irdb-path]
  (let [loader (SequentialLoader. target-dir 1000000)
        local-encrypt-file (format "%s/encryption_key" target-dir)]
    (load-encryption-key local-encrypt-file)
    (log/info "Loading concept cubes from irdb")
    (load-cubes-from-irdb loader irdb-path)
    (log/info "Saving the new store")
    (save-store loader)
    (log/info "Dumping store stats")
    (dump-stats loader)))

(defn run [input-opts]
  (let [src-irdb-path (get-in input-opts [:opts :input-irdb])
        target-dir-path (get-in input-opts [:opts :target-dir])
        encrypt-file-path (get-in input-opts [:opts :encryption-file])]
    (log/info (format "Source irdb: %s" src-irdb-path))
    (log/info (format "Target javabin directory: %s" target-dir-path))
    (log/info (format "Encryption file: %s" encrypt-file-path))
    (setup-output-store-directory target-dir-path encrypt-file-path)
    (create-javabins target-dir-path src-irdb-path)
    (log/info "All Done!")
    (prn input-opts)))

(comment
  (def test-root-dir "/Users/idas/git/i2/pic-sure-extras")
  (def test-irdb (s/join "/" [test-root-dir "data/duckdb/test-merge.duckdb"]))
  (def test-target-dir (s/join "/" [test-root-dir "data/test-javabin"]))
  (def test-encrypt-file (s/join "/" [test-root-dir "data/encryption_key"]))
  (def test-sql "select concept_path as concept, cube from pheno_cubes order by concept_path LIMIT 1")
  (def record (with-open [conn (u/duckdb-connect-ro test-irdb)]
                 (let [ds-opts (jdbc/with-options conn {:builderfn u/sqlite-blob-builder})
                       row (jdbc/execute-one! ds-opts [test-sql])]
                   row)))
  (:concept record)
  (.getBytes (:cube record))
  (with-open [conn (u/duckdb-connect-ro test-irdb)]
    (let [ds-opts (jdbc/with-options conn {:builderfn u/sqlite-blob-builder})
          row (jdbc/execute-one! ds-opts [test-sql])
          raw-cube (:cube row)
          size (.length raw-cube)
          buffit (byte-array size)]
      (.read (.getBinaryStream raw-cube) buffit 0 size)
      (u/deserialize buffit))))
