(ns gic.tools.subcommands.irdb.add
  (:require [next.jdbc :as jdbc]
            [next.jdbc.prepare :as p]
            [clojure.tools.logging :as log]
            [clojure.string :as s]
            [gic.tools.utils.db :as u]
            [gic.tools.subcommands.irdb.add :as a])
  (:import (java.util Date)
           (edu.harvard.hms.dbmi.avillach.hpds.data.phenotype PhenoCube PhenoInput)))

(defn get-concepts-from-list-path [input-path]
  (if-not (nil? input-path)
   (-> input-path
      slurp
      s/split-lines)
   nil))

(defn get-all-concepts-in-input-parquet [input-parquet-path]
  (with-open [conn (u/duckdb-connect-ro "")]
   (let [sql-template (str "select concept_path, count(*) as count "
                           "from read_parquet('%s') group by concept_path")
         sql (format sql-template input-parquet-path)]
     (mapv :CONCEPT_PATH (sort-by :count (jdbc/execute! conn [sql]))))))

(defn assemble-concepts [input-concept input-concepts-list-path input-parquet-path]
  (let [ concepts-list (get-concepts-from-list-path input-concepts-list-path)]
    (cond 
      (and (nil? input-concept) 
           (nil? concepts-list)) (get-all-concepts-in-input-parquet input-parquet-path)
      (and (not (nil? input-concept)) 
           (nil? concepts-list)) (vector input-concept)
      (and (nil? input-concept)
           (not (nil? concepts-list))) concepts-list
      :else (->> (concat (vector input-concept) concepts-list)
                 (apply sorted-set)
                 vec))))

(defn concept-input-records-count [concept src-parquet-path]
  (with-open [conn (u/duckdb-connect-ro "")]
   (let [sql-template (str "select count(*) as count "
                           "from read_parquet('%s') "
                           "where concept_path = ?")
         sql (format sql-template src-parquet-path)]
     (jdbc/execute-one! conn [sql concept]))))

(defn peek-concept-record [concept src-parquet-path]
  (with-open [conn (u/duckdb-connect-ro "")]
    (let [sql-template (str "select * "
                            "from read_parquet('%s') "
                            "where concept_path = ? "
                            "LIMIT 1")
          sql (format sql-template src-parquet-path)]
      (jdbc/execute-one! conn [sql concept]))))

(defn create-pheno-input [record-row]
  (let [patient-id (:PATIENT_NUM record-row)
        concept-path (:CONCEPT_PATH record-row)
        tval-char (:TVAL_CHAR record-row)
        nval-num (:NVAL_NUM record-row)
        nval-num-str (if (nil? nval-num) nil (str nval-num))
        timestamp (-> (:TIMESTAMP record-row) long Date.)]
   (doto (PhenoInput.)
     (.setPatientNum patient-id)
     (.setConceptPath concept-path)
     (.setTextValue tval-char)
     (.setNumericValue nval-num-str)
     (.setDateTime timestamp))))

(defn create-pheno-cube [concept parquet-path]
  (let [sample-record (peek-concept-record concept parquet-path)
        pheno-input   (create-pheno-input sample-record)
        class-type    (if (.isAlpha pheno-input) (class "") (class 1.0))]
    (PhenoCube. concept class-type)))

(defn add-record-into-pheno-cube! [pheno-cube [i row-record]]
  (let [pheno-input (create-pheno-input row-record)
        interval 100000]
    (when (= (mod i interval) 0)
      (log/info (format "    # records processed: %d" i)))
    (.addPhenoInput pheno-cube pheno-input)))

(defn insert-concept-into-irdb! [irdb-path concept pheno-cube]
  (with-open [conn (u/duckdb-connect-rw irdb-path)]
    (let [sql (str "insert into cubes (concept_path, cube) values (?, ?) "
                   "on conflict do update set cube = excluded.cube")
          ps   (jdbc/prepare conn [sql])]
     (jdbc/execute-one! (p/set-parameters ps [concept ^PhenoCube pheno-cube])))))

(defn process-concept! [concept parquet-path irdb-path]
  (let [pheno-cube (create-pheno-cube concept parquet-path)
        sql-template (str "select * "
                          "from read_parquet('%s') "
                          "where concept_path = ?")
        sql (format sql-template parquet-path)]
    (with-open [conn (u/duckdb-connect-ro "")] 
      (run! #(a/add-record-into-pheno-cube! pheno-cube %)
            (map-indexed vector (jdbc/plan conn [sql concept]))))
    (log/info (format "    Finished adding records into cube (%d records)" (.numRecords pheno-cube)))
    (log/info "    Inserting pheno-cube into irdb")
    (insert-concept-into-irdb! irdb-path concept pheno-cube)
    (log/info "    Done")))

(defn import-concept-data! [concept parquet-path irdb-path]
  (let [concept-records-count (:count (concept-input-records-count concept parquet-path))]
    (log/info (format "    # of records with concept: %d" (:count (concept-input-records-count concept parquet-path))))
    (if (== concept-records-count 0)
      (log/info (format "    skipping loading"))
      (process-concept! concept parquet-path irdb-path))))

(defn add-concepts-to-irdb! [concepts irdb-path parquet-path]
  (let [total-concepts (count concepts)]
    (doseq [[i concept] (map-indexed vector concepts)]
      (log/info (format "[ %d | %d ] Loading Prior Concept: %s" (+ i 1) total-concepts concept))
      (import-concept-data! concept parquet-path irdb-path)
      (log/info (format "[ %d | %d ] Finished Loading Concept: %s" (+ i 1) total-concepts concept)))))

(defn run [input-opts]
  (let [target-irdb-path (get-in input-opts [:opts :target-irdb])
        input-parquet (get-in input-opts [:opts :input-parquet])
        input-concept (get-in input-opts [:opts :concept])
        concept-list-path (get-in input-opts [:opts :concepts-list])
        final-concepts (assemble-concepts input-concept concept-list-path input-parquet)]
    (log/info (format "Found %d concepts to process" (count final-concepts)))
    (comment add-concepts-to-irdb! final-concepts target-irdb-path input-parquet)
    (comment prn input-opts)
    (log/info "All Done!")))

(comment
  (def test-opts {:dispatch ["add"]
                  :opts {:debug false 
                         :input-parquet "foo.parquet"
                         :concept "\\path\\to\\foo"
                         :concepts-list "concepts.list"}
                  :args nil
                  :subcommand :add
                  :command :irdb})
  (run test-opts)
  (get-concepts-from-list-path "/Users/idas/Downloads/test-concepts.list")
  (get-concepts-from-list-path nil)
  (get-all-concepts-in-input-parquet "/Users/idas/Downloads/age.parquet")
  (concept-input-records-count "\\ACT Demographics\\Age" "/Users/idas/Downloads/age.parquet")
  (str (:NVAL_NUM (peek-concept-record "\\ACT Demographics\\Age" "/Users/idas/Downloads/age.parquet"))))
  
