(ns gic.tools.subcommands.irdb.add
  (:require [next.jdbc :as jdbc]
            [next.jdbc.prepare :as p]
            [clojure.tools.logging :as log]
            [clojure.string :as s]
            [gic.tools.utils.db :as u])
  (:import (java.util Date)
           (java.io File)
           (edu.harvard.hms.dbmi.avillach.hpds.data.phenotype PhenoCube PhenoInput)))

(def all-patient-ids (atom (sorted-set)))

(defn get-concepts-from-list-path [input-path]
  (if-not (nil? input-path)
   (-> input-path
      slurp
      s/split-lines)
   nil))

(defn get-all-concepts-in-input-parquet [input-parquet-path]
  (with-open [conn (u/duckdb-connect-rw "")]
   (let [sql-template (str "select concept_path, count(*) as count "
                           "from read_parquet('%s') group by concept_path")
         sql (format sql-template input-parquet-path)]
     (mapv :CONCEPT_PATH (sort-by :count (jdbc/execute! conn [sql]))))))

(defn assemble-concepts [input-concept input-concepts-list-path input-parquet-path]
  (let [concepts-list (get-concepts-from-list-path input-concepts-list-path)]
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
  (with-open [conn (u/duckdb-connect-rw "")]
   (let [sql-template (str "select count(*) as count "
                           "from read_parquet('%s') "
                           "where concept_path = ?")
         sql (format sql-template src-parquet-path)]
     (jdbc/execute-one! conn [sql concept]))))

(defn peek-concept-record [concept src-parquet-path]
  (with-open [conn (u/duckdb-connect-rw "")]
    (let [sql-template (str "select * "
                            "from read_parquet('%s') "
                            "where concept_path = ? "
                            "LIMIT 1")
          sql (format sql-template src-parquet-path)]
      (jdbc/execute-one! conn [sql concept]))))

(defn sanitize-concept [concept-path]
  (let [pheno-input (doto (PhenoInput.)
                       (.setConceptPath concept-path))]
    (.sanitizeConceptPath pheno-input)))

(defn create-pheno-input [record-row]
  (let [patient-id (:PATIENT_NUM record-row)
        concept-path (:CONCEPT_PATH record-row)
        tval-char (:TVAL_CHAR record-row)
        nval-num (:NVAL_NUM record-row)
        nval-num-str (if (nil? nval-num) nil (str nval-num))
        timestamp (:TIMESTAMP record-row)]
   (doto (PhenoInput.)
     (.setPatientNum patient-id)
     (.setConceptPath concept-path)
     (.setTextValue tval-char)
     (.setNumericValue nval-num-str)
     (.setDateTime timestamp))))

(defn create-pheno-cube [concept parquet-path]
  (let [sample-record (peek-concept-record concept parquet-path)
        pheno-input   (create-pheno-input sample-record)
        class-type    (if (.isAlpha pheno-input) (class "") (class 1.0))
        sanitized-concept (sanitize-concept concept)]
    (PhenoCube. sanitized-concept class-type)))

(defn add-record-into-pheno-cube! [pheno-cube [i row-record]]
  (let [pheno-input (create-pheno-input row-record)
        interval 100000]
    (when (= (mod i interval) 0)
      (log/info (format "    # records processed: %d" i)))
    (.addPhenoInput pheno-cube pheno-input)))

;(defn insert-cube-into-irdb! [irdb-path pheno-cube]
;  (with-open [conn (u/duckdb-connect-rw irdb-path)]
;    (let [concept (.name pheno-cube)
;          sql (str "insert into pheno_cubes (concept_path, cube) values (?, ?) "
;                   "on conflict do update set cube = excluded.cube")
;          ps   (jdbc/prepare conn [sql])]
;      (log/info (format "irdb-path: %s" irdb-path))
;      (log/info (format "insert SQL: %s" sql))
;      (jdbc/execute-one! (p/set-parameters ps [concept ^PhenoCube pheno-cube])))))

(defn insert-cube-into-irdb! [irdb-path pheno-cube]
  (with-open [conn (u/duckdb-connect-rw irdb-path)]
    (let [concept (.name pheno-cube)
          sql (str "insert into pheno_cubes (concept_path, cube) values (?, ?) "
                   "on conflict do update set cube = excluded.cube")]
      (comment log/info (format "irdb-path: %s" irdb-path))
      (comment log/info (format "insert SQL: %s" sql))
      (jdbc/execute-one! conn [sql concept ^PhenoCube pheno-cube]))))

(defn collect-patient-id [row-record]
  (swap! all-patient-ids conj (:PATIENT_NUM row-record)))

(defn add-patient-ids-to-irdb! [irdb-path]
   (let [all-ids (s/join "\n" @all-patient-ids)
         tmp-file (File/createTempFile "patient-ids" ".csv")
         tmp-file-name (.getPath tmp-file)
         sql-1 (format "CREATE TABLE temp AS SELECT * from read_csv('%s', names = ['id'])" tmp-file-name)
         sql-2 (str "INSERT INTO allids "
                    "SELECT t.id FROM temp t WHERE t.id NOT IN "
                    "(SELECT patient_id from allids)")
         sql-3 "DROP TABLE temp"]
     (log/info (format "\tWriting patient-ids to temp csv file: %s" tmp-file-name))
     (spit tmp-file-name all-ids)
     (log/info "\tUpdating allids table with new patients ids in temp csv file")
     (with-open [conn (u/duckdb-connect-rw irdb-path)]
       (run! #(jdbc/execute-one! conn [%]) [sql-1 sql-2 sql-3]))))

(defn process-concept! [concept parquet-path irdb-path]
  (let [pheno-cube (create-pheno-cube concept parquet-path)
        sql-template (str "select * "
                          "from read_parquet('%s') "
                          "where concept_path = ? "
                          "LIMIT 2")
        sql (format sql-template parquet-path)
        counter (atom 1)]
    (with-open [conn (u/duckdb-connect-rw "")]
      (run! #(do (->> (vector @counter %)
                      (add-record-into-pheno-cube! pheno-cube))
                 (collect-patient-id %)
                 (swap! counter inc))
            (jdbc/plan conn [sql concept])))
    (log/info (format "    Finished adding records into cube (%d records)" (.numRecords pheno-cube)))
    (log/info "    Inserting pheno-cube into irdb")
    (insert-cube-into-irdb! irdb-path pheno-cube)
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
    (add-concepts-to-irdb! final-concepts target-irdb-path input-parquet)
    (log/info (format "Logging new patient ids into allIds table in irdb"))
    (add-patient-ids-to-irdb! target-irdb-path)
    (comment prn input-opts)
    (log/info "All Done!")))

(comment
  (def test-opts-v1 {:dispatch ["add"]
                     :opts {:debug false
                            :input-parquet "foo.parquet"
                            :concept "\\path\\to\\foo"
                            :concepts-list "concepts.list"}
                     :args nil
                     :subcommand :add
                     :command :irdb})
  (run test-opts-v1)

  (def test-input-parquet "/Users/idas/Downloads/age.parquet")
  (def test-input-irdb "/Users/idas/git/i2/pic-sure-extras/data/duckdb/test-irdb.duckdb")
  (def test-opts-v2 {:dispatch ["add"]
                     :opts {:debug false
                            :input-parquet test-input-parquet
                            :target-irdb test-input-irdb
                            :concept nil
                            :concepts-list nil}
                     :args nil
                     :subcommand :add
                     :command :irdb})
  (run test-opts-v2)

  (def test-concepts (assemble-concepts nil nil test-input-parquet))
  (def test-concept (first test-concepts))
  (sanitize-concept test-concept)
  (def test-concept-record (peek-concept-record test-concept test-input-parquet))
  (def test-pheno-input (create-pheno-input test-concept-record))
  (bean test-pheno-input)
  (def test-pheno-cube (create-pheno-cube test-concept test-input-parquet))
  (bean test-pheno-cube)
  (.name test-pheno-cube)
  (import-concept-data! test-concept test-input-parquet test-input-irdb)

  (get-concepts-from-list-path "/Users/idas/Downloads/test-concepts.list")
  (get-concepts-from-list-path nil)
  (get-all-concepts-in-input-parquet "/Users/idas/Downloads/age.parquet")
  (concept-input-records-count "\\ACT Demographics\\Age" "/Users/idas/Downloads/age.parquet")
  (str (:NVAL_NUM (peek-concept-record "\\ACT Demographics\\Age" "/Users/idas/Downloads/age.parquet")))

  ; testing patient ids merging
  @all-patient-ids
  (swap! all-patient-ids empty)
  (swap! all-patient-ids conj 2)
  (swap! all-patient-ids conj 3)
  (swap! all-patient-ids conj 1001)
  (swap! all-patient-ids conj 1002)
  (swap! all-patient-ids conj 2004)
  (add-patient-ids-to-irdb! test-input-irdb))
  
