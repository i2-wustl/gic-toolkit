(ns gic.tools.subcommands.irdb.inspect
  (:require [next.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [clojure.string :as s]
            [gic.tools.utils.db :as u]))

(defn get-concepts-from-list-path [input-path]
  (if-not (nil? input-path)
   (-> input-path
      slurp
      s/split-lines)
   nil))

(defn list-irdb-concepts [irdb-path]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
   (let [sql (str "select concept_path "
                  "from pheno_cubes "
                  "order by concept_path")]
     (mapv :concept_path (jdbc/execute! conn [sql])))))

(defn assemble-concepts [input-concept input-concepts-list-path]
  (let [concepts-list (get-concepts-from-list-path input-concepts-list-path)]
    (cond
      (and (not (nil? input-concept))
           (nil? concepts-list)) (vector input-concept)
      (and (nil? input-concept)
           (not (nil? concepts-list))) concepts-list
      :else (->> (concat (vector input-concept) concepts-list)
                 (apply sorted-set)
                 vec))))

(defn concept-in-irdb? [conn concept]
  (let [sql (str "select 1 as exists from pheno_cubes where concept_path = ?")
        in-irdb? (:exists (jdbc/execute-one! conn [sql concept]))]
    (if in-irdb? true false)))

(defn filter-valid-concepts [input-concepts irdb-path]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
    (filterv #(concept-in-irdb? conn %) input-concepts)))

(defn display-summary [result-set]
  (let [fmt "----------\nconcept_path: %s\nobservation count: %d\nis_alpha: %s\ncolumn width: %d"]
   (run! #(-> (format fmt
                  (:concept_path %)
                  (:observation_count %)
                  (:is_alpha %)
                  (:column_width %))
              (println)) result-set)))

(defn display-selected-concepts [concepts irdb-path]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
   (let [sql (str "select concept_path, is_alpha, observation_count, column_width "
                    "from pheno_cubes "
                    "where concept_path in ( "
                    (s/join "," (mapv (constantly "?") concepts))
                    " ) "
                    "order by observation_count, concept_path")
         result-set (jdbc/plan conn (into [] (concat [sql] concepts)))]
     (display-summary result-set))))

(defn display-selected-concepts-summary [input-concept concept-list-path irdb-path]
  (-> (assemble-concepts input-concept concept-list-path)
      (filter-valid-concepts irdb-path)
      (display-selected-concepts irdb-path)))

(defn display-all-concepts-summary [irdb-path]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
   (let [sql (str "select concept_path, is_alpha, observation_count, column_width "
                  "from pheno_cubes "
                  "order by observation_count, concept_path")
         result-set (jdbc/plan conn [sql])]
     (display-summary result-set))))

(defn display-selected-concepts-data [input-concept concept-list-path irdb-path]
  (-> (assemble-concepts input-concept concept-list-path)
      (filter-valid-concepts irdb-path)
      (display-selected-concepts-data-details irdb-path)))

(defn display-all-concepts-data [irdb-path]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
   (let [sql (str "select concept_path, loading_map "
                  "from pheno_cubes "
                  "order by observation_count, concept_path")
         fmt "----------\nconcept_path: %s\nobservation count: %d\nis_alpha: %s\ncolumn width: %d"
         result-set (jdbc/plan conn [sql])]
     (display-details result-set))))

(defn run [input-opts]
  (let [irdb-path (get-in input-opts [:opts :irdb])
        input-concept (get-in input-opts [:opts :concept])
        concept-list-path (get-in input-opts [:opts :concepts-list])
        show-data? (get-in input-opts [:opts :show-data])
        list-irdb-concepts-only? (get-in input-opts [:opts :display-concepts])]
    (cond
      list-irdb-concepts-only? (list-irdb-concepts irdb-path)
      (and (not show-data?)
           (or input-concept concept-list-path)) (display-selected-concepts-summary input-concept concept-list-path irdb-path)
      (and show-data?
           (or input-concept concept-list-path)) (display-selected-concepts-data input-concept concept-list-path irdb-path)
      (and (not show-data?)
           (not (or input-concept concept-list-path))) (display-all-concepts-summary irdb-path)
      (and show-data?
           (not (or input-concept concept-list-path))) (display-all-concepts-data irdb-path)
      :else (log/info "Don't know how to proceed, exiting out."))
    (comment prn input-opts)
    (println "")
    (log/info "All Done!")))

(comment
  (def test-opts-v1 {:dispatch ["add"]
                     :opts {:debug false
                            :input-parquet "foo.parquet"
                            :concept "\\path\\to\\foo"
                            :concepts-list "concepts.list"}
                     :args nil
                     :subcommand :add
                     :command :irdb}))
