(ns gic.tools.subcommands.irdb.inspect
  (:require [next.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [clojure.string :as s]
            [gic.tools.utils.db :as u])
  (:import (java.util Date)
           (java.text SimpleDateFormat)))

(defn format-time [date fmt-string]
  (let [formatter (SimpleDateFormat. fmt-string)]
    (.format formatter date)))

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
     (run! #(-> (:concept_path %)
                (println)) (jdbc/execute! conn [sql])))))

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

(defn display-summary-renderer [result-set]
  (let [fmt "----------\nconcept_path: %s\nobservation count: %d\nis_alpha: %s\ncolumn width: %d"]
   (run! #(-> (format fmt
                  (:concept_path %)
                  (:observation_count %)
                  (:is_alpha %)
                  (:column_width %))
              (println)) result-set)))

(defn display-selected-concepts-summary-assembler [concepts irdb-path]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
   (let [sql (str "select concept_path, is_alpha, observation_count, column_width "
                    "from pheno_cubes "
                    "where concept_path in ( "
                    (s/join "," (mapv (constantly "?") concepts))
                    " ) "
                    "order by observation_count, concept_path")
         result-set (jdbc/plan conn (into [] (concat [sql] concepts)))]
     (display-summary-renderer result-set))))

(defn display-selected-concepts-summary [input-concept concept-list-path irdb-path]
  (-> (assemble-concepts input-concept concept-list-path)
      (filter-valid-concepts irdb-path)
      (display-selected-concepts-summary-assembler irdb-path)))

(defn display-all-concepts-summary [irdb-path]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
   (let [sql (str "select concept_path, is_alpha, observation_count, column_width "
                  "from pheno_cubes "
                  "order by observation_count, concept_path")
         result-set (jdbc/plan conn [sql])]
     (display-summary-renderer result-set))))

(defn get-concept-observations [record-row limit]
  (let [concept (:concept_path record-row)
        loading-map (u/duckdb-blob->object (:loading_map record-row))
        observations (if limit (vec (take limit loading-map))
                               loading-map)]
    {:concept concept :observations observations}))

(defn observations-printer [record]
  (let [concept (:concept record)
        observations (:observations record)
        fmt "%s\t%s\t%s\t%s"]
    (run! #(-> (format fmt
                       concept
                       (.getKey %)
                       (-> (.getValue %) str)
                       (-> (.getTimestamp %)
                           (Date.)
                           (format-time "yyyy-MM-dd HH:mm:ss")))
               (println))
          observations)))

(defn display-observations-renderer [result-set limit]
  (let [fmt "%s\t%s\t%s\t%s"]
    (println (format fmt "Concept-Path" "Patient-Id" "Value" "Timestamp"))
    (run! #(-> (get-concept-observations % limit)
               (observations-printer)) result-set)))

(defn display-selected-concepts-data-assembler [concepts irdb-path limit]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
   (let [sql (str "select concept_path, loading_map "
                    "from pheno_cubes "
                    "where concept_path in ( "
                    (s/join "," (mapv (constantly "?") concepts))
                    " ) "
                    "order by observation_count, concept_path ")
         result-set (jdbc/plan conn (into [] (concat [sql] concepts)))]
     (display-observations-renderer result-set limit))))

(defn display-selected-concepts-observations [input-concept concept-list-path irdb-path limit]
  (-> (assemble-concepts input-concept concept-list-path)
      (filter-valid-concepts irdb-path)
      (display-selected-concepts-data-assembler irdb-path limit)))

(defn display-all-concepts-observations [irdb-path limit]
  (with-open [conn (u/duckdb-connect-ro irdb-path)]
   (let [sql (str "select concept_path, loading_map "
                  "from pheno_cubes "
                  "order by observation_count, concept_path ")
         result-set (jdbc/plan conn [sql])]
     (display-observations-renderer result-set limit))))

(defn run [input-opts]
  (let [irdb-path (get-in input-opts [:opts :irdb])
        input-concept (get-in input-opts [:opts :concept])
        concept-list-path (get-in input-opts [:opts :concepts-list])
        show-data? (get-in input-opts [:opts :show-data])
        limit (get-in input-opts [:opts :limit])
        list-irdb-concepts-only? (get-in input-opts [:opts :display-concepts])]
    (cond
      list-irdb-concepts-only? (list-irdb-concepts irdb-path)
      (and (not show-data?)
           (or input-concept concept-list-path)) (display-selected-concepts-summary input-concept concept-list-path irdb-path)
      (and show-data?
           (or input-concept concept-list-path)) (display-selected-concepts-observations input-concept concept-list-path irdb-path limit)
      (and (not show-data?)
           (not (or input-concept concept-list-path))) (display-all-concepts-summary irdb-path)
      (and show-data?
           (not (or input-concept concept-list-path))) (display-all-concepts-observations irdb-path limit)
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
