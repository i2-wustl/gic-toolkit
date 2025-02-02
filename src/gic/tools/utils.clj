(ns gic.tools.utils
  (:require [next.jdbc.prepare :as p]
            [next.jdbc.result-set :as rs]
            [clojure.java.io :as io]
            [next.jdbc :as jdbc])
  (:import (java.sql PreparedStatement ResultSet ResultSetMetaData)
           (edu.harvard.hms.dbmi.avillach.hpds.data.phenotype PhenoCube)))

(defn- die [status msg]
  (println msg)
  (System/exit status))

(defn exit [status msg]
  (cond
    (= (System/getProperty "gic.tools.repl") "true") (throw (ex-info msg {:exit-code status}))
    :else (die status msg)))

(defn file-exists? [file-path]
  (if (instance? java.io.File file-path)
    (.exists file-path)
    (.exists (io/file file-path))))

(defn dir-exists? [dir-path]
  (if (instance? java.io.File dir-path)
    (.isDirectory dir-path)
    (.isDirectory (io/file dir-path))))

(defn delete-file [file-path]
  (if (instance? java.io.File file-path)
    (io/delete-file (str file-path))
    (io/delete-file file-path)))

(defn duckdb-connect-ro [db-path]
  (let [jdbc-url (format "jdbc:duckdb:%s" db-path)]
    (jdbc/get-connection {:jdbcUrl jdbc-url "duckdb.read_only" true})))

(defn duckdb-connect-rw [db-path]
  (let [jdbc-url (format "jdbc:duckdb:%s" db-path)]
    (jdbc/get-connection {:jdbcUrl jdbc-url "duckdb.read_only" false})))

(defn sqlite-connect-ro [db-path]
  (let [jdbc-url (format "jdbc:sqlite:%s" db-path)]
    (jdbc/get-connection {:jdbcUrl jdbc-url "open_mode" 1})))

(defn sqlite-connect-rw [db-path]
  (let [jdbc-url (format "jdbc:sqlite:%s" db-path)]
    (jdbc/get-connection {:jdbcUrl jdbc-url "open_mode" 6})))

(extend-protocol p/SettableParameter
   PhenoCube
   (set-parameter [^PhenoCube v ^PreparedStatement ps ^long i]
     (let [buff (java.io.ByteArrayOutputStream.)]
       (with-open [dos (java.io.ObjectOutputStream. buff)]
         (.writeObject dos v))
       (.setBytes ps i (.toByteArray buff)))))

(defn sqlite-blob-reader [^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (when (= java.sql.Types/BLOB (.getColumnType rsmeta i))
    (when-let [buf (.getObject rs i)]
      (with-open [dis (-> buf java.io.ByteArrayInputStream. java.io.ObjectInputStream.)]
        (.readObject dis)))))

(def sqlite-blob-builder (rs/as-maps-adapter rs/as-unqualified-lower-maps sqlite-blob-reader))

(defn deserialize [buf]
  (with-open [dis (-> buf java.io.ByteArrayInputStream. java.io.ObjectInputStream.)]
     (.readObject dis)))

(comment
  (exit 1 "kill repl?"))
