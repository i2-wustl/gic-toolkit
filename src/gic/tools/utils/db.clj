(ns gic.tools.utils.db
  (:require [next.jdbc.prepare :as p]
            [next.jdbc.result-set :as rs]
            [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [selmer.parser :as t])
  (:import (java.sql PreparedStatement ResultSet ResultSetMetaData)
           (edu.harvard.hms.dbmi.avillach.hpds.data.phenotype PhenoCube)))

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

(defn duckdb-blob->object [blob-rs]
  (let [size (.length blob-rs)
        buf  (byte-array size)
        bstream (.getBinaryStream blob-rs)
        _ (.read bstream buf 0 size)]
    (deserialize buf)))

(defn render-sql-template [sql-template-path params]
  (let [sql-template (slurp (io/resource sql-template-path))]
    (t/render sql-template params)))
