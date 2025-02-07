(ns gic.tools.subcommands.irdb.init
  (:require [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [gic.tools.utils.db :as u]))

(defn init-irdb! [db-path]
  (with-open [conn (u/duckdb-connect-rw db-path)]
    (let [create-cube-sql (slurp (io/resource "sql/irdb/init/create-cubes-table.sql"))
          create-allids-sql (slurp (io/resource "sql/irdb/init/create-allids-table.sql"))]
      (run! #(jdbc/execute! conn [%]) (list create-cube-sql create-allids-sql)))))

(defn run [input-opts]
  (let [db-path (get-in input-opts [:opts :dbpath])]
    (log/info (format "Initializing duckdb database: %s" db-path))
    (init-irdb! db-path)
    (log/info "All Done!")))

(comment
  (def test-opts {:dispatch ["init"]
                  :opts {:debug false :dbpath "foo.db"}
                  :args nil
                  :subcommand :init
                  :command :irdb})
  (run test-opts))
