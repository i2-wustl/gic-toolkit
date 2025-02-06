(ns gic.tools.subcommands.irdb.init)

(defn run [input-opts]
  (let [db-path (get-in input-opts [:opts :dbpath])]
    (println (format "The duckdb database to initialize is: %s" db-path)))
  (prn input-opts))

(comment
  (def test-opts {:dispatch ["init"]
                  :opts {:debug false :dbpath "foo.db"}
                  :args nil
                  :subcommand :init
                  :command :irdb})
  (run test-opts))
