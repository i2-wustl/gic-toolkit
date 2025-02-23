(ns gic.tools.core
  (:require [babashka.cli :as cli]
            [clojure.string :as str]
            [gic.tools.subcommands.irdb.cli :as i])
  (:gen-class))

(def version-number "0.0.1")

(defn help [opts]
  (println (str/trim "
     Usage: gic-tk <subcommand> <options>

     Most subcommands support the options:
       --help   subcommand help
       --debug  enable debug logging mode

     Subcommands:

     irdb: commands to manipulate Intermediate Representation Databases (irdb)
       init     initialize a new irdb
       add      add or update concept data to an existing irdb
       merge    merge multiple target irdbs into a source irdb
       dump     generate a javabin file from an irdb
       inspect  inspect an irdb contents
       help     subcommand help message

     help: this help message
     version: version number information
   "))
  (prn opts))

(defn version [opts]
  (println (format "Version: %s" version-number)))


(defn irdb [opts]
  (i/main (assoc opts :command :irdb)))

(def dispatch-table
  [{:cmds ["irdb"] :fn irdb}
   {:cmds ["help"] :fn help}
   {:cmds ["version"] :fn version}
   {:cmds [] :fn help}])

(defn -main [& args]
  (cli/dispatch dispatch-table args))
