(ns gic.tools.subcommands.irdb.cli
  (:require [babashka.cli :as cli]
            [clojure.string :as str]
            [gic.tools.utils :as utils]))

(def active-subcommand (atom nil))

(def global-options {:debug {:coerce :boolean
                             :desc   "enable additional debug logging"
                             :default false}})

(def init-options-spec {:dbpath {:ref "/path/to/irdb.db"
                                 :desc "The file path to the irdb database"
                                 :alias :i
                                 :require true
                                 :validate {:pred #(-> % utils/file-exists? not)
                                            :ex-msg #(format "[err] irdb file already exists: %s" (:value %))}}})
                                  
(def add-options-spec {})
(def merge-dbs-options-spec {})
(def dump-options-spec {})

(def options-dispatch {:init  init-options-spec
                       :add   add-options-spec
                       :merge merge-dbs-options-spec
                       :dump  dump-options-spec})

(defn merge-options [subcommand-options]
  (merge global-options subcommand-options))

(defn show-subcommand-help [subcommand]
  (let [subcommand-spec (options-dispatch subcommand)
        full-spec (merge-options subcommand-spec)]
    (binding [*out* *err*] 
     (println (cli/format-opts {:spec full-spec})))))

(defn display-subcommand-options [subcommand]
  (let [subcommand-spec (options-dispatch subcommand)
        full-spec (merge-options subcommand-spec)]
    (cli/format-opts {:spec full-spec})))

(defn init [opts]
  (let [full-opts (assoc opts :subcommand :init :command :irdb)]
    (when (get-in full-opts [:opts :help])
      (show-subcommand-help (:subcommand full-opts)))
    (prn full-opts)))

(defn add [opts]
  (let [full-opts (assoc opts :subcommand :add :command :irdb)]
    (when (get-in full-opts [:opts :help])
      (show-subcommand-help (:subcommand full-opts)))
    (prn full-opts)))

(defn merge-dbs [opts]
  (let [full-opts (assoc opts :subcommand :merge :command :irdb)]
    (if (get-in full-opts [:opts :help])
      (show-subcommand-help (:subcommand full-opts)))
    (prn full-opts)))

(defn dump [opts]
  (let [full-opts (assoc opts :subcommand :dump :command :irdb)]
    (if (get-in full-opts [:opts :help])
      (show-subcommand-help (:subcommand full-opts)))
    (prn full-opts)))

(def default-help-string
 (str/trim "
     Usage: gic-tk irdb <subcommand> <options>

     All subcommands support the options:
       --help   subcommand help
       --debug  enable debug logging mode

     Subcommands:

       init     initialize a new irdb
       add      add or update concept data to an existing irdb
       merge    merge multiple target irdbs into a source irdb
       dump     generate a javabin file from an irdb
       help     this help message

     Try:
         gic-tk irdb help <subcommand>

     for more details on a subcommand
   "))

(defn display-subcommand-help-header [subcommand]
  (-> "Usage: gic-tk irdb %s <options> <arguments>"
      (format (name subcommand))
      str/triml))

(defn subcommand-help-string [args]
  (let [subcommand (keyword (first args))
        valid-subcommand? (contains? options-dispatch subcommand)]
    (if valid-subcommand?
      (str/join "\n" 
        [(display-subcommand-help-header subcommand)
         "Options:"
         (display-subcommand-options subcommand)])
      (format "Invalid irdb subcommand: '%s'" (name subcommand)))))

(defn help [opts]
  (binding [*out* *err*]
   (if (empty? (opts :args))
    (println default-help-string)
    (println (subcommand-help-string (opts :args)))))
  (prn opts))

(defn error-fn [{:keys [spec type cause msg option] :as data}]
  (if (= :org.babashka/cli type)
    (binding [*out* *err*]
      (case cause 
         :require (println (format "[err] Missing required argument: %s\n\nPlease see 'gic-tk irdb help %s' for more information.\n" msg @active-subcommand))))
    (throw (ex-info msg data)))
  (System/exit 1))

(def dispatch-table
 [{:cmds ["init"]  :fn init      :spec (merge-options init-options-spec)  :error-fn error-fn}
  {:cmds ["add"]   :fn add       :spec (merge-options add-options-spec)   :error-fn  error-fn}
  {:cmds ["merge"] :fn merge-dbs :spec (merge-options merge-dbs-options-spec)  :error-fn error-fn}
  {:cmds ["dump"]  :fn dump      :spec (merge-options dump-options-spec) :error-fn error-fn}
  {:cmds ["help"]  :fn help}
  {:cmds [] :fn help}])

(defn update-active-subcmd-atom [args]
  (let [subcommand (keyword (first args))
        valid-subcommand? (contains? options-dispatch subcommand)]
    (when valid-subcommand?
      (swap! active-subcommand (constantly (name subcommand))))))

(defn main [opts]
  (let [args (:args opts)]
    (update-active-subcmd-atom args)
    (cli/dispatch dispatch-table args)
    (prn opts)))

