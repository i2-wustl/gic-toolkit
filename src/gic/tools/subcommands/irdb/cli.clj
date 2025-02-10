(ns gic.tools.subcommands.irdb.cli
  (:require [babashka.cli :as cli]
            [clojure.string :as str]
            [gic.tools.utils.fs :as utils]
            [gic.tools.subcommands.irdb.init :as i]
            [gic.tools.subcommands.irdb.add :as a]))

(declare subcommands)
(declare global-options)

(def active-subcommand (atom nil))

(defn get-spec [subcommand]
  (get-in subcommands [(keyword subcommand) :spec]))

(defn get-full-spec [subcommand]
  (let [subcmd-spec (get-spec subcommand)]
    (merge global-options subcmd-spec)))

(defn get-dispatch-fn [subcommand]
  (let [subcmd (keyword subcommand)]
    (get-in subcommands [subcmd :dispatch])))

(defn display-subcommand-options [subcommand]
  (let [subcmd-spec (get-full-spec subcommand)]
      (cli/format-opts {:spec subcmd-spec})))

(defn show-help? [opts]
  (if (get-in opts [:opts :help])
    true
    nil))

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
        valid-subcommand? (contains? subcommands subcommand)]
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

(defn print-subcommand-help [subcommand]
  (let [subcmd-help-string (subcommand-help-string [subcommand])]
    (binding [*out* *err*]
     (println subcmd-help-string))))

(def global-options {:debug {:coerce :boolean
                             :desc   "Enable additional debug logging"
                             :default false}})

(def subcommands 
  {:init {:spec {:dbpath {:ref "/path/to/irdb.db"
                          :desc "The file path to the irdb database [required] "
                          :alias :i
                          :require true
                          :validate {:pred #(-> % utils/file-exists? not)
                                     :ex-msg #(format "[err] irdb file already exists: %s" (:value %))}}}

          :dispatch (fn [opts]
                      (let [full-opts (assoc opts :subcommand :init :command :irdb)]
                        (when (show-help? full-opts)
                          (print-subcommand-help (:subcommand full-opts)))
                        (i/run full-opts)))}

   :add {:spec {:input-parquet {:ref "/path/to/input.parquet"
                                :desc "The input data source to add to the irdb database [required]"
                                :alias :i
                                :require true
                                :validate {:pred #(-> % utils/file-exists?)
                                           :ex-msg #(format "[err] could not find on file system: %s" (:value %))}}

                :target-irdb {:ref "/path/to/irdb.db"
                              :desc "The target irdb database to add data into [required]"
                              :alias :i
                              :require true
                              :validate {:pred #(-> % utils/file-exists?)
                                         :ex-msg #(format "[err] could not find on file system: %s" (:value %))}}

                :concept {:ref "\\concept\\path"
                          :desc "A specific concept path to add into the irdb from the input parquet data source"
                          :alias :c
                          :require false}

                :concepts-list {:ref "/path/to/concepts.list"
                                :desc "A list of concept paths to add into the irdb from the input parquet data source (one concept per line)"
                                :alias :l
                                :require false
                                :validate {:pred #(-> % utils/file-exists?)
                                           :ex-msg #(format "[err] could not find on file system: %s" (:value %))}}} 
         :dispatch (fn [opts]
                     (let [full-opts (assoc opts :subcommand :add :command :irdb)]
                        (when (show-help? full-opts)
                          (print-subcommand-help (:subcommand full-opts)))
                        (a/run full-opts)))}
   :merge {:spec {}
           :dispatch (fn [opts]
                       (let [full-opts (assoc opts :subcommand :merge :command :irdb)]
                        (when (show-help? full-opts)
                          (print-subcommand-help (:subcommand full-opts)))
                        (prn full-opts)))}
   :dump {:spec {}
          :dispatch (fn [opts]
                      (let [full-opts (assoc opts :subcommand :dump :command :irdb)]
                        (when (show-help? full-opts)
                          (print-subcommand-help (:subcommand full-opts)))
                        (prn full-opts)))}
   :help {:spec {}
          :dispatch #'help}})

(defn error-fn [{:keys [spec type cause msg option] :as data}]
  (if (= :org.babashka/cli type)
    (binding [*out* *err*]
      (case cause 
         :require (println (format "[err] Missing required argument: %s\n\nPlease see 'gic-tk irdb help %s' for more information.\n" msg @active-subcommand))
         :validate (println msg)))
    (throw (ex-info msg data)))
  (System/exit 1))

(def dispatch-table
 [{:cmds ["init"]  :fn (get-dispatch-fn "init")  :spec (get-full-spec "init")  :error-fn error-fn}
  {:cmds ["add"]   :fn (get-dispatch-fn "add")   :spec (get-full-spec "add")   :error-fn error-fn}
  {:cmds ["merge"] :fn (get-dispatch-fn "merge") :spec (get-full-spec "merge") :error-fn error-fn}
  {:cmds ["dump"]  :fn (get-dispatch-fn "dump")  :spec (get-full-spec "dump")  :error-fn error-fn}
  {:cmds ["help"]  :fn help}
  {:cmds [] :fn help}])

(defn set-active-subcmd! [args]
  (let [subcommand (keyword (first args))
        valid-subcommand? (contains? subcommands subcommand)]
    (when valid-subcommand?
      (swap! active-subcommand (constantly (name subcommand))))))

(defn main [opts]
  (let [args (:args opts)]
    (set-active-subcmd! args)
    (cli/dispatch dispatch-table args)))

