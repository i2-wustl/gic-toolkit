(ns gic.tools.utils.os)

(defn- die [status msg]
  (println msg)
  (System/exit status))

(defn exit [status msg]
  (cond
    (= (System/getProperty "gic.tools.repl") "true") (throw (ex-info msg {:exit-code status}))
    :else (die status msg)))

(comment
  (exit 1 "kill repl?"))
