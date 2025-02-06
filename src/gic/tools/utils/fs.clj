(ns gic.tools.utils.fs
  (:require [clojure.java.io :as io]))

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


