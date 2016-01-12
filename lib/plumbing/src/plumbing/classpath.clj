(ns plumbing.classpath
  (:require
   [clojure.java.classpath :as classpath]
   [clojure.tools.namespace :as namespace]))

(defn ^java.io.InputStream stream-from-classpath
  "Returns a String for the classpath resource at the given path."
  [path]
  (doto (.getResourceAsStream
         (.getClassLoader
          (class *ns*)) path)
    assert))

(defn read-from-classpath
  "Returns a String for the classpath resource at the given path."
  [path]
  (slurp (stream-from-classpath path)))

(defn ns-path ^java.io.File [ns-name]
  (let [dirs (classpath/classpath-directories)
        ns-dir (first (filter (fn [d] (contains? (set (namespace/find-namespaces-in-dir d))
                                                 (symbol ns-name)))
                              dirs))]
    (if ns-dir
      (.getCanonicalFile ^java.io.File ns-dir)
      nil)))
