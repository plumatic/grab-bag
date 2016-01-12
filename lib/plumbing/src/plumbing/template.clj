(ns plumbing.template
  "Utility function to interpolate variables into template files."
  (:use plumbing.core)
  (:require
   [clojure.java.io :as java-io]
   [clojure.string :as str]))

(defn interpolate-into-string [s m]
  (loop [s s [[k v] & more] (seq m)]
    (if (nil? k)
      (if-let [unmatched (re-find #"#\{.+\}" s)]
        (throw ( IllegalArgumentException. (format "Placeholder %s not replaced" unmatched)))
        s)
      (recur (str/replace s (re-pattern (format "\\#\\{%s\\}" (name k))) (str v)) more))))

(defn interpolate
  "interpolate key values from m into a file identified by fname available in the classpath"
  [fname m]
  (-> (java-io/resource fname)
      slurp
      (interpolate-into-string m)))