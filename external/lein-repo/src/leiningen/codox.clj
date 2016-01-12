(ns leiningen.codox
  (:require
   [leiningen.core.eval :as eval]
   [lein-repo.plugin :as plugin]))

(def ^:private options
  {:name "Grabbag"
   :description "all grabbag"
   :sources (plugin/ordered-source-and-test-paths true)})

(defn codox
  "Generate API documentation from source code."
  [project]
  (eval/eval-in-project
   (-> @plugin/all-source-project
       (update-in [:source-paths] conj (str plugin/repo-root "/external/codox/src"))
       (update-in [:resource-paths] conj (str plugin/repo-root "/external/codox/resources")))
   `(codox.main/generate-docs
     '~options)
   `(require 'codox.main)))
