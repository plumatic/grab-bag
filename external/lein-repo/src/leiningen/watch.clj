(ns leiningen.watch
  "Watch tests in this namespace.  Options are fast/unit/all/unlabeled, default all"
  (:require [leiningen.core.eval  :as eval]
            [lein-repo.plugin :as plugin]))

(defn watch [project & [op]]
  (eval/eval-in-project (update-in project [:dependencies] conj ['lein-repo plugin/lein-version])
                        `(lein-repo.test/watch-tests
                          ~(vec (concat (:source-paths project) (:test-paths project)))
                          :test-selector ~(keyword (or op "all")))
                        '(require 'lein-repo.test)))
