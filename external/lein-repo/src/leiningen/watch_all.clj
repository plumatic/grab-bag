(ns leiningen.watch-all
  "Watch tests in this namespace.  Options are fast/unit/all/unlabeled, default all"
  (:require [leiningen.core.eval  :as eval]
            [lein-repo.plugin :as plugin]))

(defn watch-all [project & [op]]
  (let [mega-project @plugin/test-all-project]
    (eval/eval-in-project (update-in mega-project [:dependencies] conj ['lein-repo plugin/lein-version])
                          `(lein-repo.test/watch-tests
                            ~(vec (plugin/ordered-source-and-test-paths))
                            :skip-system-tests? false #_ true
                            :test-selector ~(keyword (or op "fast")))
                          '(require 'lein-repo.test))))
