(ns leiningen.test-all
  (:require [lein-repo.plugin :as plugin]
            [clojure.set :as set]
            [leiningen.core.eval :as eval]
            [leiningen.core.main :as main]
            [leiningen.core.classpath :as classpath]))

(defn test-all [project & [option]]
  (let [mega-project @plugin/test-all-project
        summary (eval/eval-in-project
                 mega-project
                 `(do
                    (plumbing.error/init-logger! :fatal)
                    (let [summary# (lein-repo.test/test-dirs
                                    ~(vec (plugin/ordered-source-and-test-paths))
                                    :test-selector ~(keyword (or option "fast")))]
                      (println summary#)
                      (System/exit (+ (:error summary# 0) (:fail summary# 0)))))
                 '(require 'lein-repo.test))]))
