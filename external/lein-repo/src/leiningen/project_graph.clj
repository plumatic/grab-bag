(ns leiningen.project-graph
  (:require
   [leiningen.core.eval :as eval]
   [lein-repo.plugin :as plugin]))

(defn project-graph
  "Generate dot file for project dependencies"
  [project]
  (let [projects (plugin/ordered-projects @plugin/all-source-project)
        edges (for [p projects
                    c (:internal-dependencies (plugin/read-project-file p))]
                [c p])]
    (eval/eval-in-project
     (plugin/middleware (plugin/read-project-file 'viz))
     `(println (viz.graphviz/dot-file-contents-el
                '~(vec edges)
                '~(into {} (for [p projects] [p p]))))
     `(require 'viz.graphviz))))
