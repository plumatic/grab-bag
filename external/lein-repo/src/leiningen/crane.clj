(ns leiningen.crane
  (:require
   [leiningen.core.eval :as eval]
   [clojure.java.shell :as shell]
   [clojure.java.io :as io]
   [leiningen.uberjar :as uberjar])
  (:import (java.util.zip ZipFile ZipOutputStream ZipEntry)
           (java.io File FileOutputStream PrintWriter)))

(defn pre-deploy-cmds [cmds]
  (doseq [cmd-arr cmds]
    (println "Pre-deploy command: " (pr-str cmd-arr))
    (let [res (apply shell/sh cmd-arr)]
      (println (:out res))
      (when-not (zero? (:exit res))
        (throw (RuntimeException. (str "Shell Error: " (pr-str res))))))))

(defn crane
  "use me to deploy a service"
  [project & args]
  (let [uberjar (when (#{"deploy" "redeploy"} (first args))
                  (pre-deploy-cmds (:pre-deploy-cmds project))
                  (uberjar/uberjar project))]
    (eval/eval-in-project
     (if (= (first args) "run")
       project
       (update-in ;; fewer deps = faster.  Include direct source paths to get config.clj.
        (lein-repo.plugin/middleware (lein-repo.plugin/read-project-file 'crane))
        [:source-paths]
        concat (:direct-source-paths project)))
     `(binding [crane.core/*uberjar-file* ~uberjar
                crane.core/*trampoline-file* (System/getenv "CRANE_TRAMPOLINE_FILE")]
        (crane.main-local/main ~(:name project) ~(vec args)))
     '(require 'crane.main-local 'crane.core))))
