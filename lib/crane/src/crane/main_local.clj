(ns crane.main-local
  "Entry point for local crane operations."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.classpath :as classpath]
   [crane.config :as config]
   [crane.core :as crane]
   crane.task))

(defn resolve-task [x]
  (ns-resolve 'crane.task (symbol x)))

(defn main [project-name [task config-sym & args]]
  (crane/init-main!)
  (let [ec2-creds (config/read-dot-crane)
        env (keyword config-sym)]
    (if (= :global env)
      (apply @(resolve-task task)
             ec2-creds args)
      (apply @(resolve-task task)
             ec2-creds
             (config/abstract-config
              (config/enved-config-spec
               (config/str->config-spec
                (classpath/read-from-classpath
                 (format "%s/config.clj"
                         (.replace ^String project-name "-" "_"))))
               env)
              project-name)
             args)))
  (when-not (= task "run")
    (System/exit 0)))
