(ns crane.main
  "Entry point for remote start of services.  Should eventually be replaced by direct
   call into service.core or the deploy ns."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [crane.config :as config]
   [crane.core :as crane]))

(defn crane-config []
  (System/getProperty "crane.config"))

(defn main [raw-args]
  (assert (#{["run" "start"] ["start"]} raw-args))
  (crane/init-main!)
  (load "crane_deploy")

  (@(crane/resolve-deploy "start")
   (config/remote-config
    (config/read-dot-crane)
    (s/validate config/AbstractConfig @(crane/resolve-deploy (crane-config))))))
