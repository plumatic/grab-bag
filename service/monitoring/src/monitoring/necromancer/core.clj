(ns monitoring.necromancer.core
  (:use plumbing.core)
  (:require [plumbing.graph :as graph]
            [plumbing.parallel :as parallel]
            [plumbing.logging :as log]
            [plumbing.new-time :as new-time]
            [aws.ec2 :as ec2]))

(def bundle
  (graph/graph
   :ec2-client (fnk [ec2-keys] (ec2/ec2 ec2-keys))

   :retrainer-instances (fnk [ec2-client]
                          (fn []
                            (mapcat
                             #(ec2/list-instances ec2-client #{%})
                             (map (partial ec2/->security-name "retrainer") [:stage :prod]))))

   :social-scores-instances (fnk [ec2-client]
                              #(ec2/list-instances ec2-client #{"grabbag-social-scores"}))

   :revive-retrainer
   (graph/instance parallel/scheduled-work [should-revive-instances? hour-to-revive-instances ec2-client retrainer-instances social-scores-instances]
     {:period-secs (new-time/convert 1 :day :secs)
      :initial-offset-secs (new-time/time-until-hour (new-time/pst-calendar) hour-to-revive-instances :seconds)
      :f (fn []
           (when should-revive-instances?
             (let [instances (concat (social-scores-instances) (retrainer-instances))]
               (when-let [running-instances (seq (filter ec2/is-running? instances))]
                 (log/errorf "Trying to start already running instances: %s" (pr-str running-instances)))
               (log/infof "Starting instances: %s" (seq instances))
               (ec2/start ec2-client (map :instanceId instances)))))})))
