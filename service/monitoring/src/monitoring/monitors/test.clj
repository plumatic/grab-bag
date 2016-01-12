(ns monitoring.monitors.test
  "Test monitor that can be triggered by swanking an atom"
  (:use plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [monitoring.monitor :as monitor]))


(def monitor-resources
  (graph/graph
   :test-atom (fnk []
                (atom (map-from-keys
                       (constantly true)
                       [:lowpri :highpri :rad-highpri :rad-lowpri])))
   :monitors (fnk [test-atom]
               (map-from-keys
                (fn [tier]
                  {:monitor (monitor/simple-monitor
                             (fn []
                               (when-not (get @test-atom tier)
                                 {:test-alert (format "hello world! you set the atom to false for %s." tier)}))
                             50 1)
                   :tier tier
                   :prod-only? false})
                (keys @test-atom)))))
