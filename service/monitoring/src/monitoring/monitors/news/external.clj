(ns monitoring.monitors.news.external
  "Monitors for external-facing services of the old Grabbag news product"
  (:use plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [web.client :as client]
   [monitoring.monitor :as monitor]
   [monitoring.monitors.core :as monitors]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: monitor resources

(def monitor-resources
  (graph/graph
   :monitors
   (fnk [env ec2-keys]
     {:image-load-balancer
      (monitors/highpri-prod-only
       (monitor/ring-monitor
        ec2-keys "image" 2
        #(monitors/simple-health-check (throw (Exception. "IMAGE URL")))
        500
        (fn [inst]
          (monitors/simple-health-check
           (format "http://%s:4000/ping" (safe-get inst :privateDnsName))))))})))
