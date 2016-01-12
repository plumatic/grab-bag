(ns monitoring.monitors.topic-api
  "Monitors for the public topic-api service."
  (:use plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [aws.elb :as elb]
   [web.client :as client]
   [monitoring.monitor :as monitor]
   [monitoring.monitors.core :as monitors]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private helpers

(def +machine-learning-article+
  "Taken from wikipedia for monitoring the topic api"
  {:title "Machine Learning"
   :body
   "Machine learning is a scientific discipline that explores the
    construction and study of algorithms that can learn from
    data. Such algorithms operate by building a model based on inputs
    and using that to make predictions or decisions, rather than
    following only explicitly programmed instructions.

   Machine learning can be considered a subfield of computer science
   and statistics. It has strong ties to artificial intelligence and
   optimization, which deliver methods, theory and application domains
   to the field. Machine learning is employed in a range of computing
   tasks where designing and programming explicit, rule-based
   algorithms is infeasible. Example applications include spam
   filtering, optical character recognition (OCR), search engines and
   computer vision. Machine learning is sometimes conflated with data
   mining, although that focuses more on exploratory data
   analysis. Machine learning and pattern recognition can be viewed
   as two facets of the same field."})

(defn topic-api-elb-name [env]
  (case env
    :prod "prod-topic"
    :stage "staging-topic"))

(defn topic-api-token [env]
  (case env
    ;; generated for email: monitoring@example.com
    :prod (throw (Exception. "API-TOKEN"))
    :stage (throw (Exception. "API-TOKEN"))))

(defn topic-api-url [env]
  (case env
    :prod "http://interest-graph.example.com"
    :stage "http://topic.staging.example.com"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: monitor resources

(def monitor-resources
  (graph/graph
   :monitors
   (fnk [env ec2-keys]
     {:topic-api-load-balancer-min-instances
      (monitors/highpri-prod-only
       (monitor/simple-monitor
        (fn []
          (let [api-count (count (elb/instances ec2-keys (topic-api-elb-name env)))]
            (when (< api-count 2)
              {:too-few-topic-api-machines api-count})))
        10000 15))

      :topic-api-classification
      (monitors/highpri
       (monitor/simple-monitor
        #(letk [[topics] (client/json
                          (client/json-post
                           (format "%s/text/topic?api-token=%s"
                                   (topic-api-url env)
                                   (topic-api-token env))
                           +machine-learning-article+))]
           (when-not (some (fnk [topic] (= topic "Machine Learning")) topics)
             {:failed-to-classify topics}))
        500))})))
