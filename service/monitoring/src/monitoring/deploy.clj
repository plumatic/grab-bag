(ns monitoring.deploy
  (:use plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [plumbing.parallel :as parallel]
   [service.core :as service]
   [service.observer :as service-observer]
   [monitoring.necromancer.core :as necromancer]
   [monitoring.monitor :as monitor]
   [monitoring.monitors.machines :as monitors-machines]
   [monitoring.monitors.news.external :as monitors-news-external]
   [monitoring.monitors.test :as monitors-test]
   [monitoring.monitors.topic-api :as monitors-topic-api]))


(service/defservice
  [:persistent-metrics (graph/instance service-observer/register-persistent-metrics! []
                         {:agg-fns {:onboarding-queue (comp :state :onboarding-queue-atom)}})

   :send-fail-emails (fnk [send-email env]
                       (fnk [subject text]
                         (let [e (case env
                                   :stage "backend+stage+alert@example.com"
                                   :prod "monitoring@example.com")]
                           (send-email {:from "noreply+alert@example.com"
                                        :to e
                                        :subject subject
                                        :text text}))))

   :necromancer necromancer/bundle

   :last-error (fnk [] (atom {}))

   :monitor-resources {:machines monitors-machines/monitor-resources
                       :test monitors-test/monitor-resources
                       :topic-api monitors-topic-api/monitor-resources
                       :news-external monitors-news-external/monitor-resources}

   :monitor (fnk [env monitor-resources]
              (let [all-monitors (for-map [[prefix g] monitor-resources
                                           [suffix monitor] (safe-get g :monitors)
                                           :when (not (and (safe-get monitor :prod-only?)
                                                           (not= env :prod)))]
                                   (keyword (str (name prefix) "_" (name suffix)))
                                   monitor)]
                {:alert-name->pd-svc (map-vals #(safe-get % :tier) all-monitors)
                 :monitors (monitor/composite-monitor
                            (map-vals #(safe-get % :monitor) all-monitors))}))

   :monitoring (graph/instance parallel/scheduled-work
                   [env last-error send-fail-emails monitor]
                 {:period-secs 60
                  :f (fn []
                       (let [alert-name->pd-svc #(safe-get-in monitor [:alert-name->pd-svc %])
                             report (try (monitor/execute (:monitors monitor))
                                         (catch Exception e {:fcs (str "error monitoring" e)}))
                             cur-ks (set (keys report))
                             last-report @last-error
                             last-ks (set (keys last-report))
                             subject-prefix (case env
                                              :stage "stage "
                                              :prod "")]
                         (doseq [curr-alert cur-ks]
                           (::TRIGGER_ALERT_PLACEHOLDER
                            curr-alert
                            (alert-name->pd-svc curr-alert)
                            (get report curr-alert))
                           (when-not (last-ks curr-alert)
                             (send-fail-emails
                              {:subject (str subject-prefix "ALERT: " curr-alert)
                               :text (select-keys report [curr-alert])})))
                         (doseq [resolved-alert (remove (set cur-ks) last-ks)]
                           (::RESOLVE_ALERT_PLACEHOLDER
                            resolved-alert
                            (alert-name->pd-svc resolved-alert))
                           (send-fail-emails
                            {:subject (str subject-prefix "RESOLVED: " resolved-alert)
                             :text (select-keys report [resolved-alert])}))
                         (reset! last-error report)))})

   :daily-report (graph/instance parallel/scheduled-work [env send-fail-emails monitor]
                   {:period-secs (* 60 60 24)
                    :f #(do (Thread/sleep 1800000)
                            (send-fail-emails
                             {:subject
                              (str
                               (case env :stage "stage " :prod "")
                               "Grabbag daily service report")
                              :text (monitor/daily-report (:monitors monitor))}))})])
