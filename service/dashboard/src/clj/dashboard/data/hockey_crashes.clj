(ns dashboard.data.hockey-crashes
  "API Client for hockey app crash reporting."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.error :as err]
   [plumbing.json :as json]
   [plumbing.time :as time]
   [web.client :as client]
   [dashboard.data.core :as data]))

(s/defn ^:always-validate crashes-last-month :- data/Timeseries
  "Return the last 30 days of crashes for the app store version of the Grabbag app."
  []
  (err/with-retries
    3 500 #(throw %)
    (let [res (client/fetch
               :get
               {:scheme "https"
                :host "rink.hockeyapp.net"
                :uri (format "/api/2/apps/0c45308388eaeb4a906407d0f8f6a457/crashes/histogram?start_date=20140101&end_date=%s"
                             (data/utc-date))
                :headers {"X-HockeyAppToken" "801fb1c395824ad0895bf9513dbb6849"}})]
      (assert (= (:status res) 200))
      (-> res
          :body
          json/parse-string
          (safe-get "histogram")
          (->> (map (fn [[d c]] [(.getMillis (time/parse d)) c])))))))
