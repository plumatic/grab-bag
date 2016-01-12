(ns dashboard.data.google-analytics
  "Getting real-time users on site from google analytics.

   Provisioned here: https://console.developers.google.com/project/apps~black-abode-606/apiui/credential?authuser=1
   and by giving the email address below access to our google analytics account."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.auth :as auth]
   [plumbing.error :as err]
   [plumbing.graph :as graph]
   [plumbing.parallel :as parallel]
   [plumbing.serialize :as serialize]
   [store.bucket :as bucket]
   [store.snapshot-bucket :as snapshot-bucket]
   [dashboard.data.core :as data])
  (:import
   [com.google.api.client.googleapis.auth.oauth2 GoogleCredential GoogleCredential$Builder]
   [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]
   [com.google.api.client.http.javanet NetHttpTransport]
   [com.google.api.client.json.jackson2 JacksonFactory]
   [com.google.api.services.analytics Analytics Analytics$Builder AnalyticsScopes]
   [com.google.api.services.analytics.model RealtimeData]))

(set! *warn-on-reflection* true)

(def +key+ "KEY")

(def ^NetHttpTransport +http-transport+ (GoogleNetHttpTransport/newTrustedTransport))

(defn ^GoogleCredential credential []
  (-> (GoogleCredential$Builder.)
      (.setTransport +http-transport+)
      (.setJsonFactory (JacksonFactory/getDefaultInstance))
      (.setServiceAccountId (throw (Exception. "ACCOUNT-ID")))
      (.setServiceAccountScopes [AnalyticsScopes/ANALYTICS])
      (.setServiceAccountPrivateKey (serialize/deserialize (auth/decode-base64 +key+)))
      (.build)))

(defn ^Analytics analytics [cred]
  (-> (Analytics$Builder. +http-transport+ (JacksonFactory/getDefaultInstance) cred)
      (.setApplicationName "Real-time user dashboard")
      (.build )))

(def +ios-table+ "IOS-TABLE")
(def +web-table+ "WEB-TABLE")
(def +android-table+ "ANDROID-TABLE")

(def clients {:web +web-table+ :ios +ios-table+ :android +android-table+})

(s/defn active-users :- long
  [a :- Analytics
   table :- String]
  (let [a ^Analytics a]
    (-> (.data ^Analytics a)
        .realtime
        (.get table "rt:activeUsers")
        ^RealtimeData (.execute)
        .getRows
        ffirst
        Long/parseLong)))

(def real-time-users-history-graph
  (graph/graph
   :bucket (graph/instance snapshot-bucket/s3-snapshot-bucket [env]
             {:snapshot-bucket-name "grabbag-analytics"
              :snapshot-key (format "analytics/%s/google-realtime" (name env))
              :period-secs 600})
   #_#_ ;; removed
   :update (graph/instance parallel/scheduled-work [bucket]
             {:period-secs 60
              :f (fn []
                   (err/?warn
                    "Error accessing google analytics"
                    (let [analytics (analytics (credential))]
                      (doseq [[k table] clients]
                        (let [n-users (active-users analytics table)]
                          (bucket/update bucket k #(conj (or % []) [(millis) n-users])))))))})))

(s/defschema RealTimeHistory
  (for-map [k (keys clients)]
    k
    data/Timeseries))

(s/defn raw-timeseries :- RealTimeHistory
  [g]
  (into {} (bucket/seq (safe-get g :bucket))))

(set! *warn-on-reflection* false)
