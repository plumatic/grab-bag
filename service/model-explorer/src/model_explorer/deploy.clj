(ns model-explorer.deploy
  (:use plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [plumbing.new-time :as new-time]
   [plumbing.serialize :as serialize]
   [plumbing.parallel :as parallel]
   [web.fnhouse :as fnhouse]
   [aws.ec2 :as ec2]
   [store.bucket :as bucket]
   [store.sql :as sql]
   [service.core :as service]
   [model-explorer.isis.db :as isis-db]
   [model-explorer.isis.model-db :as model-db]
   [model-explorer.models :as models]
   [model-explorer.core :as model-explorer]
   model-explorer.handlers))

(defnk ensure-consistent!
  "Ensures that each tweet has a tweeter in the DB"
  [tweets tweeters :as training-data]
  (let [now (millis)
        tweeter-ids (set (map (fn-> (safe-get :id)) (model-explorer/all-data tweeters)))]
    (->> (model-explorer/all-data tweets)
         (map (fnk [id [:datum user]]
                (isis-db/->entry :tweeter user {"consistency-check" {:source-tweet id :date now}})))
         (remove (fnk [id] (tweeter-ids id)))
         (distinct-by (fn-> (safe-get :id)))
         (apply model-explorer/put!))))

(defn write-experiment! [b name html]
  (assert (string? html))
  (let [k (str (millis) "-" name)]
    (bucket/put b k html)
    (str (throw (Exception. "EXPERIMENTS-URL")) k)))

(service/defservice
  [:connection-pool (fnk [env] (sql/connection-pool (safe-get isis-db/env->+db-spec+ env)))
   :isis-data isis-db/training-data-graph
   :training-data (fnk [[:isis-data stores]] stores)

   :model-store model-db/model-store

   :model-graph models/model-graph

   :archive-bucket (graph/instance bucket/bucket-resource []
                     {:type :s3
                      :name "grabbag-training-data"
                      :key-prefix "model-explorer/label-archive/"
                      :serialize-method serialize/+default+})

   ;; bucket to put arbitrary HTML pages.
   :experiments-bucket (graph/instance bucket/bucket-resource []
                         {:type :s3
                          :name "grabbag-training-data"
                          :key-prefix "model-explorer/experiments/"
                          :serialize-method serialize/+default+})

   :list-data (fnk [training-data]
                (fn [datatype]
                  (when (= datatype :ad-impression)
                    (model-explorer/all-data (safe-get training-data :ad-impression))))
                ;;(fn [datatype] (isis-db/all-data-with-neighbors isis-data datatype (models/datum->auto-labels-fn model-graph)))
                )

   :web-server (graph/instance (fnhouse/simple-fnhouse-server-resource
                                {"" 'model-explorer.handlers})
                   [env [:service server-port]]
                 {:port server-port
                  :admin? (not= env :test)})

   :kill-me-please
   (graph/instance parallel/scheduled-work
       [ec2-keys [:instance instance-id]]
     {:period-secs (new-time/convert 1 :hour :secs)
      :initial-offset-secs (new-time/convert 24 :hours :secs)
      :f (fn [] (ec2/stop (ec2/ec2 ec2-keys) [instance-id] false))})])
