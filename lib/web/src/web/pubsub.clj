(ns web.pubsub
  (:use plumbing.core)
  (:require
   [plumbing.accumulators :as accumulators]
   [plumbing.error :as err]
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [plumbing.new-time :as new-time]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [store.bucket :as bucket]
   [store.s3 :as s3]
   [web.client :as client]
   [web.data :as data]
   [web.server :as server]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private helpers

(defn topic-bucket [bucket topic] (s3/sub-bucket bucket (str topic "/")))

(defn env-topic-name [topic env]
  (str topic "-" (name env)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Broker (shared between pub and sub)

(def broker-bucket
  "Bucket where subscribers write files under topic/service-name with info
   about where to reach them.

   Not an env-bucket, since we actually want cross-env talk currently
   (e.g. doc-poller prod --> index-master-stage).

   Suffixed \"-prod\" only because of a historical mistake, which
   will require a migration to fix."
  (graph/instance bucket/bucket-resource []
    {:type :s3
     :name "grabbag-unreliable-pubsub-prod"}))

(def pubsub-resources
  "Overall resource, to be included whenever pubbing or subbing is going on"
  (graph/graph
   :broker broker-bucket
   :sub-callbacks bucket/bucket-resource ;; topic --> [callback]
   ))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Subscribing

(def subscribers-resources
  "Resource shared between all subscribers"
  (graph/graph
   :pubsub-subscriber-port
   (fnk [] (server/get-available-port))

   :subscriber-server
   (graph/instance server/server-resource [[:pubsub sub-callbacks] pubsub-subscriber-port]
     {:port pubsub-subscriber-port
      :join? false
      :root-handler (fnk [uri body]
                      (let [^String raw-topic uri
                            topic (subs raw-topic 1)
                            body (data/decode-serialized-content body)]
                        (assert (.startsWith raw-topic "/"))
                        (if-let [callbacks (bucket/get sub-callbacks topic)]
                          (do (doseq [x body
                                      c callbacks]
                                (c x))
                              {:body "\"OK\""})
                          (log/throw+ {:client-message "Topic not found"}))))})

   :put-subscriber!
   (graph/instance parallel/scheduled-work-now
       [[:pubsub broker sub-callbacks]
        [:instance service-name] server-host pubsub-subscriber-port]
     {:period-secs 60
      :f (fn put-subscriber! []
           (doseq [topic (bucket/keys sub-callbacks)]
             (bucket/put
              (topic-bucket broker topic) service-name
              {:host server-host
               :port pubsub-subscriber-port
               :id service-name
               :topic topic
               :uri (str "/" topic)
               :date (millis)})))})))

(defn sync! [subscribers-graph]
  (parallel/refresh! (safe-get subscribers-graph :put-subscriber!)))

(defnk raw-subscriber
  "A subscriber to a specific raw-topic (which will not be env-d)"
  [subscriber*
   [:instance service-name]
   [:pubsub sub-callbacks]
   pubsub-stats
   raw-topic]

  (bucket/update
   sub-callbacks raw-topic
   (fn [x]
     (assert (not x))
     [(fn [x] (pubsub-stats :sub raw-topic))]))

  (sync! subscriber*)

  (fn [f] (bucket/update sub-callbacks raw-topic #(conj % f))))

(def subscriber
  "A subscriber to a topic, which will be auto-namespaced with env"
  (graph/instance raw-subscriber [env topic]
    {:raw-topic (env-topic-name topic env)}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Publishing

(defnk publish-resource [init-spec drain drain-trash-size]
  (let [spec-atom (atom init-spec)
        f (fn [msgs] (client/serialized-post (client/build-url @spec-atom) msgs))]
    (if drain
      (let [df (accumulators/time-batching-fn
                {:secs drain
                 :f f
                 :on-error (fn [t] (log/warn t {:message (str "Error publishing" (pr-str @spec-atom))}))
                 :max-queue-size drain-trash-size})]
        {:spec-atom spec-atom
         :f #(accumulators/offer-to df %)
         :res df})
      {:spec-atom spec-atom
       :res nil
       :f (comp f list)})))

(defn- mirror-subscribers! [broker pub-args pub-resources]
  (doseq [[topic topic-pub-args] (bucket/seq pub-args)]
    (let [broker-bucket (topic-bucket broker topic)
          specs (for-map [[id spec] (bucket/seq broker-bucket)
                          :let [dead? (when (and (:date spec)
                                                 (< (:date spec) (new-time/time-ago 10 :minutes)))
                                        (do (log/infof "Deleting old-ass subscriber %s %s" id spec)
                                            (bucket/delete broker-bucket id)
                                            true))]
                          :when (not dead?)]
                  id spec)]
      ;; ensure entries in local bucket
      (doseq [[id spec] specs]
        (when-not (contains? (bucket/get pub-resources topic) id)
          (bucket/update
           pub-resources topic
           (fn-> (assoc id (publish-resource (assoc (bucket/get pub-args topic) :init-spec spec))))))
        (reset! (safe-get-in (bucket/get pub-resources topic) [id :spec-atom]) spec))

      ;; delete stale entries
      (doseq [[id pub-resource] (bucket/get pub-resources topic)]
        (when-not (contains? specs id)
          (log/infof "Deleting subscriber %s" id)
          (resource/close (safe-get pub-resource :res))
          (bucket/update pub-resources topic (fn-> (dissoc id))))))))

(def publishers-resources
  "Resource shared between all publishers"
  (graph/graph
   :pub-args
   bucket/bucket-resource ;; topic --> pub-args

   :pub-resources
   bucket/bucket-resource ;; topic --> {service-id {:f fn (optional-key :res) PCloseable :spec-atom ...}}

   :shutdown-pub-resources
   (fnk [pub-resources]
     (reify resource/PCloseable
       (close [this]
         (doseq [[topic pub-map] (bucket/seq pub-resources)
                 [service-id pub-resource] pub-map
                 :let [res (safe-get pub-resource :res)]]
           (when res
             (err/?error
              (format "Error shutting down publisher %s to %s" topic service-id)
              (resource/close res)))))))

   :mirror-subscribers!
   (graph/instance parallel/scheduled-work-now
       [[:pubsub broker] pub-args pub-resources {refresh 30}]
     {:period-secs refresh
      :f (fn [] (mirror-subscribers! broker pub-args pub-resources))})))

(defnk raw-publisher
  "A publisher to a specific raw-topic (which will not be env-d)"
  [[:pubsub sub-callbacks]
   [:instance service-name]
   [:publisher* pub-args pub-resources mirror-subscribers!]
   pubsub-stats
   raw-topic
   {drain 10} {drain-trash-size nil} {refresh 30} {force-remote? false}]
  (assert (not (bucket/get pub-args raw-topic)))
  (bucket/put pub-args raw-topic {:drain drain :drain-trash-size drain-trash-size})
  (parallel/refresh! mirror-subscribers!)
  (fn [msg]
    (pubsub-stats :pub raw-topic)
    (when-not force-remote?
      (doseq [sub (bucket/get sub-callbacks raw-topic)]
        (sub msg)))
    (doseq [[id pub-resource] (bucket/get pub-resources raw-topic)]
      (when (and msg (or force-remote? (not= id service-name)))
        ((safe-get pub-resource :f) msg)))))

(def publisher
  "A publisher to a topic, which will be auto-namespaced with env"
  (graph/instance raw-publisher [env topic]
    {:raw-topic (env-topic-name topic env)}))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; A few async tools for combining and transforming subscribers.

(defn- make-subscriber
  "Make a subscriber from a function that takes its publish fn and somehow schedules
   publications on it."
  [sub-pub-fn!]
  (let [my-subscribers (atom [])]
    (sub-pub-fn! (fn [m] (doseq [s @my-subscribers] (s m))))
    (fn [s] (swap! my-subscribers conj s))))

(defn subscriber-concat
  "The asynchrounous version of concat.  Takes a sequence of input subscribers, and produces
   a subscriber that publishes one message for every message received on any input-subscriber."
  [& input-subscribers]
  (make-subscriber
   (fn [pub-fn!]
     (doseq [s input-subscribers]
       (s pub-fn!)))))
