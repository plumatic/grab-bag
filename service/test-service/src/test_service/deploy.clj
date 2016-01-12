(ns test-service.deploy
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.streaming-map :as streaming-map]
   [store.bucket :as bucket]
   [web.fnhouse :as fnhouse]
   [web.pubsub :as pubsub]
   [service.core :as service]
   [service.observer :as service-observer]))


(def pointless-first-graph
  (graph/instance streaming-map/streaming-fn [publish-test]
    {:f identity
     :publisher publish-test}))

(def pointless-second-graph
  (graph/instance streaming-map/streaming-fn [subscribe-test message-bucket]
    {:f (fn [m] (bucket/update message-bucket "got" (partial cons m)))
     :subscriber subscribe-test}))

(defnk $inc-get-handler$GET
  {:responses {200 s/Any}}
  [[:request [:query-params x :- long]]]
  {:body (inc x)})

(def +port+ 9876)

(service/defservice
  [:persistent-metrics (graph/instance service-observer/register-persistent-metrics! []
                         {:agg-fns {:memory (comp :used :heap :jvm :machine)}})
   :bucket bucket/bucket-resource
   :message-bucket bucket/bucket-resource
   :subscribe-test    (graph/instance pubsub/subscriber {:topic "test-topic"})
   :publish-test      (graph/instance pubsub/publisher {:topic "test-topic" :drain nil})
   :graph1            (graph/instance pointless-first-graph [foo] {:pointless-opt-arg foo})
   :graph2            pointless-second-graph
   :inc-get-handler   (graph/instance (fnhouse/simple-fnhouse-server-resource {"" 'test-service.deploy})
                          {:port +port+})
   :on-startup (fnk [bucket] (bucket/put bucket "key" "val"))])
