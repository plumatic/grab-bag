(ns test-service.deploy-test
  (:use clojure.test plumbing.core plumbing.test test-service.deploy)
  (:require
   [plumbing.graph :as graph]
   [plumbing.streaming-map :as streaming-map]
   [store.bucket :as bucket]
   [web.client :as client]))

(deftest deploy-test
  (test-service
   (fnk [graph1 message-bucket :as s]
     (doseq [x ["m1" "m2" "m3"]]
       (streaming-map/submit graph1 x))
     (is-eventually
      (= ["m3" "m2" "m1"] (bucket/get message-bucket "got")))
     (is (= 16 (client/json
                (client/fetch
                 {:host "localhost" :port +port+ :uri "/inc-get-handler" :query-params {:x 15}})))))))
