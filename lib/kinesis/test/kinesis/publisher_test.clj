(ns kinesis.publisher-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [plumbing.resource :as resource]
   [crane.config :as config]
   [kinesis.publisher :as publisher]
   [kinesis.core :as kinesis]))

(deftest publisher-test
  (with-millis 100
    (let [records ((resource/with-open [g (resource/bundle-run
                                           publisher/publisher-graph
                                           {:env :test
                                            :ec2-keys config/+test-ec2-creds+
                                            :stream "bla"
                                            :num-threads 1
                                            :observer nil
                                            })]
                     (publisher/submit! g {:a 1})
                     (publisher/submit! g {:b 2})
                     (safe-get g :publish!)))]
      (is-= 1 (count records))
      (is-= {:date 100 :messages [{:a 1} {:b 2}]}
            (kinesis/decode-record (first records))))))
