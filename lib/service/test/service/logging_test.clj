(ns service.logging-test
  (:use clojure.test plumbing.core service.logging)
  (:require
   [plumbing.logging :as log]
   [plumbing.resource :as resource]
   [store.bucket :as bucket]
   [store.mongo :as mongo]))

(deftest ^:slow test-logging-resource
  (let [emails (atom nil)
        log-data (mongo/test-log-data-store)]
    (resource/with-open [resource (logging-resource
                                   {:instance {:service-name "test-service"}
                                    :log-data-store log-data
                                    :send-email     (fnk [:as m] (swap! emails conj m))
                                    :flush-freq     1})]
      (log/errorf "Here's an email for you.")
      (log/errorf "Here's another email for you.")
      (dotimes [_ 10]
        (log/infof "Multiple infos from the same line"))
      (log/infof "Another infos from the same line")
      (log/warnf "And a warning to round it out")

      (Thread/sleep 1500)
      (is (= (count @emails) 1))
      (is (= (count (:text (first @emails))) 1974))

      (is (= 3 (count (bucket/seq (mongo/bucket-collection log-data "log")))))
      (is (= 5 (count (bucket/seq (mongo/bucket-collection log-data "agg-log"))))))))
