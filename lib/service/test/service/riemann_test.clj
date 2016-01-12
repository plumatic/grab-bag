(ns service.riemann-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [service.riemann :as riemann]))

(deftest riemann-test
  (let [c (riemann/client
           {:env :test :service {:type "bob"} :instance {:service-name "bob-test"} :nameserver nil})
        send! (fn [e] (riemann/send-events! c [e]))
        latest-event (fn [] (first (riemann/latest-events c)))]
    (testing "event elaboration"
      (is (send! {:service "tezt" :tags ["hi!"]}))
      (is-=-by #(update-in % [:time] class)
               {:host "bob-test"
                :service "tezt"
                :time 123
                :tags ["hi!" "env-test" "service-bob"]}
               (latest-event)))

    (testing "validation"
      (is (thrown? Exception (send! {})))
      (is (thrown? Exception (send! {:foo :bar})))
      (is (thrown? Exception (send! {:time "123"})))
      (is (thrown? Exception (send! {:tags "a"}))))


    (testing "keeps last 10 events"
      (riemann/send-events! c (for [i (range 12)] {:service "wow!" :metric i}))
      (is-= (range 11 1 -1)
            (map :metric (riemann/latest-events c))))))