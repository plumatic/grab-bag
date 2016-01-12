(ns service.nameserver-test
  (:use clojure.test
        service.nameserver)
  (:require [store.bucket :as bucket]))

(deftest ^{:slow true} pub-test
  (let [b (bucket/bucket {:type :mem})
        a {:public {:host "pub" :port 3} :private {:host "prv" :port 4}}
        c (nameserver-publisher b "test" a 1)
        v (bucket/get b "test")]
    (is v)
    (is (= (map v [:addresses :period-s]) [a 1]))
    (is (:timestamp v))
    (dotimes [_ 2]
      (bucket/delete b "test")
      (is (nil? (bucket/get b "test")))
      (Thread/sleep 1200)
      (is (= (map (bucket/get b "test") [:addresses :period-s]) [a 1])))
    (c)
    (bucket/delete b "test")
    (Thread/sleep 1200)
    (is (nil? (bucket/get b "test")))))

(deftest ^{:slow true} client-test
  (let [b (bucket/bucket {:type :mem})
        cprv (nameserver-client b {:stale-s 2})
        cpub (nameserver-client b {:stale-s 2 :host-key :public})]
    ((nameserver-publisher b "foo" {:public {:host "pubfoo" :port 3}
                                    :private {:host "prvfoo" :port 4}} 1))
    ((nameserver-publisher b "bar" {:public {:host "pubbar" :port 3}
                                    :private {:host "prvbar" :port 4}} 1))
    (is (= (lookup-host cprv "foo") "prvfoo"))
    (is (= (lookup-host cpub "foo") "pubfoo"))
    (is (= (lookup-address cprv "bar") {:host "prvbar" :port 4}))
    (is (= (lookup-address cpub "bar") {:host "pubbar" :port 3}))
    (is (= (set (all-service-names cpub)) #{"foo" "bar"}))
    (let [fv (lookup cpub "foo")]
      (is (= (set (keys fv)) #{:addresses :timestamp :period-s :age :started :uptime :config}))
      (is (= (:age fv) 0)))
    (is (= (set (keys (service-map cpub))) #{"foo" "bar"}))
    (service-map cpub)
    (Thread/sleep 1000)
    (is (= (:age (lookup cpub "foo")) 1))
    (Thread/sleep 2000)
    (is (= (set (all-service-names cpub)) #{"foo" "bar"}))
    (is (empty? (service-map cpub)))
    (is (empty? (bucket/keys b)))))
