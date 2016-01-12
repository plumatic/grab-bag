(ns kinesis.core-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [plumbing.serialize :as serialize]
   [kinesis.core :as kinesis]))

(deftest round-trip-test
  (with-millis 100
    (let [r (serialize/pack (kinesis/record-encoder) ["m1" {:m 2}])]
      (is-= 1 (count r))
      (is-= {:date 100 :messages ["m1" {:m 2}]}
            (kinesis/decode-record (first r))))))
