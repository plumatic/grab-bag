(ns classify.core-test
  (:use clojure.test plumbing.core)
  (:require
   [plumbing.io :as io]
   [flop.map :as map]
   [classify.core :as classify]
   [classify.algorithms.max-ent :as max-ent]))

(def io-roundtrip (comp io/from-data io/to-data))

(deftest train-read-write-test
  (let [data [[[[:0 1.0] [:1 1.0]] :0] [[[:1 1.0] [:2 1.0]] :1]]
        classifier ((max-ent/trainer {:normalize? true}) data)
        rt-classifier (io/from-data (io/to-data classifier))]
    (is (= [:0 :1] (keys (:label->weights classifier))))
    (is (= (keys (:label->weights classifier)) (keys (:label->weights rt-classifier))))
    (is (= (map-vals io/to-data (:label->weights classifier))
           (map-vals io/to-data (:label->weights rt-classifier))))))


(deftest unpack-test
  (doseq [b [true false]]
    (let [data [[(map/map->fv {0 1.0 1 1.0}) :0] [(map/map->fv {1 1.0 2 1.0}) :1]]
          classifier ((max-ent/trainer {:unpack? b :normalize? true}) data)
          rt-classifier (io/from-data (io/to-data classifier))]
      (doseq [[fv l] data]
        (let [fv (if b fv (seq (.asMap fv)))]
          (is (= l (classify/best-guess rt-classifier fv)))))

      (is (= [:0 :1] (keys (:label->weights rt-classifier))))
      (is (= (keys (:label->weights classifier)) (keys (:label->weights rt-classifier))))
      (is (= (map-vals io/to-data (:label->weights classifier))
             (map-vals io/to-data (:label->weights rt-classifier)))))))
