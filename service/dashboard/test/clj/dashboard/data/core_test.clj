(ns dashboard.data.core-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [schema.core :as s]
   [dashboard.data.core :as data]))

(deftest timeseries-schema-test
  (is (not (s/check data/Timeseries [[0 1] [2 1.0]])))
  (is (s/check data/Timeseries [[2 1] [0 1.0]])))

(deftest ceil-timestamp-test
  (is (= 200 (data/ceil-timestamp 199 10)))
  (is (= 200 (data/ceil-timestamp 200 10)))
  (is (= 210 (data/ceil-timestamp 201 10))))

(deftest downsample-test
  (is (= [[100 10] [300 20] [400 30]]
         (data/downsample
          [[10 10] [201 12] [300 20] [320 20] [330 10] [340 30]]
          100
          (partial apply max)))))

(use-fixtures :once validate-schemas)
