(ns flop.weight-vector-test
  (:use clojure.test plumbing.core flop.weight-vector)
  (:require [flop.map :as fm] [plumbing.io :as io])
  (:import [flop LongDoubleFeatureVector IWeightVector]))

(def io-roundtrip (comp io/from-data io/to-data))

(defn simple-weight-vec-test [^IWeightVector v]
  ;; represents [1 2 3]
  (is (= 14.0 (.dot-product v (double-array [1 2 3]))))
  (is (= 2.0 (.val-at v 1)))
  (.inc! v 0 1.0)
  (is (= 2.0 (.val-at v 0)))

  ;; Test dot product against indexed and unindexed
  (is (= 4.0 (.dot-product v (fm/map->fv {0 1.0 1 1.0}))))

  (is (= [[0 2.0] [1 2.0] [2 3.0]]
         (sort-by first
                  (.reduce  v
                            (fn [acc ^long idx ^double val] (conj acc  [idx val]))
                            []))))

  ;; io round-trip

  (is (= (class v) (class (io-roundtrip v))))

  )

(deftest weight-vec-test
  (simple-weight-vec-test (new-dense (double-array [1 2 3])))
  (simple-weight-vec-test (map->sparse {0 1 1 2 2 3}))
  (simple-weight-vec-test (io-roundtrip (new-dense (double-array [1 2 3]))))
  (simple-weight-vec-test (io-roundtrip (map->sparse {0 1 1 2 2 3}))))
