(ns classify.utils-test
  (:use clojure.test classify.utils)
  (:require
   [hiphip.double :as d]
   [flop.math :as dm]))

(deftest l2-reg-fn-test
  (let [f (fn [w] [0.0 (double-array [0 0 0 0 0 0])])
        f-l2 (l2-reg-fn f 2)
        w (double-array [1 2 3 4 5 6])
        [v g] (f-l2 w)]
    (is (dm/within 1e-6 22.75 v))
    (is (dm/within 1e-6 0.5 (d/aget g 0)))
    (is (dm/within 1e-6 1.0 (d/aget g 1)))
    (is (dm/within 1e-6 1.5 (d/aget g 2)))
    (is (dm/within 1e-6 2.0 (d/aget g 3)))
    (is (dm/within 1e-6 2.5 (d/aget g 4)))
    (is (dm/within 1e-6 3.0 (d/aget g 5)))))


(deftest fancy-l2-reg-fn-test
  (let [f (fn [w] [0.0 (double-array [0 0 0 0 0 0])])
        f-l2 (fancy-l2-reg-fn f (double-array (repeat 6 0.0)) (double-array (repeat 6 0.5)))
        w (double-array [1 2 3 4 5 6])
        [v g] (f-l2 w)]
    (is (dm/within 1e-6 22.75 v))
    (is (dm/within 1e-6 0.5 (d/aget g 0)))
    (is (dm/within 1e-6 1.0 (d/aget g 1)))
    (is (dm/within 1e-6 1.5 (d/aget g 2)))
    (is (dm/within 1e-6 2.0 (d/aget g 3)))
    (is (dm/within 1e-6 2.5 (d/aget g 4)))
    (is (dm/within 1e-6 3.0 (d/aget g 5))))

  (let [f (fn [w] [0.0 (double-array [0 0 0 0 0 0])])
        f-l2 (fancy-l2-reg-fn f (double-array [1 2 3 4 5 5]) (double-array (repeat 6 0.5)))
        w (double-array [1 2 3 4 5 6])
        [v g] (f-l2 w)]
    (is (dm/within 1e-6 0.25 v))
    (is (dm/within 1e-6 0.0 (d/aget g 0)))
    (is (dm/within 1e-6 0.0 (d/aget g 1)))
    (is (dm/within 1e-6 0.0 (d/aget g 2)))
    (is (dm/within 1e-6 0.0 (d/aget g 3)))
    (is (dm/within 1e-6 0.0 (d/aget g 4)))
    (is (dm/within 1e-6 0.5 (d/aget g 5)))))
