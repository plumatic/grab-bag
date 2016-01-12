(ns flop.map-test
  (:use clojure.test plumbing.core plumbing.test flop.map)
  (:require
   [plumbing.serialize :as serialize]))


(deftest ld-conversion-test
  (let [m1 {1 1.0 2 2.0 1000000000000000 3.0 4 -4.0}]
    (is (= m1 (trove->map (map->trove m1))))
    (is (= m1 (fv->map (map->fv m1))))
    (is (= m1 (trove->map (fv->trove (map->fv m1)))))
    (is (= (map->trove m1) (fv->trove (map->fv m1))))))

(deftest incremental-fv-test
  (let [m1 {1 1.0 2 2.0 1000000000000000 3.0}
        t (map->trove m1)
        fv (feature-vector)]
    (doseq [i (range 100)]
      (.put fv i i))
    (is (= (count fv) 99))
    (is (<= 100 (.capacity fv) 200))
    (is (= (.dotProduct fv t) 5.0))
    (.put fv 1 3.0)
    (is (= (.dotProduct fv t) 7.0))
    (.increment fv 1 3.0)
    (is (= (.dotProduct fv t) 10.0))
    (.put fv 1 1.0)
    (is (= (count fv) 99))
    (.increment fv 2 -2.0)
    (is (= (count fv) 98))
    (is (= (.get fv 2) 0.0))

    (doseq [i (range 90)]
      (.remove fv i))
    (is (= (.dotProduct fv t) 0.0))
    (is (= (count fv) 10))
    (is (<= 10 (.capacity fv) 40))

    (let [nv (serialize/serialize serialize/+java+ fv)
          fv2 (serialize/deserialize nv)]
      (is (= (count fv2) 10))
      (is (= (.capacity fv2) 10))
      (is (= (.dotProduct fv2 (map->trove {90 1.0 95 2.0})) 280.0)))

    (doseq [i (range 90 100)]
      (.remove fv i))


    (.compact fv)
    (is (= 0 (.capacity fv) ))
    (is (= 0 (count fv) ))
    (.put fv 1 10.0)
    (.put fv 4 20.0)
    (is (= 2 (count fv)))
    (is (= (.dotProduct fv t) 10.0))
    (.put fv 1000000000000000 3.0)
    (is (= (.dotProduct fv t) 19.0))
    (.compact fv)
    (is (= 3 (.capacity fv) ))
    (is (true?  (.remove fv 4)))
    (is (false?  (.remove fv 4)))
    (is (= 2 (count fv)))
    (is (= (.dotProduct fv t) 19.0))
    (is (true? (.remove fv 1)))
    (is (= (.dotProduct fv t) 9.0))
    (.put fv 1 2.0)
    (is (= (.dotProduct fv t) 11.0))))

(set! *warn-on-reflection* true)

(deftest pivot-normalize-test
  (let [pn! (fn [m p] (trove->map (doto (map->trove m) (pivot-normalize! p))))]
    (is (= {1 1.0} (pn! {1 20.0} 20)))
    (is (= {1 0.05} (pn! {1 1.0} 20)))
    (is (= {1 0.05 2 0.05} (pn! {1 1.0 2 1.0} 20)))
    (is (= {1 0.5 2 0.5} (pn! {1 10.0 2 10.0} 0.1)))))

(deftest dot-product-test
  (let [m1 (into {} (for [[i v] (map-indexed vector (take 100 (distinct (repeatedly #(long (rand-int 10000))))))]
                      [v (double i)]))
        ld1 (map->trove m1)
        od1 (map->otrove m1)
        m2 (into {} (for [i (range 10000)] [i 1.0]))
        ld2 (map->trove m2)
        od2 (map->otrove m2)]
    (is (== (trove-dot-product ld1 ld1) 328350))
    (is (== (fast-trove-dot-product ld1 ld1) 328350))
    (is (== (trove-dot-product od1 od1) 328350))
    (is (== (trove-dot-product ld1 od1) 328350))

    (is (== (trove-dot-product ld1 ld2) 4950))
    (is (== (fast-trove-dot-product ld1 ld2) 4950))
    (is (== (fast-trove-dot-product ld2 ld1) 4950))
    (is (== (trove-dot-product ld2 ld1) 4950))
    (is (== (trove-dot-product od1 od2) 4950))))


(defn time-dot-products []
  (let [m1 (into {} (for [[i v] (map-indexed vector (take 100 (distinct (repeatedly #(rand-int 10000)))))]
                      [v (double i)]))
        m1t (map->trove m1)
        m1v (map->fv m1)
        m2 (map->trove (into {} (for [i (range 10000)] [i 1.0])))]
    (is (= (.capacity m1v) (count m1)))
    (doseq [[n f] {:trove #(trove-dot-product m1t m2)
                   :trover #(trove-dot-product m2 m1t)
                   :trove2 #(fast-trove-dot-product m1t m2)
                   :trove2r #(fast-trove-dot-product m2 m1t)
                   ;; :trove3 #(fast-trove-dot-product m2 m1t)
                   :fv #(.dotProduct ^flop.LongDoubleFeatureVector m1v ^gnu.trove.TLongDoubleHashMap m2)}]
      (println n)
      (is (= (f) 4950.0))
      (dotimes [_ 3]
        (println (time (reduce + (repeatedly 100000 f))))))))

(deftest explain-trove-dot-product-test
  (is (= (explain-trove-dot-product (map->trove {1 1 2 2}) (map->trove {1 5 3 10 2 -1.0}))
         [[1 1.0 5.0 5.0]
          [2 2.0 -1.0 -2.0]])))

(deftest increment-key-test
  (let [t (map->trove {})]
    (increment-key! t 1 2.0)
    (is (= (.get t 1) 2.0))
    (increment-key! t 1 3.0)
    (is (= (.get t 1) 5.0))))

;; (time-dot-products)

(deftest select-keys-test
  (is-=-by trove->map
           (map->trove {42 1.0 43 2.0})
           (ld-select-keys
            (map->trove {42 1.0 43 2.0 44 3.0})
            [42 43])))

(set! *warn-on-reflection* false)
