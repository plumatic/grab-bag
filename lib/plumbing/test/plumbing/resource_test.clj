(ns plumbing.resource-test
  (:refer-clojure :exclude [with-open])
  (:use clojure.test plumbing.core plumbing.resource)
  (:require
   [schema.core :as s]
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.graph :as graph]
   [plumbing.map :as map]))

(defn simple-deref-resource [r close-fn]
  (reify
    clojure.lang.IDeref (deref [this] r)
    PCloseable (close [this] (when close-fn (close-fn r)))))

(deftest resource-transform-test
  (with-redefs [plumbing.observer/sub-observer (fn [o k] (conj o k))]
    (let [g {:first (fnk [i observer]
                      (is (= observer [:first]))
                      (simple-deref-resource (inc i) nil))
             :second (fnk [first] (inc @first))
             :third {:third1 (fnk [second observer]
                               (is (= observer [:third :third1]))
                               (inc second))
                     :third2 (fnk [third1]
                               (simple-deref-resource (inc third1) nil))}}
          res (graph/run (resource-transform :instantiated-atom (observer-transform g))
                         {:i 1 :observer []})]
      (is (= {:first 2
              :second 3
              :third {:third1 4
                      :third2 5}}
             (map/map-leaves #(if (number? %) % @%) (dissoc res :instantiated-atom))))
      (is (= [[:first] [:second] [:third :third1] [:third :third2]]
             (map first @(:instantiated-atom res))))
      (let [[v1 v2 v3 v4] (map second @(:instantiated-atom res))]
        (is (= [3 4] [v2 v3]))))))


(deftest shutdown!-test
  (let [shutdown-atom (atom [])
        shut! (fn [k] (swap! shutdown-atom conj k))
        g {:first (fnk [i]
                    (simple-deref-resource (inc i) shut!))
           :second (fnk [first] (inc @first))
           :third {:third1 (fnk [second] (inc second))
                   :third2 (fnk [third1]
                             (simple-deref-resource (inc third1) shut!))}
           :fourth (fnk [[:third third1]]
                     (simple-deref-resource (* third1 2) shut!))
           :fifth (fnk [fourth]
                    (reify
                      clojure.lang.IDeref
                      (deref [this] (* @fourth 10))
                      PCloseable
                      (close [this] (shut! "SPARTA!!!!"))))}
        res (graph/run (resource-transform :instantiated-atom g)
                       {:i 1})]
    (is (= {:first 2
            :second 3
            :third {:third1 4
                    :third2 5}
            :fourth 8
            :fifth 80}
           (map/map-leaves #(if (number? %) % @%) (dissoc res :instantiated-atom))))
    (shutdown! @(:instantiated-atom res))
    (is (= ["SPARTA!!!!" 8 5 2] @shutdown-atom))))

(deftest bundle-test
  (let [shutdown-atom (atom [])
        shut! (fn [k] (swap! shutdown-atom conj k))
        b (bundle
           :foo (fnk [x {y 1}]
                  (simple-deref-resource [x y] shut!))
           :bar (fnk [foo z] (simple-deref-resource [@foo z] shut!)))]
    (is (= {:x s/Any (s/optional-key :y) s/Any :z s/Any s/Keyword s/Any} (pfnk/input-schema b)))
    (let [g (b {:x 10 :z 100})]
      (is (= {:foo [10 1] :bar [[10 1] 100]} (map-vals deref g)))
      (is (empty? @shutdown-atom))
      (close g)
      (is (= [[[10 1] 100] [10 1]] @shutdown-atom)))))

(deftest upstream-subgraph-test
  (let [g {:u (fnk [a] (inc a))
           :v (fnk [b u] (+ b u))
           :w (fnk [u] (inc u))
           :x (fnk [w] (inc w))
           :y (fnk [v u] (+ v u))
           :z (fnk [x] (inc x))}]
    (is (= nil (keys (upstream-subgraph g {}))))
    (is (= [:u] (keys (upstream-subgraph g {:u true}))))
    (is (= [:u :w :x :z] (keys (upstream-subgraph g {:z true}))))
    (is (= [:u :v :y] (keys (upstream-subgraph g {:y true}))))
    (is (= (set [:u :v :w :x :y :z]) (set (keys (upstream-subgraph g {:y true :z true})))))))

(deftest resource-optional-instance-test
  (let [b (bundle :x (fnk [{o 1}] o))
        i (graph/instance b {:o 10})]
    (is (= 10 (:x (i {}))))))
