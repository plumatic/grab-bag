(ns service.graft-test
  (:use plumbing.core clojure.test plumbing.test)
  (:require
   [plumbing.graph :as graph]
   [plumbing.resource :as resource]
   [service.graft :as graft]))


(defn test-res [updates-atom name]
  (swap! updates-atom conj [:starting name])
  (reify
    resource/PCloseable
    (close [_] (swap! updates-atom conj [:closing name]))))

(defn test-service-graph
  ([updates-atom extra-graph]
     (let [test-res (partial test-res updates-atom)]
       (graph/->graph
        (concat
         [[:a (fnk [] (test-res :a))]
          [:b (fnk [a] (test-res :b))]
          [:c (fnk [b] (test-res :c))]]
         extra-graph))))
  ([updates-atom]
     (test-service-graph updates-atom [])))


(defn test-current-service [test-service-graph]
  (let [running-graph ((plumbing.graph/lazy-compile test-service-graph) {})]
    (assoc running-graph
      :service.impl/graph test-service-graph
      :instantiated-resources-atom
      (atom (vals (plumbing.map/map-leaves-and-path
                   (fn [keyseq node] [keyseq (get-in running-graph keyseq)]) test-service-graph))))))

(deftest hotswap-a-resource-c
  (let [service-log-atom (atom [])
        test-res (partial test-res service-log-atom)
        service-graph (test-service-graph service-log-atom)
        current-service (test-current-service service-graph)]
    (testing "The resources should have started"
      (is-= [[:starting :a] [:starting :b] [:starting :c]] @service-log-atom)
      (reset! service-log-atom []))
    (testing "Now going to graft a new resource into the service"
      (let [new-service (graft/graft-service-graph current-service [:c (fnk [b] (test-res :c'))])]
        (is-= [[:closing :c] [:starting :c']] @service-log-atom)
        (reset! service-log-atom [])
        (testing "Closing all resources"
          (plumbing.resource/shutdown! @(:instantiated-resources-atom new-service))
          (is-= [[:closing :c'] [:closing :b] [:closing :a]] @service-log-atom)
          (reset! service-log-atom []))))))

(deftest hotswap-a-resource-b
  (let [service-log-atom (atom [])
        test-res (partial test-res service-log-atom)
        service-graph (test-service-graph service-log-atom)
        current-service (test-current-service service-graph)]
    (testing "The resources should have started"
      (is-= [[:starting :a] [:starting :b] [:starting :c]] @service-log-atom)
      (reset! service-log-atom []))
    (testing "Now going to graft a new resource into the service"
      (let [new-service (graft/graft-service-graph current-service [:b (fnk [a] (test-res :b'))])]
        (is-= [[:closing :c] [:closing :b] [:starting :b'] [:starting :c]] @service-log-atom)
        (reset! service-log-atom [])

        (testing "Closing all resources"
          (plumbing.resource/shutdown! @(:instantiated-resources-atom new-service))
          (is-= [[:closing :c] [:closing :b'] [:closing :a]] @service-log-atom)
          (reset! service-log-atom []))))))

(deftest hotswap-a-new-resource-d
  (let [service-log-atom (atom [])
        test-res (partial test-res service-log-atom)
        service-graph (test-service-graph service-log-atom)
        current-service (test-current-service service-graph)]
    (testing "The resources should have started"
      (is-= [[:starting :a] [:starting :b] [:starting :c]] @service-log-atom)
      (reset! service-log-atom []))
    (testing "Now going to graft a new resource into the service"
      (let [new-service (graft/graft-service-graph current-service [:d (fnk [b c] (test-res :d))])]
        (is-= [[:starting :d]] @service-log-atom)
        (reset! service-log-atom [])
        (testing "There should be a d in the keys, in the right order"
          (is-=-by set [:d :c :b :a] (remove #{:instantiated-resources-atom :service.impl/graph} (keys new-service))))

        (testing "Closing all resources"
          (plumbing.resource/shutdown! @(:instantiated-resources-atom new-service))
          (is-= [[:closing :d] [:closing :c] [:closing :b] [:closing :a]] @service-log-atom)
          (reset! service-log-atom []))))))

(deftest preserve-existing-keys
  (let [service-log-atom (atom [])
        test-res (partial test-res service-log-atom)
        service-graph (test-service-graph service-log-atom [[:e (fnk [] (test-res :e))]])
        current-service (merge (test-current-service service-graph) {:extra "lol"})]
    (testing "The resources should have started"
      (is-= [[:starting :a] [:starting :b] [:starting :c] [:starting :e]] @service-log-atom)
      (reset! service-log-atom []))
    (testing "Now going to graft a new resource into the service"
      (let [new-service (graft/graft-service-graph current-service [:d (fnk [b c] (test-res :d))])]
        (is-= [[:starting :d]] @service-log-atom)
        (reset! service-log-atom [])
        (testing "There should be a d and e in the keys"
          (is-=-by set [:d :e :c :b :a]
                   (remove #{:extra :instantiated-resources-atom :service.impl/graph} (keys new-service))))
        (testing "Make sure that the extra key is still present in the new service"
          (is (contains? new-service :extra)))
        (testing "Closing all resources"
          (plumbing.resource/shutdown! @(:instantiated-resources-atom new-service))
          (is-= [[:closing :d] [:closing :e] [:closing :c] [:closing :b] [:closing :a]] @service-log-atom)
          (reset! service-log-atom []))))))
