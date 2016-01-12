(ns service.impl-test
  (:use clojure.test plumbing.core service.impl)
  (:require
   [plumbing.resource :as resource]
   [crane.config :as config]))

(defn make-config [params resources br bo bf]
  (let [graph (build-service-graph resources br bo bf)]
    (check-service-graph params graph)
    {:parameters params :resources graph}))

(defn simple-abstract-config [params]
  (-> {:parameters params
       :machine {:tags {:owner "grabbag-corp"}}}
      (config/enved-config-spec :test)
      (config/abstract-config "test")))

(deftest make-config-test
  (let [params (simple-abstract-config {:foo :bar :service-name "bla"})]
    (is (= params (:parameters (make-config params [:x (fnk [foo])] {} {} {}))))
    (is (thrown? RuntimeException  (make-config params [] [] nil)))

    (is (= [:a :b]
           (keys (:resources (make-config
                              params
                              [:a (fnk [foo])
                               :b (fnk [a])]
                              {} {} {})))))

    (is (thrown? RuntimeException
                 (make-config
                  params
                  [:a (fnk [foo b])
                   :b (fnk [])]
                  [] [])))

    (is (= [:x :y :z :u :v :w :a :b :c :q]
           (keys (:resources (make-config
                              params
                              [:a (fnk [foo {v 1}])
                               :b (fnk [w a])
                               :c (fnk [a {b 2}])]
                              (array-map
                               :x (fnk [])
                               :y (fnk [x])
                               :z (fnk [y]))
                              (array-map
                               :u (fnk [])
                               :v (fnk [u])
                               :w (fnk [])
                               :out (fnk []))
                              {:q (fnk [x])})))))))

(defn simple-config [params]
  (-> params simple-abstract-config test-config))

(deftest service-test
  (let [shut! (let [a (atom [])] (fn ([] @a) ([x] (swap! a conj x))))
        s (service
           (simple-config {:one 1 :two 2})
           (array-map
            :a (fnk [] (throw (RuntimeException.)))
            :b (fnk [a] (throw (RuntimeException.)))
            :four (fnk [one] (+ one 3))
            :shut-four (fnk [four] (reify resource/PCloseable (close [_] (shut! four))))
            :ten  (fnk [one {two 17} {three 3} four]
                    (is (= [1 2 3 4] [one two three four]))
                    (+ one two three four))
            :shut-ten (fnk [ten] (reify resource/PCloseable (close [_] (shut! ten))))
            :c (fnk [ten {four 2}] (throw (RuntimeException.)))))]
    (is (= [1 2 4 10] (take 4 (mapv s [:one :two :four :ten :shut-four :shut-ten]))))
    (is (empty? (shut!)))
    (resource/shutdown! @(:instantiated-resources-atom s))
    (is (= [10 4] (shut!)))
    (is (thrown? RuntimeException (s :c)))))
