(ns plumbing.streaming-map-test
  (:use clojure.test plumbing.core plumbing.test plumbing.streaming-map)
  (:require
   [plumbing.core-incubator :as pci]
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.graph :as graph]
   [plumbing.logging :as log]))

;; set up a streaming map, with a shutdown
;; ensure that on close the shutdown steps both occur
(deftest streaming-map-shutdown-test
  (let [effect (atom 0)
        sm (map->StreamingMap
            {:shutdown  [#(swap! effect inc)
                         #(throw (RuntimeException. "thrown!"))
                         #(swap! effect inc)]})]
    (.close sm)
    (is (= @effect 2))))

(deftest observer-rewrite-order-test
  (let [g (graph/graph
           :x (fnk [a])
           :y {:y1 (fnk [b])
               :y2 (fnk [c])}
           :z (fnk [b]))
        gprime (observer-rewrite g nil)]
    (is (= (keys g) (keys gprime)))
    (is (= (keys (:y g)) (keys (:y gprime))))
    (is (= (pfnk/io-schemata g) (pfnk/io-schemata gprime)))))

(deftest observer-rewrite-big-order-test
  (let [g (apply graph/graph
                 (apply concat
                        (for [i (range 50)]
                          [(keyword (str "s" i))
                           (fnk [a])])))
        gprime (observer-rewrite g nil)]
    (is (= (keys g) (keys gprime)))
    (is (= (pfnk/io-schemata g) (pfnk/io-schemata gprime)))))

(deftest with-node-name-exceptions-test
  (let [g (with-node-name-exceptions
            (graph/graph {:a (fnk [n] (/ 1 n))}))]
    (is (= {:a 1/23} (graph/run g {:n 23})))
    (is (= [:a] (try
                  (graph/run g {:n 0})
                  (catch clojure.lang.ExceptionInfo ex
                    (-> ex ex-data :node-keyseq)))))))

(deftest streaming-map-constructor-test
  (let [effects (atom [])
        sm (streaming-map #(swap! effects conj (inc (:input %))) {:queue-type :fifo-unbounded :num-threads 10})]
    (dotimes [i 10] (submit sm i))
    (Thread/sleep 10)
    (.close sm)
    (is (.isShutdown (:pool sm)))
    (is (= (sort @effects) (range 1 11)))))

(def worker-graph
  (graph/graph
   :x (fnk [[:input a]] (Thread/sleep 1) (inc a))
   :y {:y1 (fnk [[:input b]] (inc b))
       :y2 (fnk [x y1] (* x y1))}
   :z (fnk [[:y y2] [:input a {c 1}]]
        (when (= c :info) (log/throw+ {:log-level :info}))
        (when (= c :sleep) (Thread/sleep 10))
        (* a y2 c))))

(deftest streaming-graph-sub-test
  (let [observe-queue (atom nil)
        sub     (atom nil)
        outputs (atom {})
        pub (fn [k] (fn [x] (swap! outputs update-in [k] conj x)))]
    (with-redefs [plumbing.observer/report-hook
                  (fn [_ _ f] (reset! observe-queue f))]
      (let [gr (streaming-graph
                {:observer nil
                 :graph worker-graph
                 :num-threads 1
                 :observe-queues? true
                 :queue-type :fifo-unbounded
                 :publishers {:y (pub :y) :z (pub :z)}
                 :subscriber (fn [f] (reset! sub f))})]
        (with-open [gr gr]
          (@sub {:a 10 :b 20})
          (@sub {:a 10 :b 20 :c :info})
          (@sub {:a 10 :b 20 :c nil})
          (@sub {})
          (@sub {:a 2 :b 5})
          (is (> (@observe-queue) 2))
          (Thread/sleep 10)))
      (is (= [{:y1 6 :y2 18} {:y1 21 :y2 231}] (@outputs :y)))
      (is (= [36 2310] (@outputs :z))))))


(def sleepy-graph
  (graph/graph
   :slept-time (fnk [[:input nap-time]]
                 (Thread/sleep nap-time)
                 nap-time)))

(deftest streaming-graph-cancel-test
  (let [outputs (atom {})
        pub (fn [k] (fn [x] (swap! outputs update-in [k] conj x)))]
    (let [gr (streaming-graph
              {:observer nil
               :graph sleepy-graph
               :num-threads 5
               :queue-type :fifo-unbounded
               :publishers {:slept-time (pub :slept-time)}
               :refill-fn (fn [queue-size] [ {:nap-time 10} {:nap-time 5000}])
               :timeout-seconds 0.1
               :refill-period 0.01})]
      (Thread/sleep 30)
      (.close gr))
    (is (every? #{10} (-> outputs deref :slept-time)))
    (is (not-any? #{5000} (-> outputs deref :slept-time)))))


(deftest ^:flaky graph-resource-refill-test
  (let [observe-queue (atom nil)
        outputs (atom {})
        pub (fn [k] (fn [x] (swap! outputs update-in [k] conj x)))]
    (with-redefs [plumbing.observer/report-hook
                  (fn [_ _ f] (reset! observe-queue f))]
      (let [gr (streaming-graph
                {:observer nil
                 :graph worker-graph
                 :num-threads 1
                 :queue-type :fifo-unbounded
                 :observe-queues? true
                 :publishers {:y (pub :y) :z (pub :z)}
                 :refill-fn (fn [queue-size]
                              [{:a 10 :b 20}
                               {:a 2 :b 5}
                               {:a 1 :b 2 :c :sleep}
                               {:a 3 :b 6}])
                 :refill-period 0.012})]
        (Thread/sleep 20)
        (.close gr))
      (is (= [{:y1 6 :y2 18} {:y1 21 :y2 231} {:y1 6 :y2 18} {:y1 21 :y2 231}] (@outputs :y)))
      (is (= [36 2310 36 2310] (@outputs :z))))))

(deftest ^:flaky graph-resource-priority-refill-test
  (let [observe-queue (atom nil)
        outputs (atom {})
        pub (fn [k] (fn [x] (swap! outputs update-in [k] conj x)))]
    (with-redefs [plumbing.observer/report-hook
                  (fn [_ _ f] (reset! observe-queue f))]
      (let [gr (streaming-graph
                {:observer nil
                 :graph worker-graph
                 :num-threads 1
                 :queue-type :priority
                 :observe-queues? true
                 :publishers {:y (pub :y) :z (pub :z)}
                 :refill-fn (fn [queue-size]
                              (map
                               (fn [m] (map->PrioritizedItem {:item m :priority (:b m)}))
                               [{:a 10 :b 20}
                                {:a 2 :b 5}
                                {:a 1 :b 2 :c :sleep}
                                {:a 3 :b 1}]))
                 :refill-period 0.012})]
        (Thread/sleep 20)
        (.close gr))
      (is (= [{:y1 6 :y2 18} {:y1 21 :y2 231} {:y1 6 :y2 18} {:y1 21 :y2 231}] (@outputs :y)))
      (is (= [36 2310 36 2310] (@outputs :z))))))

(deftest sharded-streaming-fn-test
  (let [thread-id (fn [] (.getId (Thread/currentThread)))
        outputs (atom {})
        sm (sharded-streaming-fn
            {:observer nil
             :num-threads 4
             :observe-queues? true
             :shard-val str
             :f (fn [input]
                  (swap! outputs update (thread-id) conj input))})]

    (dotimes [_ 5] (submit sm 0))
    (is-eventually (= [0 0 0 0 0] (first (vals @outputs))))

    (swap! outputs empty)

    (doseq [x (shuffle (aconcat (repeat 4 (range 20))))]
      (submit sm x))
    (testing "each value should have gone to same queue 4 times."
      (is-eventually
       (every? #{4} (aconcat
                     (for [queue (vals @outputs)]
                       (vals (frequencies queue)))))))))
