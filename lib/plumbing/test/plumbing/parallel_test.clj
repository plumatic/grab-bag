(ns plumbing.parallel-test
  (:use clojure.test plumbing.core plumbing.parallel plumbing.test)
  (:require
   [plumbing.resource :as resource])
  (:import
   [java.util.concurrent TimeoutException]
   java.util.concurrent.TimeUnit plumbing.SaneExecutorService))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Creating and destroying thread pools


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Scheduling and waiting for tasks

(deftest ^:slow schedule-limited-work-test
  (let [times (atom [100 200 110 1000 10])
        results (atom [])]
    (resource/with-open [closeable (schedule-limited-work
                                    {:f #(let [t (first @times)]
                                           (swap! times next)
                                           (Thread/sleep t)
                                           (swap! results conj t))
                                     :submit-rate-ms 20
                                     :time-limit-ms 150
                                     :max-threads 2})]
      (is (= [] @results))
      (Thread/sleep 190)
      (is (= 1 (count @times)))
      (is (= [100] @results))
      (Thread/sleep 300)
      (is (= [100 110 10] @results))))

  (let [never-reset-after-this (atom 0)]
    (resource/with-open [closeable
                         (schedule-limited-work
                          {:f #(do (Thread/sleep 100) (reset! never-reset-after-this 1))
                           :submit-rate-ms 10
                           :time-limit-ms 30 ; time-limit. much shorter than the sleep in the function above.
                           :max-threads 1})]
      (Thread/sleep 150) ; sleep longer than the function sleeps.
      (is (= 0 @never-reset-after-this)))))

(deftest ^{:slow true} waiting-until
  (let [start (System/currentTimeMillis)]
    (wait-until (fn [] nil) 1)
    (is (< (- 1000
              (- (System/currentTimeMillis) start))
           500))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Running tasks in parallel

(deftest work-first-arg-test
  (resource/with-open [pool (fixed-thread-pool 5)]
    (is (= (range 1 100) (map-work pool inc (range 0 99)))))
  (is (= (range 1 100) (map-work 5 inc (range 0 99))))
  (is (= (range 1 100) (map-work inc (range 0 99)))))

(deftest ^:slow map-work-test
  (let [f (fn [batch?]
            (let [args (concat
                        (when batch? [15])
                        [200 #(doto % Thread/sleep) (range 100)])]
              (-> (apply map-work args)
                  doall
                  future
                  (.get (long 1500) java.util.concurrent.TimeUnit/MILLISECONDS))))]
    (is (= (range 100)
           (f true)
           (f false))))

  (let [start (System/currentTimeMillis)
        s (map-work 10 #(do (Thread/sleep 10) (inc %)) (concat (range 100) [""]))]
    (is (< (- (System/currentTimeMillis) start) 10))
    (is (= (range 1 11) (take 10 s)))
    (is (< (- (System/currentTimeMillis) start) 100))
    (is (thrown? Exception (dorun s)))))

(deftest ^:slow keep-work-test
  (let [f (fn [batch?]
            (let [args (concat
                        (when batch? [15])
                        [200 #(do (Thread/sleep %) (when (odd? %) %)) (range 100)])]
              (-> (apply keep-work args)
                  doall
                  future
                  (.get (long 1500) java.util.concurrent.TimeUnit/MILLISECONDS))))]
    (is (= (range 1 100 2)
           (f true)
           (f false)))))


(deftest ^:slow do-work-test
  (let [a (atom #{})]
    (do-work 10 #(swap! a conj (inc %)) (range 100))
    (is (= @a (set (range 1 101))))
    (reset! a #{})
    (is (thrown? Exception (do-work 10 #(do (Thread/sleep 100) (swap! a conj (inc %)))
                                    (cons "" (range 1000)))))
    (is (< (count @a) 11))))

(deftest letp-test
  (testing "correctness"
    (letp [a 1 b 2 c 3]
          (is-= a 1)
          (is-= b 2)
          (is-= c 3)))
  (testing "parallel-ness"
    (letp [a (do (Thread/sleep 100) (millis))
           b (millis)]
          (is (< b a)))))

(deftest weighted-future-task-test
  ;; ensure that callback gets called, and with correct value
  ;; ensure that sorting by priority works
  (let [callback-results (atom [])
        callback (fn [result] ( swap! callback-results conj result))]
    (is (= (map #(.priority %) (sort (for [priority [3 5 2 6 9 1 1]]
                                       (plumbing.SaneExecutorService$WeightedFutureTask. (with-meta (constantly 3) {:priority (double priority)})))))
           (map double [9 6 5 3 2 1 1])))
    (.run (plumbing.SaneExecutorService$WeightedFutureTask. (with-meta (constantly 2) {:callback callback})))
    (is (= [2] @callback-results))))

(deftest submit-task-test
  (let [side-effects (atom [])
        executor (exec-service {:num-threads 1 :queue-type :priority})
        p (promise)]
    (try
      (submit-task executor (fn [] @p))
      ;; add in a bunch of extra tasks, which we will clear out
      ;; ensure they're cleared
      ;; and then ensure that their side-effects didn't happen
      (dotimes  [_ 10]
        (submit-task executor identity {:callback (partial swap! side-effects conj)}))
      (clear-tasks executor) ;; clear
      (is (= 0 (.size (.getQueue executor))))
      (doseq [w [3 5 2 9 1 6 3]]
        (submit-task executor (constantly w) {:priority w :callback (partial swap! side-effects conj)}))
      (deliver p nil)
      (finally
        (.shutdown executor)
        (.awaitTermination executor 10000000 TimeUnit/MILLISECONDS)
        (is (= @side-effects [9 6 5 3 3 2 1]))))))


(deftest exec-service-caller-runs-test
  (let [side-effects (atom [])
        fifo-blocking ^SaneExecutorService (exec-service {:num-threads 1
                                                          :queue-type :fifo-bounded-blocking
                                                          :bound 10})]
    (resource/with-open [fifo-blocking fifo-blocking]
      (let [my-promise (promise)]
        (doseq [n  (range 11)]
          (submit-task fifo-blocking (fn [] @my-promise (swap! side-effects conj n))))
        (submit-task fifo-blocking ^Callable (fn [] (swap! side-effects #(conj % -1))))
        (deliver my-promise 0)))
    ;; we should run this one due to CallerRunsPolicy
    (.awaitTermination fifo-blocking 1000 TimeUnit/MILLISECONDS)
    (is (= @side-effects (range -1 11)))))

;; confirm that fifo-bounded-reject-newest actually does reject newest
(deftest exc-service-reject-newest
  (let [fifo-reject-newest (exec-service {:num-threads 1 :queue-type :fifo-bounded-reject-newest :bound 10})
        my-promise (promise)]
    (try
      (dotimes [n 11] (submit-task fifo-reject-newest #(@my-promise)))
      (is (thrown? Throwable (submit-task fifo-reject-newest (constantly 3))))
      (deliver my-promise 0)
      (finally
        (.shutdown fifo-reject-newest)
        (.awaitTermination fifo-reject-newest 1000 TimeUnit/MILLISECONDS)))))

(deftest exec-service-dump-oldest-test
  (let [side-effects (atom [])
        record-to-side-effects (fn [ft]
                                 (swap! side-effects #(conj %1 1)))
        fifo-blocking ^SaneExecutorService (exec-service {:num-threads 1
                                                          :queue-type :fifo-bounded-blocking
                                                          :bound 10})
        fifo-bounded-reject-newest ^SaneExecutorService (exec-service {:num-threads 2
                                                                       :queue-type :fifo-bounded-reject-newest
                                                                       :bound 10})
        fifo-bounded-dump-oldest ^SaneExecutorService (exec-service {:num-threads 1 :queue-type :fifo-bounded-dump-oldest
                                                                     :bound 10
                                                                     :rejection-callback record-to-side-effects})
        fifo-unbounded ^SaneExecutorService (exec-service {:num-threads 2
                                                           :queue-type :fifo-unbounded})
        priority ^SaneExecutorService (exec-service {:num-threads 2
                                                     :queue-type :priority})]


    ;; confirm that fifo-bounded-dump-oldest calls callback for oldest
    (submit-task fifo-bounded-dump-oldest ^Callable #( Thread/sleep 100))
    (dotimes [n 11] (submit-task fifo-bounded-dump-oldest ^Callable (constantly 2)))
    (.shutdown fifo-bounded-dump-oldest)
    (is (= @side-effects [1])))) ;; reject handler just puts 1 into side-effects))

(deftest with-timeout-test
  (is-= 42 (with-timeout 100 42 (throw!)))
  (is-= 42 (with-timeout 100 (do (Thread/sleep 10) 42) (throw!)))
  (is-= :fail (with-timeout 10 (do (Thread/sleep 100) 42) :fail))
  (is (thrown? RuntimeException (with-timeout 10 (do (Thread/sleep 100) 42) (throw!)))))

(deftest throttle-test
  (let [a1 (atom 0)
        a2 (atom 0)
        throttled (throttle 10 #(swap! a1 inc) #(swap! a2 inc))]
    (with-millis 100
      (dotimes [_ 100] (throttled)))
    (is (= @a1 1))
    (with-millis 200
      (dotimes [_ 100] (throttled)))
    (is (= @a1 2))
    (is (= @a2 198))))
