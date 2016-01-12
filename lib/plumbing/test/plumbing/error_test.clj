(ns plumbing.error-test
  (:refer-clojure :exclude [future])
  (:use clojure.test plumbing.error))


(set! *warn-on-reflection* true)

(defn foo [])

;; TODO: fix

(deftest -?>-test
  (is  (= (-?> {:a 1} :a inc) 2))
  (is  (= (-?> {:a 1} inc) nil)))

(deftest silent-nil
  (is (= [nil 3 4] (map (partial with-ex
                                 (fn [& args] nil)
                                 inc)
                        ["1" 2 3]))))

(deftest logging
  (let [[a l] (atom-logger)
        f (with-ex l / 4 0)]
    (is (= "java.lang.ArithmeticException: Divide by zero" @a))))

(deftest failure
  (let [[a l] (atom-logger)]
    (is (= "java.lang.ArithmeticException: Divide by zero"
           (with-ex l / 4 0)))))

(deftest error
  (let [[a l] (atom-logger)]
    (is (= "java.lang.AssertionError: foo"
           (with-ex l (fn [] (throw (AssertionError. "foo"))))))))


(deftest with-print-all
  (let [r (with-ex (partial print-all 100) / 4 0)]
    (is (= {:ex "java.lang.ArithmeticException: Divide by zero",
                                        ;           :fn "clojure.core//",
            :args ["4" "0"]}
           (dissoc r :stack :fn)))))

(deftest vertex-with-print-all
  (let [r (with-ex (partial print-all 100) {:f / :id "assclown"} 4 0)]
    (is (= {:ex "java.lang.ArithmeticException: Divide by zero",
            :fn "assclown",
            :args ["4" "0"]}
           (dissoc r :stack)))))

(def horribly-long-string "of course, Macs. Anyone who argues that the iPad 2 falls short because it doesn&#8217;t offer enough to get current iPad owners to upgrade is missing the point. Apple&#8217;s target is not the 15-20 or so million people who&#8217;ve already bought a tablet. They&#8217;re looking at the hundreds of millions of people who haven&#8217;t yet, but will soon.")

(def truncated-long-str "of course, Macs. Anyone who argues that the iPad 2 falls short because it doesn&#8217;t offer enough")

(deftest truncate-str
  (is (= truncated-long-str
         (truncate 100 horribly-long-string))))

(defn gnarly-data-structure [s]
  (zipmap (range 6)
          (repeat 5
                  (cons [1 {:a 2 :q [43]}] (repeat 2 s)))))

(deftest truncated-walk-test
  (is (= (repeat 5 truncated-long-str)
         (truncate-walk 100 (repeat 5 horribly-long-string))))
  (is (= (gnarly-data-structure truncated-long-str)
         (truncate-walk 100
                        (gnarly-data-structure horribly-long-string)))))

(deftest assert-keys-test
  (is (= nil
         (assert-keys [:a] {:a "a"})))
  (let [[a l] (atom-logger)]
    (is (= "java.lang.Exception: The Map argument {} is missing keys: #{:a}"
           (with-ex l assert-keys [:a] {})))))


(deftest ^{:slow true} with-retries-fn-test
  (let [n (atom 0)
        f (fn [& _]
            (if (<= @n 0)
              (do (swap! n inc)
                  (throw (RuntimeException.)))
              42))
        wrf (with-retries-fn 2 (constantly ::fail) f)]
    (is (= (wrf) 42))
    (reset! n -42)
    (is (= (wrf) ::fail))
    (reset! n -42)
    (is (= 42 ((with-retries-fn 44 (constantly ::fail) f))))))

(deftest with-retries-test
  (let [n (atom 0)]
    (is (thrown? IllegalArgumentException
                 (with-retries
                   3 0 #(throw (IllegalArgumentException. "asdf" %))
                   (do (swap! n inc) (throw (RuntimeException. "asdf"))))))
    (is (= @n 3))
    (is (= 5
           (with-retries
             3 0 #(throw (IllegalArgumentException. "asdf" %))
             (do (swap! n inc) 5))))
    (is (= @n 4))))

(set! *warn-on-reflection* false)
