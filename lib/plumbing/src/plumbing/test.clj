(ns plumbing.test
  "Tools for testing"
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [clojure.test :as test]
   [clojure.data :as data]
   [plumbing.core-incubator :as pci]
   [plumbing.serialize :as serialize]
   [plumbing.parallel :as parallel]
   potemkin
   [schema.test :as schema-test]))

(defmacro with-millis
  "mock time and time will mock you"
  [t & body]
  `(with-redefs [millis (fn ^long [] (long ~t))] ~@body))

;; for when you :use plumbing.test
(def pprint pprint/pprint)

(defn empty-diff? [[f s sh]] (and (nil? f) (nil? s)))

(defn pprint-diff [[left right both]]
  (pci/pprint-str (array-map :left-side-only left
                             :right-side-only right
                             :both-sides both)))

(defn assert-=-by [key-fn x y]
  (let [diff (data/diff (key-fn x) (key-fn y))]
    (assert (empty-diff? diff) (pprint-diff diff))))

(defn assert-= [x y]
  (assert-=-by identity x y))

(defmacro is-=-by
  [key-fn x y]
  ;; first test/is is to catch exceptions in forms
  `(let [diff# (test/is (data/diff (~key-fn ~x) (~key-fn ~y)))]
     (when-not (empty-diff? diff#)
       (test/is (empty? [:the-diff])
                (pprint-diff diff#)))))

(defmacro is-= [x y]
  `(is-=-by identity ~x ~y))

(defn is-approx-= [m1 m2 & [tolerance]]
  (with-redefs [clojure.data/atom-diff ;; until diff supports pluggable atom equality.
                (fn [a b]
                  (if (if (and (number? a) (number? b))
                        (< (Math/abs (double (- a b))) (or tolerance 0.001))
                        (= a b))
                    [nil nil a]
                    [a b nil]))]
    (is-= m1 m2)))

(defmacro is-eventually [condition & [secs sleep-ms]]
  `(do (parallel/wait-until (fn [] ~condition) (or ~secs 5) (or ~sleep-ms 1))
       (test/is ~condition)))

(potemkin/import-vars schema-test/validate-schemas)

(defn recorder
  "Return a function that records all calls, which can be retrieved (and cleared) on deref."
  ([] (recorder (constantly nil)))
  ([f] (let [a (atom [])]
         (reify
           clojure.lang.IFn
           (invoke [this] (.applyTo this (list)))
           (invoke [this x1] (.applyTo this (list x1)))
           (invoke [this x1 x2] (.applyTo this (list x1 x2)))
           (invoke [this x1 x2 x3] (.applyTo this (list x1 x2 x3)))
           (invoke [this x1 x2 x3 x4] (.applyTo this (list x1 x2 x3 x4)))
           (invoke [this x1 x2 x3 x4 x5] (.applyTo this (list x1 x2 x3 x4 x5)))
           ;; beyond this, you're on your own...
           (applyTo [this s] (swap! a conj s) (apply f s))

           clojure.lang.IDeref
           (deref [this] (get-and-set! a []))))))


(defn is-serializable?
  "Predicate for checking whether an object remains unchanged after round-trip serialization"
  [obj]
  (is-= obj (serialize/round-trip obj)))

(defn throw! [& args] (throw (RuntimeException. "This function should not be called.")))

(defn subset-eq
  "Checks whether one map is a subset of the other."
  [m1 m2]
  (is-= m1
        (select-keys m2 (keys m1))))

(defmacro is-subset
  "Check whether all elements present in x are present in y, recursively."
  [x y]
  ;; first test/is is to catch exceptions in forms
  `(let [diff# (test/is (data/diff ~x ~y))]
     (when-not (empty? (first diff#))
       (test/is (empty? [:the-diff])
                (pprint-diff diff#)))))
