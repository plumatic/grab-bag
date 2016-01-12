(ns plumbing.stats
  "Sampling and other statistical functions"
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.hash :as hash])
  (:import
   [java.util Random]))

(s/defschema Distribution [(s/pair s/Any "option" s/Num "weight")])

(s/defn sample :- s/Any
  "Given a random number generator and a Distribution, deterministically sample a
   condition in a way that will be consistent across (rnd, distribution) pairs"
  [rnd :- Random
   dist :- Distribution]
  (loop [q (* (sum second dist) (.nextDouble rnd))
         [[o w] & dist] dist]
    (if (< q w)
      o
      (recur (- q w) dist))))

(s/defn interleave-by-weight
  [rand :- Random
   weights :- [s/Num]
   colls :- [[s/Any]]]
  (assert (= (count weights) (count colls)))
  (let [dist (remove (fn [[idx w]] (or (empty? (nth colls idx)) (zero? w))) (indexed weights))]
    (if (seq dist)
      (let [idx (sample rand dist)
            [sq1 [f & r]] (split-at idx colls)]
        (lazy-seq (cons (first f) (interleave-by-weight rand weights (concat sq1 [(rest f)] r)))))
      [])))
