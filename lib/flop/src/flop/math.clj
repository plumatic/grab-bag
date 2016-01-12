(ns flop.math
  (:require
   [schema.core :as s]
   [plumbing.core :as plumbing])
  (:import
   [flop DMath DArray Sample]
   [java.util Map Random]))

(definline sample-boolean [^Random r p]
  `(< (.nextDouble ~r) (double ~p)))

(definline sample-gaussian [^Random r]
  `(DMath/sampleGaussian ~r))

(definline sample-t
  "Recommended to use plumbing.MersenneTwister as random, otherwise
   results may be inaccurate.  See
   http://www.ams.org/journals/mcom/1994-62-206/S0025-5718-1994-1219702-8/S0025-5718-1994-1219702-8.pdf"
  [^Random r dof]
  `(DMath/sampleStudentT ~r ~dof))

(definline sample-gamma [^Random r a rate]
  `(DMath/sampleGamma ~r (double ~a) (double ~rate)))

(definline digamma [x]
  `(DMath/digamma (double ~x)))

(definline log-gamma [x]
  `(DMath/logGamma (double ~x)))

(definline sloppy-log [x]
  `(DMath/sloppyLog (double ~x)))


(definline sloppy-exp [x]
  `(DMath/sloppyExp (double ~x)))

(definline sloppy-exp-negative [x]
  `(DMath/sloppyExpNegative (double ~x)))

(definline sample-beta [^Random r alpha beta]
  `(Sample/sampleBeta ~r (double ~alpha) (double ~beta)))

(defn logistic ^double [^double score]
  (/ 1.0 (+ 1.0 (Math/exp (- score)))))

(comment
  (defn sloppy-exp [x]
    (DMath/sloppyExp (double x)))

  (defn sloppy-exp-negative [x]
    (DMath/sloppyExp (double x))))

(defn within
  "|x-y| < z"
  [z x y]
  (< (Math/abs (double (- x y))) (double z)))

(defn sparse-dot-product
  "Dot product between two vectors represented by maps. Bottleneck, written for efficiency"
  [^Map x ^Map y]
  (if (<= (count x) (count y))
    (plumbing/sum (fn [e] (* (double (val e)) (double (get y (key e) 0.0))))  x)
    (sparse-dot-product y x)))

(defn norm-sparse-vec [v]
  (let [norm (Math/sqrt (sparse-dot-product v v))]
    (if (> norm 0.0)
      (plumbing/map-vals #(/ % norm) v)
      v)))

(defn mean [vs]
  (/ (plumbing/sum vs) (count vs)))

(let [to-log2 (/ 1.0 (Math/log 2))]
  (defn log2 ^double [^double x]
    (* to-log2 (Math/log x))))


(defn categorical-distribution
  "A schema describing a distribution over various options,
   where each option matches the option-schema"
  [option-schema]
  (s/both
   [(s/pair s/Num "weight" option-schema "option schema")]
   (s/pred
    (fn [distribution-seq]
      (let [probs (map first distribution-seq)]
        (and (not-any? neg? probs)
             (< (Math/abs (- 1.0 (plumbing/sum probs))) 0.01))))
    'normalized?)))

(s/defn sample-categorical
  [r :- Random
   d :- (categorical-distribution s/Any)]
  (->> d
       (map first)
       (double-array)
       (DArray/sampleDiscrete r)
       (nth d)
       second))

(s/defn deterministic-sample-categorical
  [p :- Double
   d :- (categorical-distribution s/Any)]
  (loop [p p
         [[w item] & more] (seq d)]
    (let [np (- p w)]
      (if (or (<= np 0) (not (seq more)))
        item
        (recur np more)))))
