(ns domain.experiments
  "Data for representing experiments, options, and user assignments over time
   in a deterministic, stable, reproducable way.

   An experiment consists of a sequence of distributions over options with the
   same normalization constant.  The option for a user is a deterministic function
   of the experiment, point in time, and user; guaranteed to be uncorrelated
   with options for other users or experiments; and can be maximally stable over
   time (as experiment probabilities shift) by using the provided functions
   to construct stable sequences of distributions.

   nil is an implicit experiment option, which is the one that is returned for
   times before the experiment was started.  It can be provided as an explicit
   option as well, e.g., to indicate non-participation in an experiment.

   Typical use is to call `experiment` to create an experiment, then sample
   values using `sample-latest`.  Then `modify` can be used to update the
   experiment distribution, and `sample-at` can be used to get the value
   of an experiment at a historical point in time."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.hash :as hash]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(s/defschema Option
  "A JSON-able experiment option"
  s/Any)

(s/defschema WeightedOption
  (s/pair Option "option" (s/both Long (s/pred pos? 'pos?)) "weight"))

(s/defschema Distribution
  "An unnormalized integer probability distribution.
   For example, [[true 9] [false 1]] is a distribution of 90% true and 10% false.
   Order matters for stability of the samples."
  [WeightedOption])

(s/defschema UnorderedDistribution
  "An abstract notion of an integer distribution, without a defined order."
  {Option Long})

(s/defschema TimestampedDistribution
  {:distribution Distribution :start-date Long})

(s/defn normalization :- Long
  [d :- Distribution]
  (sum second d))

(s/defschema TimestampedDistributions
  "A list of distributions sorted by start time."
  (s/both [TimestampedDistribution]
          (s/pred #(and (vector? %) (= % (sort-by :start-date %))) 'time-ordered?)))

(s/defschema Experiment
  "An experiment can have a changing set of distributions over time,
   but the normalization constant must be stable."
  (s/both
   {:name String
    (s/optional-key :description) String
    :distributions TimestampedDistributions}
   (s/pred (fnk [distributions]
             (apply = (map (fnk [distribution] (normalization distribution)) distributions)))
           'consistent-normalization?)))

(s/defn latest-distribution :- Distribution
  [e :- Experiment]
  (safe-get (last (:distributions e)) :distribution))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: helpers for stability of experiments

(s/defn to-unordered :- UnorderedDistribution
  [d :- Distribution]
  (apply merge-with + (for [[o w] d] {o w})))

(s/defn best-overlap
  "The theoretical maximum overlap bewteen d1 and d2"
  [d1 :- Distribution d2 :- Distribution]
  (->> [d1 d2]
       (map to-unordered)
       (apply pci/merge-all-with min 0)
       vals
       sum))

(s/defn actual-overlap
  "The actual overlap bewteen d1 and d2"
  [d1 :- Distribution d2 :- Distribution]
  (let [cons-nonzero (fn [v o w] (if (zero? w) v (cons [o w] v)))]
    (loop [d1 d1 d2 d2 overlap 0]
      (assert (= (boolean (seq d1)) (boolean (seq d2))))
      (if (seq d1)
        (let [[[o1 w1] & more-d1] d1
              [[o2 w2] & more-d2] d2]
          (if (< w2 w1)
            (recur d2 d1 overlap)
            (recur more-d1
                   (cons-nonzero more-d2 o2 (- w2 w1))
                   (if (= o1 o2) (+ overlap w1) overlap))))
        overlap))))

(s/defn stable?
  "Are these pairs of distributions stable?"
  [d1 :- Distribution d2 :- Distribution]
  (= (best-overlap d1 d2)
     (actual-overlap d1 d2)))

(s/defn compact :- Distribution
  "Collapse neighboring identical elements"
  [d :- Distribution]
  (loop [in (next d) last (first d) out []]
    (if-let [[[fo fw] & more-in] (seq in)]
      (if (= fo (first last))
        (recur more-in [fo (+ fw (second last))] out)
        (recur more-in [fo fw] (conj out last)))
      (conj out last))))

(s/defn stabilize :- Distribution
  "Produce a new Distribution from an UnorderedDistribution new-dist that is ordered
   to have maximal consistency with old-dist.  Doesn't try to avoid fragmentation,
   besides compacting at the end and properly handling 2-element distributions.
   (TODO(jw): improve)"
  [new-dist :- UnorderedDistribution old-dist :- Distribution]
  (loop [in old-dist
         out []
         diffs (pci/merge-all-with - 0 new-dist (to-unordered old-dist))]
    (if-let [[[fo fw :as f] & more-in] (seq in)]
      (let [fdiff (get diffs fo 0)]
        (if (>= fdiff 0)
          (recur more-in (conj out f) diffs)
          (let [fills (loop [diffs diffs fdiff fdiff fills {}]
                        (let [[do dw :as fd] (first diffs)]
                          (if (pos? dw)
                            (if (>= (+ dw fdiff) 0)
                              (assoc fills do (- fdiff))
                              (recur (next diffs) (+ fdiff dw) (assoc fills do dw)))
                            (recur (next diffs) fdiff fills))))]
            (recur more-in
                   (let [nf (when (pos? (+ fdiff fw)) [[fo (+ fdiff fw)]])]
                     (if (seq out)
                       (into out (concat fills nf))
                       (into nf fills)))
                   (merge-with - (dissoc diffs fo) fills)))))
      (let [res (compact (into out (remove #(zero? (second %)) diffs)))]
        (assert (stable? res old-dist))
        res))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Sampling from distributions/experiments

(s/defn sample :- Option
  "Given a String seed and a Distribution, deterministically sample a
   condition in a way that will be consistent across (seed,
   distribution) pairs, but uniform and uncorrelated across small
   changes to seed."
  [dist :- Distribution
   seed :- String]
  (loop [q (mod (hash/murmur64 seed) (normalization dist))
         [[o w] & dist] dist]
    (if (< q w)
      o
      (recur (- q w) dist))))

(defn- join-seeds
  "Construct a joint seed from an external seed (user-id, etc) and experiment name"
  [experiment-name seed]
  (str experiment-name "||" seed))

(s/defn distribution-at :- Distribution
  [e :- Experiment date :- long]
  (when-let [d (last (take-while (fnk [start-date] (<= start-date date)) (safe-get e :distributions)))]
    (safe-get d :distribution)))

(s/defn sample-at :- Option
  [e :- Experiment
   seed :- String
   date :- Long]
  (when-let [d (distribution-at e date)]
    (sample d (join-seeds (safe-get e :name) seed))))

(s/defn sample-latest :- Option
  [e :- Experiment
   seed :- String]
  (sample (latest-distribution e) (join-seeds (safe-get e :name) seed)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Creating and updating experiments

(s/defn ^:always-validate experiment :- Experiment
  "Create an experiment with a name, initial start-date and distribution.
   You probably want to ensure the normalization constant is large (at least 100) since it
   determines the granularity of probabilities you can use throughout the experiment."
  [name :- String description :- String date :- Long dist :- UnorderedDistribution]
  {:name name :description description :distributions [{:start-date date :distribution (vec dist)}]})

(s/defn ^:always-validate modify-raw :- Experiment
  "Update an experiment with a new ordered probability distribution.  Ensuring
   stability is your job."
  [e :- Experiment date :- Long new-distribution :- Distribution]
  (update-in e [:distributions] conj {:start-date date :distribution new-distribution}))

(s/defn modify :- Experiment
  "Update an experiment with a new probability distribution.
   Stability (agreement with the previous distribution when they overlap)
   is automatically maximized."
  [e :- Experiment date :- Long new-distribution :- UnorderedDistribution]
  (modify-raw e date (stabilize new-distribution (latest-distribution e))))

(s/defn ^:always-validate complete :- Experiment
  "Update an experiment with a new final deterministic value"
  [e :- Experiment date :- Long final-value :- Option]
  (modify-raw e date [[final-value (normalization (latest-distribution e))]]))
