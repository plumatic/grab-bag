(ns classify.online.policies
  "Generic code for online learning infrastructure, model and task-independent"
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.io :as io]
   [plumbing.new-time :as new-time]
   [flop.distribution :as distribution]
   [flop.stats :as stats]
   [classify.online :as online])
  (:import
   [java.util Random]
   [classify.online Step]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Deterministic (non-bandit) policies

(s/defrecord ConstantPolicy [c]
  online/PPolicy
  (action [this datum] c)
  (update-policy [this step] this)
  (split! [this] this))

(defn bounded-conj [q x n]
  (conj (if (= (count q) n) (pop q) q) x))

(defn prefixes [v] (for [i (range (count v) -1 -1)] (subvec v 0 i)))

;; non-random, conditional algorithm based on function of historical steps
(s/defrecord DeterministicHistoryPolicy
    [history-size :- long
     record?
     bucket-fn
     action-fn
     history :- {s/Any clojure.lang.PersistentQueue}]
  online/PPolicy
  (action [this datum]
    (action-fn (take history-size (mapcat history (prefixes (bucket-fn datum))))))
  (update-policy [this step]
    (assoc this
      :history
      (if (record? step)
        (reduce
         (fn [history ks]
           (update history ks
                   (fn [q] (bounded-conj (or q clojure.lang.PersistentQueue/EMPTY) step history-size))))
         history
         (prefixes (bucket-fn (.observation ^Step step))))
        history)))
  (split! [this] this))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Decaying policy adapter

(defprotocol PDecayablePolicy
  (decay [this w]
    "Decay any statistics in the policy by multiplying the effective number of samples by 'w'"))

(s/defschema DecayablePolicy
  (s/conditional #(satisfies? PDecayablePolicy %) online/Policy))

(def ^:const +minute+ (long (new-time/to-millis 1 :min)))
(def ^:const +update-threshold+ +minute+)

(s/defrecord DecayingPolicy
    [rate :- double
     base-policy :- DecayablePolicy
     last-update :- long]
  online/PPolicy
  (action [this datum] (online/action base-policy datum))
  (update-policy [this step]
    (let [t (long (:date (.observation ^Step step)))
          decay? (<= (+ last-update +update-threshold+) t)]
      (DecayingPolicy.
       rate
       (-> base-policy
           (?> decay? (decay (Math/pow rate (/ (double (- t last-update)) +minute+))))
           (online/update-policy step))
       (long (if decay? t last-update)))))
  (split! [this] (update this :base-policy online/split!)))

(s/defn decaying-policy
  "A version of a policy which decays learned information at a fixed
   geometric rate (in a timescale of minutes).  For efficiency,
   decaying is only carried out at most once per minute (but in such a
   way to achieve the desired rate), and only upon update (which may
   be suboptimal if the policy is used in a batched mode).

   Assumes observations have a field :date with unix long timestamp."
  [rate :- double
   policy :- DecayablePolicy]
  (DecayingPolicy. rate policy 0))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Thompson sampling bandits

(s/defn fresh-random :- java.util.Random []
  (plumbing.MersenneTwister. (long (unchecked-int (System/nanoTime)))))

(s/defrecord ThompsonBandit
    [r :- Random
     arms :- distribution/Arms]
  online/PPolicy
  (action [this datum] (distribution/thompson-sample-best r arms))
  (update-policy [this step]
    (update-in this [:arms (.action ^Step step)]
               distribution/update-stats (.reward ^Step step)))
  (split! [this] (ThompsonBandit. (fresh-random) arms))

  PDecayablePolicy
  (decay [this w] (ThompsonBandit. r (map-vals #(distribution/decay % w) arms)))

  io/PDataLiteral
  (to-data [this] (io/schematized-record this)))

(s/defn thompson-bandit
  "A generic bandit that picks the arm with the maximum sampled posterior mean."
  [r :- Random
   arms :- [distribution/Arm]
   prior :- (s/protocol distribution/ConjugatePriorDistribution)]
  (ThompsonBandit. r (for-map [a arms] a prior)))

(s/defn decaying-thompson-bandit
  "Helper function for constructing a decaying thompson bandit."
  [r :- Random
   arms :- [distribution/Arm]
   prior :- (s/protocol distribution/ConjugatePriorDistribution)
   rate :- Double]
  (decaying-policy rate (thompson-bandit r arms prior)))

(defn bandit-best-distribution
  [bandit obs n]
  (let [results (frequencies (repeatedly n #(online/action bandit nil)))]
    (map-from-keys
     #(/ (double (results % 0)) (double n))
     (keys (safe-get bandit :arms)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Simple policy adapters

(s/defrecord BucketPolicy
    [bucket-fn
     base-policy
     sub-policies :- {}]
  online/PPolicy
  (action [this datum]
    (online/action (sub-policies (bucket-fn datum) base-policy) datum))
  (update-policy [this step]
    (update-in this [:sub-policies (bucket-fn (.observation ^Step step))]
               #(online/update-policy (or % base-policy) step)))
  (split! [this]
    (update this :base-policy online/split!)))

(defn bucket-policy
  "A simple conditional policy which maintains a separate copy of base-policy
   for each unique value of bucket-fn."
  [bucket-fn base-policy]
  (BucketPolicy. bucket-fn base-policy {}))


(s/defrecord TransformedPolicy
    [transform-action-fn transform-step-fn base-policy]
  online/PPolicy
  (action [this datum]
    (transform-action-fn datum (online/action base-policy datum)))
  (update-policy [this step]
    (update this :base-policy online/update-policy (transform-step-fn step)))
  (split! [this] (assoc this :base-policy (online/split! base-policy))))

(defn transformed-policy
  "A policy with a functional transformation on actions and steps."
  [transform-action-fn transform-step-fn base-policy]
  (->TransformedPolicy transform-action-fn transform-step-fn base-policy))


(s/defrecord RelativizedPolicy
    [random :- Random
     baseline-p :- double
     baseline-action-fn
     policy
     baseline-stats :- flop.stats.WindowedStats]
  online/PPolicy
  (action [this datum]
    (if (or (not (stats/full? baseline-stats))
            (<= (.nextDouble random) baseline-p))
      (online/->ActionWithData (baseline-action-fn datum) ::baseline)
      (online/action policy datum)))
  (update-policy [this step]
    ;; TODO: ideally we could share stats with the real policy.
    (if (= ::baseline (.data ^Step step))
      (update this :baseline-stats stats/add-windowed-obs (.reward ^Step step))
      (update this :policy online/update-policy
              (update step :reward
                      #(dec (/ % (stats/uni-mean (.stats ^flop.stats.WindowedStats baseline-stats))))))))
  (split! [this]
    (assoc this :random (fresh-random) :policy (online/split! policy)))

  PDecayablePolicy
  (decay [this w]
    (update this :policy decay w)))

(s/defn relativized-policy
  "Learn a main policy on 'rewards' representing zero-centered improvement on a
   sliding window of rewards from a baseline action fn, as recommended by MOE:

   http://yelp.github.io/MOE/objective_functions.html"
  [r :- Random
   baseline-p :- Double
   baseline-window :- Long
   baseline-action-fn
   policy]
  (RelativizedPolicy. r baseline-p baseline-action-fn policy (stats/windowed-stats baseline-window)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Tiered bucket bandits

(s/defrecord TieredThompsonBandit
    [r :- Random
     bucket-fn
     prior
     arms :- {s/Any distribution/Arms}
     smooth-fn]
  online/PPolicy
  (action [this datum]
    (distribution/thompson-sample-best
     r
     (apply pci/merge-all-with smooth-fn prior (map arms (prefixes (bucket-fn datum))))))
  (update-policy [this step]
    (let [step ^Step step]
      (TieredThompsonBandit.
       r
       bucket-fn
       prior
       (reduce
        (fn [arms ks]
          (update-in arms [ks (.action step)]
                     (fn [s] (distribution/update-stats (or s prior) (.reward step)))))
        arms
        (prefixes (bucket-fn (.observation step))))
       smooth-fn)))
  (split! [this]
    (assoc this :r (fresh-random)))

  PDecayablePolicy
  (decay [this w]
    (update this :arms (fn->> (map-vals (fn->> (map-vals (fn-> (distribution/decay w)))))))))

(defn first-n-smoother
  "Take the most specific distribution with at least n effective observations."
  [n]
  (fn [& dists]
    (or (first (filter #(>= (safe-get-in % [:uni-stats :num-obs]) n) dists))
        (last dists))))

(s/defn tiered-thompson-bandit
  "A version of a thompson bandit which learns stats of all prefixes of a bucket-fn,
   and uses a provided smothing function to combine these statistics for
   Thompson sampling."
  [r :- Random
   arms :- [distribution/Arm]
   prior :- (s/protocol distribution/ConjugatePriorDistribution)
   bucket-fn
   smooth-fn]
  (TieredThompsonBandit. r bucket-fn prior {[] (for-map [a arms] a prior)} smooth-fn))

(set! *warn-on-reflection* false)
