(ns classify.online
  "Generic code for online learning infrastructure, model and task-independent"
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [schema.core :as s]
   [plumbing.math :as math]
   [plumbing.parallel :as parallel]
   [flop.distribution :as distribution]
   [flop.stats :as stats]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: Schemas

(s/defschema Action s/Any)
(s/defschema Observation s/Any)
(s/defschema PolicyData
  "Arbitrary data returned from policy's action method, to e.g. keep track of which
   arm should be updated."
  s/Any)

(s/defrecord Step
    [observation :- Observation;; somewhat unconventionally for POMDP, observation comes before action.
     action :- Action
     data :- PolicyData
     ^double reward])

(s/defrecord ActionWithData
    [action :- Action
     data :- PolicyData])

(defprotocol PPolicy
  (action [this last-observation]
    "Returns an action, or an ActionWithData.")
  (update-policy [this ^Step step])
  (split! [this] "clone this, but making a new RNG that should be independent of the parent."))

(s/defschema Policy
  (s/protocol PPolicy))

(defrecord Outcome
    [environment
     ^double reward])

(defprotocol Environment
  (terminal? [this] "is environment in a terminal state?")
  (observation [this] "observation in current state")
  (actions [this] "actions legal in current state")
  (outcome [this action] "return a pair [new-env reward]"))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: environment implementations

(s/defschema State s/Any)

(s/defrecord SequentialEnvironment
    [states :- [State]
     observation-fn :- (s/=> Observation State)
     actions-fn :- (s/=> [Action] State)
     reward-fn :- (s/=> double State Action)]
  Environment
  (terminal? [this] (empty? states))
  (observation [this] (observation-fn (first states)))
  (actions [this] (actions-fn (first states)))
  (outcome [this action]
    (Outcome.
     (SequentialEnvironment. (next states) observation-fn actions-fn reward-fn)
     (reward-fn (first states) action))))

(defn sequential-environment
  "A simple environment that moves through a preset sequence of states regardless
   of action taken."
  [states observation-fn actions-fn reward-fn]
  (SequentialEnvironment. states observation-fn actions-fn reward-fn))

(defn online-labeling-environment
  "Environment from a sequence of labeled data."
  [reward-fn labeled-data]
  (sequential-environment
   (vec labeled-data)
   #(nth % 0)
   ::dunno
   (fn [datum action]
     (reward-fn action (nth datum 1)))))

(defn classical-bandit-environment
  "A classical bandit where each arm determines revenue by independent draws from some
   unknown distribution."
  ([r arms] (classical-bandit-environment r arms Long/MAX_VALUE))
  ([r arms len]
     (sequential-environment
      (range len)
      (constantly nil)
      (constantly (keys arms))
      (fn [_ a] (distribution/sample (safe-get arms a) r)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: evaluation framework

(defn run-step
  "Run a step, returning [step [new-env new-policy]]"
  [[env policy]]
  (let [obs (observation env)
        act (action policy obs)
        [act data] (if (instance? ActionWithData act)
                     [(.action ^ActionWithData act) (.data ^ActionWithData act)]
                     [act nil])
        outcome ^Outcome (outcome env act)
        step (Step. obs act data (.reward outcome))]
    [step [(.environment outcome) (update-policy policy step)]]))

(defn run-steps
  "Run up to max-steps eagerly, returning [steps [new-env new-policy]]"
  [^long max-steps [env policy]]
  (loop [env env policy policy steps (transient []) max-steps max-steps]
    (if (or (terminal? env) (zero? max-steps))
      [(persistent! steps) [env policy]]
      (let [[step [new-env new-policy]] (run-step [env policy])]
        (recur new-env new-policy (conj! steps step) (dec max-steps))))))

(defn epoch-stats
  "Run, calling stats-fn on [steps [env policy]] every epoch and returning a sequence of
   results."
  [env policy stats-fn epoch-size]
  (when-not (terminal? env)
    (let [[steps [new-env new-policy :as world]] (run-steps epoch-size [env policy])]
      (cons (stats-fn steps world)
            (lazy-seq (epoch-stats new-env new-policy stats-fn epoch-size))))))

(defn all-epoch-stats
  "Run epoch-stats in parallel over a map of policies, returning a map from policy
   to sequence of epoch stats."
  [env policies stats-fn epoch-size]
  (parallel/map-vals-work
   nil
   (fn [policy]
     (vec (epoch-stats env policy stats-fn epoch-size)))
   policies))

(defn replicate-policy [n policy]
  (for-map [i (range n)] i (split! policy)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: stats-fns to be passed to (all-)epoch-stats

(defn derived-continuous-stats-fn
  "Fn to be passed to epoch-stats for continuous bandits."
  [action->double]
  (fn [steps _]
    (let [actions (mapv #(action->double (:action %)) steps)]
      {:n (count steps)
       :action-mean (math/mean actions)
       :action-var (math/var actions)
       :entropy (-> actions frequencies-fast math/normalize-vals vals math/entropy)
       :reward-mean (->> steps (map :reward) math/mean)
       :reward-var (->> steps (map :reward) math/var)})))

(def continuous-stats-fn (derived-continuous-stats-fn identity))

(defn action-quantiles-fn
  [steps _]
  (let [q [0.0 0.01 0.05 0.25 0.50 0.75 0.95 0.99 1.0]]
    (map-keys
     #(keyword (str "action-" %))
     (zipmap q (math/quantiles (mapv :action steps) q)))))

(defn ->keyword [x]
  (if (keyword? x)
    x
    (keyword (str x))))

(defn discrete-posterior-stats-fn [_ [_ policy]]
  (for-map [[a dist] (safe-get policy :arms)
            :let [pmd (distribution/posterior-mean-distribution dist)]
            [spec f] {"mm" distribution/mean "ms" #(Math/sqrt (distribution/variance %))}]
    (keyword (str (name (->keyword a)) "_" spec))
    (f pmd)))

(defn discrete-action-stats-fn [steps _]
  (->> steps (map :action) frequencies-fast math/normalize-vals (map-keys ->keyword)
       (merge {:reward (sum :reward steps)})))

(defn merged [& stats-fns]
  (fn [steps ep]
    (->> stats-fns
         #(% steps ep)
         (apply merge))))

(set! *warn-on-reflection* false)
