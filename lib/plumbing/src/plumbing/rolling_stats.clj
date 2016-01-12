(ns plumbing.rolling-stats
  (:use plumbing.core)
  (:require
   [plumbing.new-time :as new-time]))

;; need to serialize properly, so we can't use record.

(defn eternal-stats [] {:tx 0 :tn 0})
(defn add-eternal-obs [{:keys [tx tn]} x n]
  {:tx (+ tx x) :tn (+ tn n)})

(defn geometric-stats [p] {:p p :tx 0 :tn 0})
(defn add-geometric-obs [{:keys [p tx tn] :as c} x n]
  (let [en (min n (/ 1 (- 1 p)))
        ep (max 0 (- 1 (* n (- 1 p))))]
    {:p p :tx (+ (* ep tx) (if (zero? n) x (* (/ en n) x))) :tn (+ (* ep tn) en)}))

(defn mean [{:keys [tx tn]}] (/ tx tn))


;; todo: upper confidence bound.


(defn update-rolling-stats [stats x n]
  (-> stats
      (update-in [:eternal] add-eternal-obs x n)
      (update-in [:geometric] add-geometric-obs x n)))

(defn rolling-stats [^double effective-window ^double init-x ^double init-n]
  (update-rolling-stats
   {:eternal (eternal-stats)
    :geometric (geometric-stats (- 1 (/ 1.0 effective-window)))}
   init-x
   init-n))


(defn update-time-series-rolling-stats [stats x now]
  (let [last-update (safe-get stats :last-update)]
    (update-rolling-stats
     (assoc stats :last-update now)
     x (/ (max 0 (- now last-update)) (new-time/to-millis :days)))))

(defn time-series-rolling-stats [effective-window-days init-x init-days now]
  (let [eff-days (min init-days effective-window-days)]
    (assoc (rolling-stats effective-window-days (* init-x (/ eff-days init-days)) eff-days)
      :last-update now)))

;; maybe tweets per day is better than per-article, actdually...
