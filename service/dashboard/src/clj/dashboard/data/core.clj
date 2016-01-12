(ns dashboard.data.core
  "Shared schemas and data utilities"
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.new-time :as new-time]))

(defn sorted-seq? [s]
  (= s (sort s)))

(s/defschema Timeseries
  (s/both
   [(s/pair long "millis" (s/maybe s/Num) "value")]
   (s/pred #(sorted-seq? (map first %)) 'sorted-seq?)))

(defn mean [xs]
  (when (seq xs)
    (double (/ (sum xs) (count xs)))))

(s/defn ceil-timestamp :- long
  [ms :- long period :- long]
  (-> ms
      dec
      (quot period)
      inc
      (* period)))

(s/defn downsample :- Timeseries
  "Produce a new timeseries with points [ts val] for each timestamp
   ts that is a multiple of period with at least one value in the input
   in the interval just preceding.  The value is the result of calling
   interpolate on the values in the period, by default the mean."
  ([ts :- Timeseries period :- long]
     (downsample ts period mean))
  ([ts :- Timeseries period :- long interpolate :- (s/=> s/Num [s/Num])]
     (for [part (partition-by #(ceil-timestamp (first %) period) ts)]
       [(ceil-timestamp (ffirst part) period)
        (interpolate (map second part))])))

(defn utc-date [& [ms]]
  (new-time/format-date
   "yyyy-MM-dd"
   (if ms (java.util.Date. (long ms)) (java.util.Date.))
   (new-time/utc-calendar)))
