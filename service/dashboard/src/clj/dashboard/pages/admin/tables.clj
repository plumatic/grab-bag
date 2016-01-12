(ns dashboard.pages.admin.tables
  (:use plumbing.core)
  (:require
   [plumbing.logging :as log]
   [store.mongo :as mongo]
   [service.observer :as ps-observer]))


(defn agg-data [log-data-store coll max-days]
  (let [first-day (ps-observer/rel-day (* -1000 60 60 24 max-days))
        _         (log/infof "Generating table for %s %s %s"
                             coll first-day max-days)
        data (mongo/query
              (mongo/bucket-collection log-data-store coll)
              {:query {:day {"$gte" (str first-day)}}})]
    (log/infof "Got %s datums, postprocessing" (count data))
    data))

(defn nested-group [keyseq maps]
  (if (empty? keyseq)
    (do (assert (= 1 (count maps)))
        (first maps))
    (map-vals
     #(nested-group (next keyseq) %)
     (group-by (first keyseq) maps))))

(defn spanwrap [h rows]
  (for [[i r] (indexed rows)]
    (if (zero? i)
      (format "<td rowspan='%s'>%s</td>%s" (count rows) (name h) r)
      r)))

(defn merge-slaves [data]
  (for [[[s] g] (group-by (fn [m]
                            [(let [s ^String (:service m)
                                   i (.indexOf s "-i-")]
                               (if (pos? i)
                                 (.substring s 0 i)
                                 s)) (:name m) (:key m) (:day m)])
                          data)]
    (assoc (first g)
      :service s
      :sum (reduce + (map :sum g))
      :count (reduce + (map :count g)))))

(defn metrics-table [data]
  (let [data (merge-slaves data)
        head-ks [:service :name :key]
        days    (->> data (map :day) set sort)]
    (str
     "<table border='1' cellspacing='0' cellpadding='4' bordercolor='#ccc'>"
     "<tr>"
     (apply str (for [k (concat head-ks days)] (str "<th>" (name k) "</th>")))
     "</tr>"
     (->>
      (for [[s ms] (sort-by key (nested-group [:service :name :key :day] data))]
        (spanwrap
         (name s)
         (apply concat
                (for [[k ms] (sort-by key ms)
                      :when (not (= k "machine"))]
                  (let [totals (when-not (or (#{"pubsub" "newsfeed-docs"} k)
                                             (.endsWith ^String k "stats"))
                                 (map-from-keys
                                  (fn [d] (sum #(:sum (get % d) 0) (vals ms)))
                                  days))
                        fmt-val (if totals
                                  (fn [m k]
                                    (let [d (get m k)]
                                      (if (:count d)
                                        (format "%s (%2.1f %%)" (:sum d) (/ (:sum d) 0.01 (totals k)))
                                        "")))
                                  (fn [m k]
                                    (let [d (get m k)]
                                      (if (:count d)
                                        (format "%s (%2.1f avg)" (:sum d) (/ (:sum d) 1.0 (:count d)))
                                        ""))))]
                    (spanwrap
                     (name k)
                     (concat
                      (when totals [(apply str (for [v (cons "total" (map totals days))] (str "<th>" v "</th>")))])
                      (for [[v dayd] (sort-by #(- (reduce max 0 (map :sum (vals (val %))))) ms)]
                        (str
                         "<td>" v "</td>"
                         (apply str (for [d days] (str "<td>" (fmt-val dayd d) "</td>"))))))))))))
      (apply concat)
      (map (partial format "<tr>%s</tr>"))
      (apply str))
     "<table>")))
