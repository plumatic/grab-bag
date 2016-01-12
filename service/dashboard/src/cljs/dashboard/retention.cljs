(ns dashboard.retention
  (:require-macros
   [om-tools.core :refer [defcomponent]]
   [cljs.core.async.macros :refer [go go-loop]])
  (:require
   [clojure.string :as str]
   [om.core :as om :include-macros true]
   [om-tools.dom :as dom :include-macros true]
   [d3-tools.core :as d3]
   [tubes.core :as t]
   [cljs.core.async :as async :refer [<!]]
   [cljs-http.client :as http]
   [cljs-http.util :as http-util]
   [cljs-request.core :as cljs-request]
   [schema.core :as s]))

(defn fetch-full-data []
  (http/request
   {:uri "/retention/data"
    :headers {"Content-Type" "application/edn"}
    :method :get}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Data manipulation

(def +earlier-cohort+ -1) ;; TODO share w/ backend

(defn safe+
  "A merge function that only adds numbers"
  [a b]
  (if (and (number? a) (number? b))
    (+ a b)
    b))

(defn group-full-data [full-data dims]
  (let [dims->ks (juxt #(select-keys % dims) :cohort_week :week)
        merge-fn (partial merge-with safe+)]
    (reduce
     (fn [data [full-dims metrics]]
       (update-in data (dims->ks full-dims) merge-fn metrics))
     {}
     full-data)))

(defn remove-edges [s] (-> s next butlast))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Display helpers

(defn format-percent [num den]
  (if (or (not den) (zero? den)) "0"
      (let [p (* 100 (/ num den))]
        (str (if (< p 10) (t/format "%1.1f" (double p)) (long p)) "%"))))

(defn leading-zero [n]
  (let [n (if (counted? n) (count n) n)]
    (if (< n 10) (str "0" n) (str n))))

(defn format-week [ts]
  (let [d (js/Date. ts)]
    (str (leading-zero (inc (.getMonth d))) "-" (leading-zero (.getDate d)))))

(defn transpose-with
  "Takes items from coll while (pred item) is true and concats to end"
  [pred coll]
  (concat (drop-while pred coll) (take-while pred coll)))

(defn cx [& {:as m}]
  "Returns a string of keys with truthy values joined together by spaces,
   or returns nil when no truthy values. Naming comes from React."
  (when-let [ks (keep #(when (val %) (name (key %))) m)]
    (str/join " " ks)))

(defn on-diag? [pos? [x1 y1 :as c1] [x2 y2 :as c2]]
  (and c1 c2
       (= ((if pos? + -) (- x2 x1)) (- y1 y2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Components

(defcomponent cohort-waterfall [data owner]
  (render-state
    [_ {:keys [mouse-coord]}]
    (let [{:keys [grouped-metrics align-left? stat-key]} data
          cohorts (->> (dissoc grouped-metrics +earlier-cohort+) keys distinct sort remove-edges)
          weeks (->> (vals grouped-metrics) (mapcat keys) distinct sort remove-edges)
          cell-selected? (cond (= :y-axis (first mouse-coord)) #(= (second %) (second mouse-coord))
                               (= :x-axis (second mouse-coord)) #(= (first %) (first mouse-coord))
                               :else #(on-diag? align-left? mouse-coord %))
          cell-width (str (float (/ 100 (inc (count weeks)))) "%")]
      (dom/div
       {:class "table-responsive cohort-waterfall"}

       (dom/div {:class "rotate90"
                 :style  {:position "absolute"
                          :top "48%"
                          :left (str "-" cell-width)}}
                "Cohort Week")
       (dom/table
        {:class (cx "table" true
                    "table-condensed" true
                    "align-left" align-left?
                    "has-selection" (boolean mouse-coord))
         :on-mouse-leave #(om/set-state! owner :mouse-coord nil)}
        (dom/thead
         (dom/tr
          (dom/th {:class "gutter0" :style {:width cell-width}})
          (for [[idx week] (map-indexed vector weeks)]
            (dom/th {:class (cx "text-center axis x-axis" true
                                "selected" (= mouse-coord [:y-axis idx]))
                     :style {:width cell-width}
                     :on-mouse-enter #(om/set-state! owner :mouse-coord [:y-axis idx])}
                    (if (= :y-axis (first mouse-coord))
                      (str "Week " idx)
                      (format-week week))))))
        (dom/tbody
         (for [[x cohort] (map-indexed vector cohorts)
               :let [cohort-label (format-week cohort)
                     first-week (some #(get-in grouped-metrics [cohort %]) weeks)
                     cohort-data (get grouped-metrics cohort)
                     cohort-weeks (if align-left?
                                    (transpose-with #(nil? (get cohort-data %)) weeks)
                                    weeks)
                     color-scale (d3/scale :linear
                                           :domain [0 (apply max
                                                             (* 0.6 (get first-week stat-key))
                                                             (keep (comp stat-key cohort-data)
                                                                   (next cohort-weeks)))]
                                           :clamp true
                                           :range ["hsl(220, 50%, 95%)" "hsl(220, 100%, 60%)"]
                                           :interpolate :hsl)]]
           (dom/tr
            (dom/td {:class (cx "axis y-axis" true
                                "selected" (= mouse-coord [x :x-axis]))
                     :on-mouse-enter #(om/set-state! owner :mouse-coord [x :x-axis])}
                    cohort-label)
            (for [[y week] (map-indexed vector cohort-weeks)]
              (if-let [metrics (get cohort-data week)]
                (let [stat-val (get metrics stat-key)
                      stat-compare-val (get first-week stat-key)
                      coord [x y]
                      selected? (cell-selected? coord)
                      first-week? (= first-week metrics)]
                  (dom/td {:class (cx "text-center" true
                                      "selected" selected?)
                           :style {:backgroundColor (color-scale stat-val)}
                           :on-mouse-enter (when-not (= mouse-coord coord)
                                             #(om/set-state! owner :mouse-coord coord))}
                          (dom/div
                           (dom/div
                            (dom/strong
                             (format-percent stat-val stat-compare-val)))
                           (dom/small (str " (" stat-val ")")))))
                (dom/td {:class "empty"
                         :on-mouse-enter #(om/set-state! owner :mouse-coord nil)})))))))))))

(defcomponent app [data owner]
  (init-state [_]
    {:split-dims #{}
     :align-left? true})

  (render-state [_ {:keys [split-dims align-left?]}]
    (let [split-data (group-full-data data split-dims)]
      (dom/div
       {:class "col-md-10 col-md-offset-1"}
       (dom/h1 "Retention Dashboard")

       (dom/div
        {:class "panel panel-default"}
        (dom/div {:class "panel-heading"}
                 "Split Dimensions")
        (dom/div {:class "panel-body"}
                 (for [k (->> data first key keys (remove #{:cohort_week :week}))
                       :let [checked? (contains? split-dims k)]]
                   (dom/label {}
                              (dom/input {:type "checkbox"
                                          :on-change #(om/update-state!
                                                       owner :split-dims
                                                       (fn [s] ((if checked? disj conj) s k)))
                                          :checked checked?})
                              (str " " (name k))))))

       (dom/div
        (for [[dims data] split-data]
          (dom/div
           (dom/h3 {} (if (seq dims) (pr-str dims) "All"))
           (om/build cohort-waterfall
                     {:grouped-metrics data
                      :align-left? align-left?
                      :stat-key :n_users}))))))))

(defcomponent loading-component [_ _]
  (render [_]
    (dom/div
     {:class "col-md-10 col-md-offset-1"}
     (dom/h1 "Retention Dashboard")
     (dom/h3 "Loading..."))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public & Export

(defn ^:export init []
  (let [full-data-req (fetch-full-data)
        target (.-body js/document)]
    (om/root loading-component {}
             {:target target})
    (go
     (let [full-data (:body (<! full-data-req))]
       (om/root app full-data
                {:target target})))))
