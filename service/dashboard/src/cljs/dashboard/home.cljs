(ns dashboard.home
  (:require-macros
   [om-tools.core :refer [defcomponent]]
   [cljs.core.async.macros :refer [go go-loop]])
  (:require
   [clojure.string :as str]
   [schema.utils :as utils]
   [om.core :as om :include-macros true]
   [om-tools.dom :as dom :include-macros true]
   [d3-tools.core :as d3]
   [tubes.core :as t]
   [dommy.core :as dommy]
   [cljs.core.async :as async :refer [<!]]
   [cljs-http.client :as http]
   [cljs-http.util :as http-util]
   [cljs-request.core :as cljs-request]
   [schema.core :as s]))

(defn fetch-full-data []
  (http/request
   {:uri "/data"
    :headers {"Content-Type" "application/edn"}
    :method :get}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Components

(defn setup-scale [scale minx maxx miny maxy]
  (-> scale
      (.domain #js [minx maxx])
      (.range #js [miny maxy])))

(def +colors+ ["#EA5506" "#2CA9E1" "#9DB200" "coral" "olive" "black" "chartreuse" "darkblue" "aqua" "gray" "hotpink"])

(defn path-str [data xscale yscale]
  (when (seq data)
    (str "M " (str/join "L " (for [[x y] data] (str (xscale x) " " (yscale y) " "))))))

(defn last-full-datum [data]
  (let [[last-ts :as last-datum] (last data)
        day-ms 86400000]
    (if (= last-ts (* day-ms (.floor js/Math (/ (.getTime (js/Date.)) day-ms))))
      (->> data (drop-last 1) last)
      last-datum)))

(def d3-time-format*
  "A copy of d3's time format fn, which isn't exposed."
  (.multi js/d3.time.format
          (clj->js
           [[".%L" #(.getMilliseconds %)]
            [":%S" #(.getSeconds %)]
            ["%I:%M" #(.getMinutes %)]
            ["%I %p" #(.getHours %)]
            ["%a %d" #(and (not= (.getDay %) 0) (not= (.getDate %) 1))]
            ["%b %d" #(not= (.getDate %) 1)]
            ["%B" #(.getMonth %)]
            ["%Y" (constantly true)]])))

(defn d3-time-format-utc [date]
  (let [d (js/Date. date)]
    (d3-time-format* (js/Date. (+ date (* 60 1000 (.getTimezoneOffset (js/Date. date))))))))

(defcomponent timeseries-graph [series owner opts]
  (init-state [_]
    {:mouse-pos nil
     :active? false})
  (render-state [_ state]
    (let [{:keys [mouse-pos active?]} state
          w (:width opts 800)
          h (:height opts 400)
          h-axis-space 30
          v-axis-space 30
          legend-space 75
          ymax (- h v-axis-space)
          xmax (- w h-axis-space legend-space)
          all-data (mapcat val series)
          xscale (let [xs (map first all-data)]
                   (setup-scale (.scale (.-time js/d3)) (apply min xs) (apply max xs) 0 xmax))
          yscale (setup-scale
                  (.linear (.-scale js/d3))
                  0 (* 1.05 (apply max (map second all-data)))
                  ymax 0)
          primary-series (some #{:total} (keys series))
          focus-x (if mouse-pos
                    (apply min-key #(.abs js/Math (- (first mouse-pos) (xscale %))) (map first all-data))
                    (->> series vals (map (comp first last-full-datum)) (apply max)))]
      (dom/div
       ;; Reactive class names don't work within svg.
       {:class (if active? "active" "inactive")}
       (dom/svg
        {:width w
         :height h
         :on-mouse-move (fn [e]
                          (let [{:keys [left top]} (dommy/bounding-client-rect (.-currentTarget e))
                                cx (- (.-clientX e) left h-axis-space)
                                cy (- (.-clientY e) top)]
                            (om/set-state! owner :active? true)
                            (om/set-state!
                             owner :mouse-pos
                             (when (and (<= 0 cx xmax) (<= 0 cy ymax)) [cx cy]))))
         :on-mouse-out #(do (om/set-state! owner :mouse-pos nil)
                            (om/set-state! owner :active? false))}
        (dom/g
         {:transform (utils/format* "translate(%s,%s)" h-axis-space 0)}
         (dom/g
          {:class "rule"}
          (let [nxticks (/ w 120)]
            (for [tick (.ticks xscale nxticks)
                  :let [x (xscale tick)
                        formatter (.tickFormat xscale nxticks)]]
              (dom/g {:class "xrule"}
                     (dom/line {:x1 x :y1 0 :x2 x :y2 ymax})
                     (dom/text {:x x :y (+ 15 ymax)} (formatter tick)))))
          (for [tick (.ticks yscale (/ h 50))
                :let [y (yscale tick)
                      formatter (.format js/d3 "s")]]
            (dom/g {:class "yrule"}
                   (dom/line {:x1 0 :y1 y :x2 xmax :y2 y})
                   (dom/text {:x -3 :y (+ y 5)} (formatter tick)))))
         (dom/g
          {:class "xrule"}
          (dom/text
           {:class "serieslabel"
            :x (+ xmax (/ legend-space 2)) :y (+ 15 ymax)}
           (d3-time-format-utc focus-x)))
         (for [[i [label data]] (->> series
                                     (sort-by #(apply max (map second (second %))))
                                     reverse
                                     (map vector (range)))
               :let [[focus-x focus-y] (first (filter #(= (first %) focus-x) data))
                     color (+colors+ i)]]
           (dom/g
            {:class (if (or (not primary-series) (= label primary-series)) "primary" "secondary")}
            (dom/path
             {:class "line"
              :fill "none"
              :stroke color
              :d (path-str data xscale yscale)})
            (dom/circle
             {:class "line"
              :cx (xscale focus-x)
              :cy (yscale focus-y)
              :r 3
              :fill color
              :stroke color
              :stroke-width 0.5})
            (dom/text
             {:class "serieslabel"
              :x (+ xmax (/ legend-space 2)) :y (+ 10 (* 45 i))}
             (name label))
            (dom/text
             {:class "seriespoint"
              :x (+ xmax (/ legend-space 2)) :y (+ 35 (* 45 i))
              :fill color}
             (let [rounded (.round js/Math focus-y)]
               (if (< rounded 1000)
                 (str rounded)
                 ((.format js/d3 ".3s") rounded))))))))))))

(defcomponent titled-graph [series owner opts]
  (render [_]
    (dom/div {:class "graphtitles"}
             (dom/h4 (:title opts))
             (om/build timeseries-graph series {:opts (dissoc opts :title)}))))

(defn grid [& rows]
  (dom/table
   (for [row rows]
     (dom/tr (for [x row] (dom/td x))))))

(defn top-nav-item [text url]
  (dom/span
   {:class "topnavitem"}
   (dom/a {:href url} text) " "))

(defcomponent app [data owner]
  (render [_]
    (dom/div
     {:class "wrapper"}
     (dom/div
      {:class "topnav"}
      (for [k [:retention :notification :experiments :admin]]
        (top-nav-item (name k) (str "/" (name k))))
      (top-nav-item "ranker-comparison" "/admin/ranking-metrics/compare?days=2"))
     (if (empty? data)
       (dom/h3 "Loading...")
       (dom/div
        {:class "content"}
        (grid
         [(om/build titled-graph {:crashes (:hockey-crashes data)}
                    {:opts {:title "hockey crashes" :width 900 :height 200}})
          (om/build titled-graph (:real-time-history data)
                    {:opts {:title "real-time user counts" :width 900 :height 200}})])
        (apply grid
               (for [client ["iphone" "web" "android"]]
                 (cons
                  (om/build titled-graph (get-in data [:signups client])
                            {:opts {:title (str "signups on " client)
                                    :width 450 :height 200}})
                  (for [period [:daily :weekly :monthly]]
                    (om/build titled-graph (get-in data [:active-users client period])
                              {:opts {:title (str (name period) " active users on " client)
                                      :width 450 :height 200}}))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public & Export

(defn ^:export init []
  (let [data (atom {})
        target (.-body js/document)]
    (om/root app data {:target target})
    (go-loop []
      (reset! data (:body (<! (fetch-full-data))))
      (<! (async/timeout (* 1000 60)))
      (recur))))
