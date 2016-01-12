(ns dashboard.pages.retention
  (:use plumbing.core)
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [garden.core :as garden]
   [hiccup.page :as page]
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.parallel :as parallel]
   [web.handlers :as handlers])
  (:import
   [java.text SimpleDateFormat]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schems

(s/defschema GroupAttributes {s/Keyword s/Str})

(s/defschema GroupMetrics {s/Keyword s/Int})

(s/defschema GroupedMetrics {GroupAttributes GroupMetrics})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private helpers

(defn round-date-sql [arg]
  (let [week 604800
        offset 345600]
    (format "FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(%s) - %s) / %s) * %s + %s)"
            arg offset week week offset)))

(defn- cohort-week [column-name]
  (format "(case when created > '2014-05-01' then %s else -1 end)"
          (round-date-sql column-name)))

(defn active-user-metrics-query [db-env]
  [(format
    (str/join
     "\n"
     ["SELECT simple_client_type AS client_type, cohort_week, week, "
      "       count(*) as n_users, "
      "       count(case when engagement>=6 then 1 else NULL end) as d6_7,"
      "       count(case when engagement>=3 and engagement<6 THEN 1 else NULL end) as d3_5,"
      "       count(case when engagement>=1 and engagement<3 THEN 1 else NULL end) as d1_2 FROM"
      "  (SELECT by_engagement.*, %s as cohort_week"
      "   FROM (SELECT *, %s as week,"
      "                COUNT(*) as engagement,"
      "                (case client_type when 'iphone_browser' then 'web' else client_type end) as simple_client_type "
      "         FROM analysis_%s.user_feed_fetch_daily_summary",
      "         WHERE date_ts > '2014-05-01'",
      "   GROUP BY user_id, simple_client_type, week) AS by_engagement",
      "   LEFT JOIN datawarehouse_%s.users ON user_id=users.id",
      "  ) AS grouped_by_user",
      "GROUP BY simple_client_type, cohort_week, week;"])
    (cohort-week "created")
    (round-date-sql "date_ts")
    (name db-env) (name db-env))])


(def +earlier-cohort+ "-1")

(s/defn grouped-metrics :- GroupedMetrics
  [active-user-metrics-data]
  (let [attr-keys [:d1_2 :d3_5 :d6_7 :n_users]
        cohort_week-fmt (SimpleDateFormat.  "yyyy-MM-dd HH:mm:ss")
        cohort_week->time #(if (= % +earlier-cohort+) -1 (.getTime (.parse cohort_week-fmt %)))]
    (for-map [metrics active-user-metrics-data]
      (-> (apply dissoc metrics attr-keys)
          (update-in [:cohort_week] cohort_week->time)
          (update-in [:week] #(.getTime %)))
      (select-keys metrics attr-keys))))

(def retention-app-css
  [[:.rotate90
    ^:prefix {:transform "rotate(-90deg)"
              :transform-origin "50% 50%"}]
   [:.cohort-waterfall
    {:position "relative"}
    [:.axis {:font-weight :bold}]

    [:.has-selection
     [:td {:opacity "0.7"}]
     [:td.selected {:opacity "1"}]]

    [:td.selected
     {:border-color :black
      :border-width "1px"}]
    [:.axis.selected
     {:background-color "#eee"}]

    [:td :th {:border "1px solid #ddd"}
     [:&.x-axis {:line-height "1em"
                 :height "3em"}]
     [:&.y-axis {:border-right-width "2px"
                 :border-left :none}]

     [:&.empty {:border :none}]

     [:&.gutter0 {:border-width "0 2px 2px 0"
                  :border-color "#ddd"}]]]])


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Handlers and resources

(def retention-data
  (graph/instance parallel/refreshing-resource [db-env dws-db-spec]
    {:secs 300
     :f (fn [] (jdbc/query dws-db-spec (active-user-metrics-query db-env)))}))

(defnk $data$GET
  {:responses {200 s/Any}}
  [[:resources retention-data]]
  (handlers/edn-response (grouped-metrics @retention-data)))

(defnk $GET
  {:responses {200 s/Any}}
  []
  (handlers/html-response
   (page/html5
    [:head
     (page/include-css "//netdna.bootstrapcdn.com/bootstrap/3.1.1/css/bootstrap.min.css")
     [:style (garden/css
              {:vendors ["webkit" "moz" "o"]}
              retention-app-css)]
     [:script {:src "/js/dashboard.js"}]]
    [:body.row {:onload "dashboard.retention.init();"}])))
