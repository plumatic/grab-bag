(ns dashboard.pages.home
  (:use plumbing.core)
  (:require
   [clojure.java.jdbc :as jdbc]
   [garden.core :as garden]
   [garden.stylesheet :as stylesheet :refer [at-import]]
   [garden.units :as units :refer [percent px]]
   [hiccup.page :as page]
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.math :as math]
   [plumbing.new-time :as new-time]
   [plumbing.parallel :as parallel]
   [plumbing.template :as template]
   [web.handlers :as handlers]
   [dashboard.data.core :as data]
   [dashboard.data.google-analytics :as google-analytics]
   [dashboard.data.hockey-crashes :as hockey-crashes]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Data schemas

(s/defschema Client (s/enum "web" "iphone" "android"))

(def engagement-cutoffs
  {:daily {:period 1 :engagement-cutoffs []}
   :weekly {:period 7 :engagement-cutoffs [1 2 6 8]}
   :monthly {:period 30 :engagement-cutoffs [1 2 5 15 31]}})

(defn engagement-buckets [cutoffs]
  (assoc (for-map [[from to] (partition 2 1 cutoffs)]
           (keyword (format "d%s-%s" from (dec to)))
           [from to])
    :total [0 Long/MAX_VALUE]))

(s/defschema ActiveUsers
  {Client
   (map-vals
    (fnk [engagement-cutoffs]
      (map-vals (constantly data/Timeseries) (engagement-buckets engagement-cutoffs)))
    engagement-cutoffs)})

(s/defschema Signups
  {Client {(s/enum :total :onboarded) data/Timeseries}})

(s/defschema HomeData
  {:hockey-crashes data/Timeseries
   :real-time-history google-analytics/RealTimeHistory
   :active-users ActiveUsers
   :signups Signups})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Queries

(def UserID long)
(def ActiveDay long)

(s/defschema ActiveUsersQueryRow
  {:user_id UserID
   :date ActiveDay
   :client Client})

(defn active-users-query [db-env since-ts]
  [(template/interpolate
    "dashboard/sql/daily_active_users.sql"
    {:env (name db-env)
     :since (data/utc-date since-ts)})])

(s/defn ^:always-validate active-users :- [ActiveUsersQueryRow]
  [dws-db-spec db-env since-ts]
  (jdbc/query dws-db-spec (active-users-query db-env since-ts)))


(s/defschema CreatedUsersQueryRow
  {:user_id UserID
   :created_date long
   :client Client})

(defn created-users-query [db-env since-ts]
  [(template/interpolate
    "dashboard/sql/created_users.sql"
    {:env (name db-env)
     :since (data/utc-date since-ts)})])

(s/defn ^:always-validate created-users :- [CreatedUsersQueryRow]
  [dws-db-spec db-env since-ts]
  (jdbc/query dws-db-spec (created-users-query db-env since-ts)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Data munging

(s/defschema ByUser
  {UserID {:active-days #{ActiveDay} :primary-client Client}})

(s/defn by-user :- ByUser
  "Collect SQL results per user"
  [active-users :- [ActiveUsersQueryRow]]
  (->> active-users
       (group-by :user_id)
       (map-vals (fn [user-rows]
                   {:active-days (->> user-rows (map :date) set)
                    :primary-client (->> user-rows (map :client) math/mode)}))))

(s/defschema ByActiveDays
  {Client [(s/pair ActiveDay "day" #{UserID} "users")]})

(defn missing-days
  "Return a sequence of dates representing days missing in the input sequence"
  [dates]
  (let [day-ms (new-time/to-millis 1 :day)]
    (doseq [d dates] (assert (zero? (mod d day-ms))))
    (remove (set dates)
            (range (apply min dates)
                   (apply max dates)
                   day-ms))))

(s/defn by-active-days :- ByActiveDays
  [by-user :- ByUser]
  (->> by-user
       (group-by #(:primary-client (val %)))
       (map-vals (fn [users]
                   (->> (for [[id {:keys [active-days]}] users
                              day active-days]
                          [day id])
                        (group-by first)
                        (map-vals #(set (map second %)))
                        ((fn [data]
                           (merge data
                                  (map-from-keys (constantly #{}) (missing-days (keys data))))))
                        (sort-by first))))))

(s/defn rolling-active-days :- [(s/pair ActiveDay "day" {UserID (s/named long 'count)} "data")]
  [client-active-days
   period :- long]
  (let [add (fn [m u] (update-in m [u] #(inc (or % 0))))
        sub (fn [m u] (if (= (m u) 1) (dissoc m u) (update-in m [u] dec)))
        sorted (sort-by first client-active-days)
        [lead more] (split-at period sorted)]
    (when (= (count lead) period)
      (reductions
       (fn [[_ prev] [[_ old] [date new]]]
         [date (as-> prev x (reduce sub x old) (reduce add x new))])
       [(first (last lead)) (frequencies (mapcat second lead))]
       (map vector client-active-days more)))))

(s/defn client-active-users
  [client-active-days
   period :- long
   engagement-cutoffs :- [long]
   max-days :- long]
  (let [rolling (rolling-active-days client-active-days period)]
    (map-vals
     (fn [[from to]]
       (for [[date user-counts] (take-last max-days rolling)]
         [date (long (count-when #(<= from % (dec to)) (vals user-counts)))]))
     (engagement-buckets engagement-cutoffs))))

(s/defn ^:always-validate active-users-data :- {:by-user ByUser :active-users ActiveUsers}
  [active-user-rows :- [ActiveUsersQueryRow] max-days :- long]
  (let [by-user (by-user active-user-rows)]
    {:by-user by-user
     :active-users
     (map-vals
      (fn [client-active-days]
        (map-vals
         (fnk [period engagement-cutoffs]
           (client-active-users client-active-days period engagement-cutoffs max-days))
         engagement-cutoffs))
      (by-active-days by-user))}))

(defn round-day [ms]
  (let [day-ms (new-time/to-millis 1 :day)]
    (* day-ms (quot ms day-ms))))

(s/defn ^:always-validate signups-data :- Signups
  [by-user :- ByUser created-rows :- [CreatedUsersQueryRow] max-days :- long]
  (->> created-rows
       (group-by :client)
       (map-vals (fn [rows]
                   (let [ids-by-day (->> rows
                                         (group-by #(round-day (:created_date %)))
                                         (map-vals (partial map :user_id)))
                         with-missing (->> (missing-days (keys ids-by-day))
                                           (map-from-keys (constantly []))
                                           (merge ids-by-day)
                                           sort
                                           (take-last max-days))]
                     (map-vals (fn [count-fn]
                                 (for [[d ids] with-missing]
                                   [d (count-when count-fn ids)]))
                               {:total (constantly true)
                                :onboarded #(contains? by-user %)}))))))

(def +max-data-days+
  "How many days to show data for"
  30)

(s/defn home-data
  [db-env dws-db-spec]
  (letk [[by-user active-users] (active-users-data
                                 (active-users dws-db-spec db-env (new-time/time-ago 62 :days))
                                 +max-data-days+)]
    {:hockey-crashes (hockey-crashes/crashes-last-month)
     :active-users active-users
     :signups (signups-data
               by-user (created-users dws-db-spec db-env (new-time/time-ago 31 :days)) +max-data-days+)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Handlers

(def colors
  {:background :#202121
   :link :#7F7F80
   :text :#C0C6C9})

(def home-css
  [(at-import "http://fonts.googleapis.com/css?family=Open+Sans:400,700,600,300")

   [:body
    {:background-color (:background colors)
     :margin 0}]

   [:table
    {:height (percent 70)
     :width (percent 100)}]

   [:td
    {:padding (px 0)
     :vertical-align "top"}]

   [:a:link
    {:color (:link colors)}]

   [:a:visited
    {:color (:link colors)}]

   [:a:hover
    {:color :#ECF0F2}]

   [:.content
    {:padding (px 40)
     :margin-top (px 0)}]

   [:.topnav
    {:font-family "Open Sans"
     :background-color (:background colors)
     :border-bottom {:width (px 1)
                     :style "solid"
                     :color :#404142}
     :padding (px 20)}]

   [:.graphtitles
    {:width (percent 100)}]

   [:.wrapper
    {:height (percent 40)}]

   [:.topnavitem
    {:margin-left (px 20)
     :color :#AEB1B2}]

   [:h3
    {:font {:size (px 28)
            :family "Open Sans"
            :weight 700}
     :color (:text colors)
     :letter-spacing (px -0.5)}]

   [:h4
    {:font {:size (px 28)
            :family "Open Sans"
            :weight 600}
     :color (:text colors)
     :letter-spacing (px 0)}]

   [:.primary
    {:stroke-width (px 2)
     :font {:weight "normal"}}]

   [:.secondary
    {:stroke-width (px 1)}]

   [:.inactive
    [:.secondary
     {:opacity 0.3}]
    [:.primary
     [:path
      {:opacity 0.5}]]
    [:.rule
     {:opacity 0.3}]]

   [:.rule
    [:line
     {:stroke :#3F3F3F
      :shape-rendering "crispEdges"
      :stroke-width (px 1)}]]

   [:text.serieslabel
    {:fill (:text colors)}]

   [:.yrule
    [:text
     {:font-family "Open Sans"}
     {:fill (:text colors)}
     {:text-anchor "end"}]]

   [:.xrule
    [:text
     {:font-family "Open Sans"}
     {:fill (:text colors)}
     {:text-anchor "middle"}]]

   [:text.serieslabel
    {:text-anchor "middle"
     :font {:family "Open Sans"}}]

   [:.serieslabel
    {:font {:family "Open Sans"}}]

   [:text.seriespoint
    {:text-anchor "middle"
     :font {:size (px 30)
            :family "Open Sans"
            :weight 700}}]])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(def home-graph
  {:real-time-history google-analytics/real-time-users-history-graph
   :real-time-summary (graph/instance parallel/refreshing-resource [real-time-history]
                        {:secs 60
                         :f (fn []
                              (map-vals
                               #(take-last
                                 (* +max-data-days+ 24)
                                 (data/downsample % (new-time/to-millis 1 :hour)))
                               (google-analytics/raw-timeseries real-time-history)))})
   :home-data (graph/instance parallel/refreshing-resource [db-env dws-db-spec]
                {:secs 600
                 :f (fn [] (home-data db-env dws-db-spec))})})

(defnk home-data-response [home-data real-time-summary :as home-graph]
  (handlers/edn-response
   (s/validate HomeData (assoc @home-data :real-time-history @real-time-summary))))

(def home-response
  (handlers/html-response
   (page/html5
    [:head
     [:style (garden/css
              {:vendors ["webkit" "moz" "o"]}
              home-css)]
     [:script {:src "/js/dashboard.js"}]]
    [:body.row {:onload "dashboard.home.init();"}])))
