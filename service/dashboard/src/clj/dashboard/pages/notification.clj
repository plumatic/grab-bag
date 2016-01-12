(ns dashboard.pages.notification
  "Dashboard for notifications"
  (:use plumbing.core)
  (:require
   [clojure.java.jdbc :as jdbc]
   [hiccup.page :as page]
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.html-gen :as html-gen]
   [plumbing.parallel :as parallel]
   [plumbing.template :as template]
   [web.data :as data]
   [web.handlers :as handlers]
   [dashboard.utils :as utils]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: helpers

(defn notification-table-simple [rows]
  [:section
   [:div.table-responsive
    [:table.table.table-striped.table-bordered
     [:thead
      [:tr
       (for [col-label ["week" "category" "service" "campaign"  "sent" "unique recipients"
                        "unique opens" "unique home clicks" "users engaged" "users clicking unsubscribe"]]
         [:th col-label])]]
     [:tbody
      (html-gen/row-major-rows
       (map (juxt :week :category :service :notification_campaign :sent :unique_users
                  (fn [row] (utils/format-with-percent (:unique_opens row) (:unique_users row)))
                  (fn [row] (utils/format-with-percent (:unique_home_clicks row) (:unique_users row)))
                  (fn [row] (utils/format-with-percent (:unique_engagement_clickers row) (:unique_users row)))
                  (fn [row] (utils/format-with-percent (:unique_unsubscribe_clickers row) (:unique_users row))))
            rows))]]]])

;; TODO(mk) This just slaps the notificaiton campaign into the table. When we have more experiments
;; running, we'll have to do something about it.
(defn notification-table [rows]
  [:section
   [:div.table-responsive
    [:table.table.table-striped.table-bordered
     [:thead
      [:tr
       (for [col-label ["week" "category" "service" "campaign"  "sent",
                        "clicked-emails" "clicked-emails-confidence" "unique recipients" "unique visitors"
                        "home-clicks" "unsubscribe-clicks"
                        "story-clicks" "interest-clicks" "profile-clicks" "explore-clicks"  ]]
         [:th col-label])]]
     [:tbody
      (html-gen/row-major-rows
       (map (juxt :week :category :service :notification_campaign :sent
                  (fn [row] (utils/format-with-percent (:clicked_emails row) (:sent row)))
                  (fn [row] (utils/format-conf-interval (:clicked_emails row) (:sent row)))
                  :unique_emails
                  (fn [row] (utils/format-number-and-percent (:unique_clicking_users row) (:percent_unique_clicking_users row)))
                  (fn [row] (utils/format-with-percent (:home_clicks row) (:sent row)))
                  (fn [row] (utils/format-with-percent (:unsubscribe_clicks row) (:sent row)))
                  (fn [row] (utils/format-with-percent (:story_clicks row) (* 5 (:sent row))))
                  (fn [row] (utils/format-with-percent (:interest_clicks row) (:sent row)))
                  (fn [row] (utils/format-with-percent (:profile_clicks row) (:sent row)))
                  (fn [row] (utils/format-with-percent (:explore_clicks row) (:sent row))))
            rows))]]]])


(defn subscription-table [rows]
  [:section
   [:div.table-responsive
    [:table.table.table-striped.table-bordered
     [:thead
      [:tr
       (for [col-label ["week" "category" "sub/unsub"
                        "ALL" "reply" "udpate" "digest" "social" "comment-on-post" "at-mention"]]
         [:th col-label])]]
     [:tbody
      (html-gen/row-major-rows
       (map (juxt :week :category :sub
                  :_all_ :reply :_update_ :digest :social :comment_on_post :at_mention) rows))]]]])

(defn- next-url [qp this-query]
  (let [rem-str (data/map->query-string (dissoc qp this-query))]
    (format "window.location='?%s%s=' + encodeURIComponent(this.value);"
            (if (empty? rem-str) "" (str rem-str "&"))
            (name this-query))))

(defn- subset [experiment category row]
  (and (or (= experiment "all")
           (= (:notification_campaign row) experiment))
       (= category (:category row))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: resources and handlers

(def notification-data
  (graph/instance parallel/refreshing-resource [dws-db-spec]
    {:secs 60
     :f (fn []
          {:notification (jdbc/query dws-db-spec [(template/interpolate "dashboard/sql/notifications.sql" {})])
           :subscription (jdbc/query dws-db-spec [(template/interpolate "dashboard/sql/subscriptions.sql" {})])
           :notification-simple (jdbc/query dws-db-spec [(template/interpolate "dashboard/sql/notifications-simple.sql" {})])})}))

(defnk $GET
  {:responses {200 s/Any}}
  [[:request [:query-params {category "digest"} {experiment "all"} :as query-params]]
   [:resources notification-data]]
  (handlers/html-response
   (page/html5
    [:head
     (page/include-css "//netdna.bootstrapcdn.com/bootstrap/3.1.1/css/bootstrap.min.css")
     (page/include-js
      "//ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js"
      "//netdna.bootstrapcdn.com/bootstrap/3.1.1/js/bootstrap.min.js")
     ]
    [:body
     [:h3 "Categories"]
     [:select
      {:onchange (next-url query-params :category)}
      (for [category (->> notification-data :notification (map :category) distinct)]
        [:option (merge {:value category}
                        (when (= cat category) {:selected "selected"}))
         category])]


     [:h3 "Experiment"]
     [:select
      {:onchange (next-url query-params :experiment)}
      (for [campaign (cons "all" (->> notification-data
                                      :notification
                                      (filter #(= cat (:category %)))
                                      (map :notification_campaign)
                                      distinct))]
        [:option (merge {:value campaign}
                        (when (= campaign experiment) {:selected "selected"}))
         campaign])]

     [:div
      [:h3 "Notifications Simple"]
      (->> notification-data
           :notification-simple
           (filter (partial subset experiment cat))
           notification-table-simple)]

     [:div
      [:h3 "Notifications"]
      (->> notification-data
           :notification
           (filter (partial subset experiment cat))
           notification-table)
      [:h3 "Subscription Toggles"]
      (-> notification-data :subscription subscription-table)]])))
