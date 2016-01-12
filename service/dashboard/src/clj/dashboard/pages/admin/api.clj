(ns dashboard.pages.admin.api
  (:use plumbing.core)
  (:require
   [hiccup.page :as page]
   [schema.core :as s]
   [plumbing.html-gen :as html-gen]
   [plumbing.string :as string]
   [store.bucket :as bucket]
   [store.mongo :as mongo]
   [store.snapshots :as snapshots]
   [web.handlers :as handlers]
   [service.logging :as service-logging]
   [dashboard.pages.admin.graphs :as graphs]
   [dashboard.pages.admin.logging :as logging]
   [dashboard.pages.admin.tables :as tables]))

(defnk $GET
  "Root /admin page."
  {:responses {200 String}}
  []
  (handlers/html-response
   (page/html5
    [:head
     [:title "Admin"]
     (apply page/include-css ["/admin/resources/admin.css"])]
    [:body
     {:class "env-desktop"}
     [:div#container
      [:div#expand "fullscreen"]
      [:div#sidebar
       [:h3 "Server Metrics"]
       [:ul
        [:li
         [:a {:data-type "services"} "Services"]]
        [:li
         [:a {:data-param "300" :data-type "graphs"} "Metrics "]
         [:a {:data-param "1440" :data-type "graphs"} "(Day) "]
         [:a {:data-param "10080" :data-type "graphs"} "(Week) "]
         [:a {:data-param "40320" :data-type "graphs"} "(Month)"]]
        [:li
         [:a {:data-param "300" :data-type "machines"} "Machines "]
         [:a {:data-param "1440" :data-type "machines"} "(Day) "]
         [:a {:data-param "10080" :data-type "machines"} "(Week) "]
         [:a {:data-param "40320" :data-type "machines"} "(Month)"]]
        [:li
         [:a {:href "/admin/metrics-table"} "Daily Metrics Table"]]
        [:li
         [:a {:href "/admin/shortener-metrics"} "Shortener Metrics Table"]]]]
      [:div#admin]]
     (for [js ["//ajax.googleapis.com/ajax/libs/jquery/1.8.3/jquery.min.js"
               "https://www.google.com/jsapi"
               "/admin/resources/d3.v2.js"
               "/admin/resources/d3.layout.js"
               "/admin/resources/d3.chart.js"
               "/admin/resources/admin.js"]]
       [:script {:src js
                 :type "text/javascript"}])])))

(defnk $resources$:**$GET
  "Resoures for the root /admin page."
  {:responses {200 s/Any}}
  [[:request [:uri-args **]]]
  (handlers/resource-response #"" (str "dashboard/admin/" **)))


(defnk $sidebar-index$GET
  "Index for the bottom sidebar of the /admin dashboard.  This should just go statically in
   page above now that analytics and dashboard are merged, but for now it's inserted on the client."
  {:responses {200 s/Any}}
  []
  {:body [["User Graphs"
           [["Ranking Metrics" "/admin/ranking-metrics/dashboard"]]]
          ["Server Logs"
           [["Warn Table" "/admin/logs/table?min-level=warn"]
            ["Warn List" "/admin/logs/list"]
            ["Info Table" "/admin/logs/table"]]]]})

(defn sanitize [x]
  (let [atomic? (fn [x] (or (number? x) (keyword? x) (string? x)))]
    (cond (atomic? x) x
          (map? x) (for-map [[k v] x] (if (atomic? k) k (pr-str k)) (sanitize v))
          (coll? x) (map sanitize x)
          :else (str x))))

(defnk $latest-snapshots$GET
  "Latest observer snapshots of a particular service."
  {:responses {200 s/Any}}
  [[:request [:query-params service {limit :- long 3}]]
   [:resources get-snapshot-store]]
  {:body (->> (snapshots/snapshot-seq (get-snapshot-store service))
              (map sanitize)
              (take limit))})

(defnk $snapshots$GET
  "Mapping from all services to latest observer snapshots."
  {:responses {200 s/Any}}
  [[:resources [:admin snapshots-cache]]]
  {:body @snapshots-cache})

(defnk $metrics-table$GET
  "Table of all metrics captured from observer"
  {:responses {200 String}}
  [[:request [:query-params {max-days :- long 7}]]
   [:resources log-data-store]]
  (handlers/html-response
   (tables/metrics-table
    (tables/agg-data log-data-store "agg-metrics" max-days))))


(defn machine? [^String c] (.endsWith c "machine"))

(defnk $metric-graphs$GET
  "Data for time series graphs of metrics (pubsub, etc)"
  {:responses {200 s/Any}}
  [[:request query-params]
   [:resources log-data-store]]
  {:body (graphs/graphs
          log-data-store
          (remove machine? (keys (graphs/list-metrics log-data-store)))
          query-params)})

(defnk $machine-graphs$GET
  "Data for time series graphs of machines (cpu, ram, etc)"
  {:responses {200 s/Any}}
  [[:request query-params]
   [:resources log-data-store]]
  {:body (graphs/graphs
          log-data-store
          (filter machine? (keys (graphs/list-metrics log-data-store)))
          query-params)})


(defnk $logs$table$GET
  "Aggregated table of log entries throughout Grabbag systems"
  {:responses {200 String}}
  [[:request [:query-params {max-days :- long 2} & args]]
   [:resources log-data-store]]
  (->> (logging/level-query args)
       (logging/log-table-data log-data-store max-days)
       logging/flatten-replicas
       logging/make-log-table
       handlers/html-response))

(defnk $logs$list$GET
  "List of raw log entries throughout Grabbag systems"
  {:responses {200 String}}
  [[:request query-params]
   [:resources log-data-store]]
  (handlers/html-response
   (html-gen/render
    [:table
     {:border "1" :cellpadding "5" :cellspacing "0"}
     (html-gen/table-rows
      [:service :level :log-ns :log-line :ex-class :ex-line :message]
      (for [e (mongo/query
               (mongo/bucket-collection log-data-store "log")
               {:query (logging/level-query query-params)
                :fields (concat [:_id :log.ns :log.line :exception.stack-trace :exception.message :log.message]
                                service-logging/+log-agg-keys+)
                :sort {"$natural" -1}
                :limit 100})]
        (merge e
               (update-in (service-logging/entry-info e (:service e) (format "details?table=log&id=%s" (:_id e)))
                          [:message] #(string/truncate-to (or % "") 50)))))])))

(defnk $logs$details$GET
  "Information about a particular exception"
  {:responses {200 String}}
  [[:request [:query-params table id :- String]]
   [:resources log-data-store]]
  (handlers/html-response
   (html-gen/render
    (let [e (logging/fix-agg-entry (bucket/get (mongo/bucket-collection log-data-store table)
                                               (org.bson.types.ObjectId. id)))]
      (service-logging/entry-details (:service e) 0 e)))))
