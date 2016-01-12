(ns dashboard.pages.admin.logging
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [plumbing.new-time :as new-time]
   [store.mongo :as mongo]
   [service.logging :as service-logging]
   [service.observer :as service-observer]
   [dashboard.pages.admin.tables :as tables])
  (:import
   [org.apache.commons.lang StringEscapeUtils]))


(defn log-table-data [log-data-store max-days level-query]
  (let [first-day (service-observer/rel-day (* -1000 60 60 24 max-days))]
    (mongo/query
     (mongo/bucket-collection log-data-store "agg-log")
     {:fields (concat [:_id :latest.time :date :count :latest.log.message :latest.exception.message] (next service-logging/+log-agg-keys+))
      :query (merge
              {:date {"$gte" (str first-day)}}
              level-query)})))

(defn flatten-replicas [log-table-data]
  (->> log-table-data
       (map #(update-in % [:service] service-logging/base-service-name))
       (group-by #(select-keys % (next service-logging/+log-agg-keys+)))
       vals
       (map #(apply max-key (comp :time :latest) %))))

(defn nested-table [outer-ks inner-f data]
  (if-let [[k & more-ks] (seq outer-ks)]
    (apply concat
           (for [[kv subdata] (sort-by key (group-by k data))]
             (tables/spanwrap (or kv "") (nested-table more-ks inner-f subdata))))
    [(str/join " " (map #(str "<td>" % "</td>") (inner-f data)))]))

(defn trim [^String msg len]
  (if (> (.length msg) len)
    (str (.substring msg 0 len) "...")
    msg))

(defn build-hover [id d]
  (let [msg (str/join "<br/>" (map #(trim % 30)
                                   (remove empty? (map str [(-> d :log :message) (-> d :exception :message)]))))]
    (format "<a title=\"%s\" href=\"details?table=agg-log&id=%s\">%s</a>"
            (StringEscapeUtils/escapeHtml (with-out-str (pprint/pprint d)))
            id
            msg)))

(def +latest-ks+ [:latest :last-message])
(defn latest-cols [id d]
  [(new-time/pretty-ms-ago (:time d))
   (build-hover id d)])

(defn make-log-table [data]
  (let [ks (next service-logging/+log-agg-keys+)
        days (->> data (map :date) distinct sort)
        data (map
              #(update-in % [:location] (partial service.logging/github-linkify-location (:service %)))
              data)]
    (str "<table border='1' cellspacing='0' cellpadding='4' bordercolor='#ccc'>"
         "<tr>"
         (apply str (for [k (concat ks days +latest-ks+)] (str "<th>" k "</th>")))
         "</tr>"
         (->> data
              (nested-table
               ks
               (fn [data]
                 (let [g (group-by :date data)]
                   (concat
                    (map #(-> g (get %) first (get :count 0)) days)
                    (let [le (->> g (sort-by key) last val first)]
                      (latest-cols (:_id le) (:latest le)))))))
              (map (partial format "<tr>%s</tr>"))
              (apply str))
         "</table>")))

(defn level-query [m]
  (when-let [l (get
                {"warn" ["warn" "error" "fatal"]
                 "error" ["error" "fatal"]}
                (get m :min-level))]
    {:level {"$in" l}}))

(defn fix-agg-entry [e]
  (merge (dissoc e :latest) (:latest e)))
