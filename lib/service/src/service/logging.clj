(ns service.logging
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.tools.logging :as clj-log]
   [plumbing.html-gen :as html-gen]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [store.mongo :as mongo]
   [service.observer :as observer]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: helpers

;; TODO: way to configure target email, either service-wide and/or via :to in log entry.

(def +log-agg-keys+ [:date :service :level :location :type :ex-location])

(defn location [e]
  (when e
    (str (or (:ns e) (:file e)) "_" (:line e))))

(defn log-agg-vals [log-entry]
  [(observer/today)
   (:service log-entry)
   (:level log-entry)
   (location (:log log-entry))
   (or (:type (:log log-entry)) (:type (:exception log-entry)))
   (location (:exception log-entry))])

(defn entry-recorder [entry-atom]
  (fn [e] (swap! entry-atom conj e)))

(defn work-around-recur-in-catch [t e]
  (log/errorf t "Error recording log entries: %s" (pr-str e)))


(defn trim-entry [e]
  (let [trim-str (fn [m ks]
                   (if-let [s (get-in m ks)]
                     (let [s (str s)]
                       (assoc-in m ks (.substring s 0 (min (count s) 10000))))
                     m))]
    (-> e
        (trim-str [:log :message])
        ;; the only unbounded string fields, see plumbing.logging/throwable->map
        (trim-str [:exception :message])
        (trim-str [:exception :data])
        (trim-str [:exception :data-chain]))))

(let [max-entries 10000]
  (defn trim-entries [es]
    (when (> (count es) max-entries)
      (log/errorf "Declaing log bankruptcy; %d errors since last round" (count es)))
    (take max-entries es)))

(defn process-entries!
  "Grab entries from the provided source and apply the provided
  function to their trimmed forms."
  [entry-atom raw? f]
  (let [e (get-and-set! entry-atom nil)]
    (try (when (seq e)
           (f (trim-entries (reverse e))))
         (catch Throwable t
           (if raw?
             (clj-log/errorf t "Error sending email about errors: %s" (pr-str e))
             (work-around-recur-in-catch t e))))))

;; TODO: log bankruptcy, long logs.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: Writing to mongo in aggregate and stream

(defn update-agg-logs
  "Update count and latest in the agg-logs, given these new entries"
  [agg-logs log-entries]
  (doseq [[agg entries] (group-by log-agg-vals log-entries)]
    (assert (= (count agg) (count +log-agg-keys+)))
    (mongo/update agg-logs (zipmap +log-agg-keys+ agg)
                  {:latest (trim-entry (last entries))}
                  {:count (count entries)})))

(defn add-to-stream-log [stream-logs log-entries]
  "Add these entries to our running stream / capped collection"
  (doseq [entry log-entries]
    (mongo/append stream-logs (trim-entry entry))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: github urls

(defn- ns->file
  "Partially cribbed from clojure.core/root-resource"
  [^String n]
  (str (.. n
           (replace \- \_)
           (replace \. \/)) ".clj"))

(defn- class->ns [^String n]
  (when n
    (let [parts (.split n "\\$")]
      (when (and (> (count parts) 1)
                 (= (first parts) (.toLowerCase ^String (first parts))))
        (first parts)))))

(defn base-service-name [^String service-name]
  (.replaceAll service-name "-i-.*" ""))

(defn- deploy-branch [service-name]
  (str "DEPLOYED-" (base-service-name service-name)))

(defn- project-path [^String service-name ^String namespace]
  (let [first-segment (first (.split namespace "\\."))]
    (str
     (cond (.startsWith service-name first-segment) "service"
           (#{"schema" "plumbing" "hiphip" "dommy" "fnhouse"} first-segment) "open-source"
           :else "grabbag")
     "/" first-segment)))

(defn- namespace-path [service-name namespace]
  (when namespace
    (when-let [project-path (project-path service-name namespace)]
      (str project-path "/src/" (ns->file namespace)))))

(defn github-link [service-name namespace line]
  (when-let [path (namespace-path service-name namespace)]
    (format "https://github.com/plumatic/grab-bag/blob/%s/%s%s"
            (deploy-branch service-name) path (if line (str "#L" line) ""))))

(defn github-linkify [service-name namespace line]
  (if-let [link (github-link service-name namespace line)]
    [:a {:href link} line]
    line))

(defn github-linkify-frame [service-name frame]
  (assoc frame :line
         (github-linkify service-name (-> frame :class class->ns) (:line frame))))

(defn- split-last [^String s ^String sub]
  (let [i (.lastIndexOf s sub)]
    (if (>= i 0)
      (remove empty? [(subs s 0 i) (subs s (inc i))])
      [sub])))

(defn github-linkify-location [service-name location]
  (let [[ns line] (split-last location "_")]
    (if-let [link (github-link service-name ns line)]
      (html-gen/render [:a {:href link} location])
      location)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: Error emails

(defn best-frame
  "Try to extract the most informative exception stack trace frame"
  [stack-trace]
  (or (first
       (filter
        (fnk [^String file ^String class]
          (and file (.contains file "clj")
               (not (.startsWith class "clojure."))
               (not (.startsWith class "plumbing.core"))))
        stack-trace))
      (first stack-trace)))

(defn extract-message
  "Extract the best message from a log entry"
  [e]
  (or (not-empty (-> e :exception :message))
      (-> e :log :message)
      ""))

(defn flatten-entry [e]
  (concat (dissoc e :log :exception)
          (for-map [[k v] (:log e)]
            (keyword (str "log_" (name k))) v)
          (for-map [[k v] (dissoc (:exception e) :stack-trace)]
            (keyword (str "ex_" (name k))) v)))

(defn inbox-teaser [sub-entries]
  (str (count sub-entries) ": "
       (->> sub-entries
            (map extract-message)
            (map #(subs % 0 (min 50 (count %))))
            frequencies
            (sort-by (comp - second))
            (map (fn [[m c]] (format "%sx[%s]" c m)))
            (str/join ", "))))

(defn entry-info [e service-name link]
  (let [best-frame (best-frame (-> e :exception :stack-trace))]
    {:log-ns (-> e :log :ns)
     :log-line (github-linkify service-name (-> e :log :ns) (-> e :log :line))
     :ex-class (-> best-frame :class)
     :ex-line (:line (github-linkify-frame service-name best-frame))
     :message [:a {:href link} (extract-message e)]}))

(defn summary-table [service-name indexed-entries]
  [:table
   {:border "1" :cellpadding "5" :cellspacing "0"}
   (html-gen/table-rows
    [:log-ns :log-line :ex-class :ex-line :message]
    (for [[i e] indexed-entries]
      (entry-info e service-name (str "#anchor" i))))])

(defn entry-details [service-name index entry]
  [:div
   (concat
    [[:hr]
     [:a {:name (str "anchor" index)} [:h3 (extract-message entry)]]
     [:table
      {:border "1" :cellpadding "3" :cellspacing "0" :font-size "8px"}
      (html-gen/row-major-rows
       (for [[k v] (sort-by first (flatten-entry entry))]
         [k (if (coll? v) (with-out-str (pprint/pprint v)) v)]))]
     [:br]]
    (when-let [st (-> entry :exception :stack-trace)]
      [[:h3 "stack trace"]
       [:table
        {:border "1" :cellpadding "3" :cellspacing "0" :font-size "6px"}
        (html-gen/table-rows [:class :line :file :method] (map (partial github-linkify-frame service-name) st))]]))])

(defn email-entries
  "Roll these entries together and send emails."
  [service-name send-email error-email entries]
  (doseq [[[email subject] sub-entries]
          (group-by (comp (juxt :error-email :error-email-subject) :log) entries)
          :let [indexed-entries (indexed (sort-by extract-message sub-entries))]]
    (send-email
     {:to (or email error-email)
      :subject (format "%s: %s" service-name (or subject "exceptions"))
      :html? true
      :text (html-gen/render
             [:div
              [:div {:font-size "4px" :font-color "#dddddd"}
               (inbox-teaser sub-entries)]
              [:h2 "Summary"]
              (summary-table service-name indexed-entries)
              [:h2 "Entries"]
              [:div
               (for [[i e] indexed-entries]
                 (entry-details service-name i e))]])})))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(defnk logging-resource [[:instance service-name] log-data-store send-email {flush-freq 60}
                         {error-email "backend+error@example.com"}
                         {agg-level :info} {log-level :warn} {email-level :error}]
  (let [agg-logs (mongo/aggregate-collection log-data-store "agg-log" {:agg-keys +log-agg-keys+})
        stream-logs (mongo/capped-collection log-data-store "log" {:max-mb 5000})
        agg-atom (atom nil) log-atom (atom nil) email-atom (atom nil)
        process (fn []
                  (process-entries! agg-atom false #(update-agg-logs agg-logs %))
                  (process-entries! log-atom false #(add-to-stream-log stream-logs %))
                  (process-entries! email-atom true #(email-entries
                                                      service-name send-email error-email %)))
        exec (parallel/schedule-limited-work
              {:f process
               :submit-rate-ms (* flush-freq 1000)
               :time-limit-ms (* flush-freq 1000)})]
    (log/init!
     service-name
     [[(entry-recorder agg-atom) agg-level]
      [(entry-recorder log-atom) log-level]
      [(entry-recorder email-atom) email-level]])
    (reify resource/PCloseable
      (close [this]
        (try (process) (catch Throwable e (clj-log/errorf e "Error processing final logs")))
        (resource/close exec)))))
