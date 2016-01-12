(ns model-explorer.handlers
  (:use plumbing.core)
  (:require
   [clojure.java.jdbc.deprecated :as jdbc]
   [clojure.set :as set]
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.html-gen :as html-gen]
   [plumbing.logging :as log]
   [plumbing.map :as map]
   [plumbing.math :as math]
   [plumbing.new-time :as new-time]
   [plumbing.parallel :as parallel]
   [plumbing.rank :as rank]
   [store.bucket :as bucket]
   [web.handlers :as handlers]
   [model-explorer.core :as model-explorer]
   [model-explorer.models :as models]
   [model-explorer.query :as query]
   [model-explorer.templates :as templates])
  (:import
   [plumbing.rank ScoredItem]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helpers

(defn query
  [data where-fn score-fn]
  (->> data
       (parallel/map-work 10 32 #(when (query/matches-where? where-fn %)
                                   (ScoredItem. % (score-fn %))))
       (remove nil?)
       (sort rank/scored-inverse-comparator)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Supporting handlers (assets, etc)

(defnk $ping$GET
  {:responses {200 String}}
  []
  {:body "pong"})

(defnk $GET
  {:responses {302 String}}
  []
  (handlers/url-redirect "/ad-impression/data"))

(defnk $assets$:**$GET
  {:responses {200 s/Any}}
  [[:request [:uri-args ** :- String]]]
  (handlers/resource-response #"\.(js|css)$" (str "model-explorer/assets/" **)))

(defnk $assets$style.css$GET
  {:responses {200 String}}
  []
  {:headers {"content-type" "text/css"}
   :body templates/css})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; REST handlers for js AJAX

(comment
  (defnk $backup$POST
    "Backup all tables to new timestamped versions."
    {:responses {200 String}}
    [[:resources connection-pool]]
    (let [all-tables ["tweet" "tweeter" "model" "relations"]
          ts (millis)]
      (doseq [t all-tables
              :let [new-t (str ts "_" t)]]
        (jdbc/with-connection connection-pool
          (jdbc/do-commands
           (format "CREATE TABLE %s like %s" new-t t )
           (format "INSERT INTO %s SELECT * FROM %s;" new-t t))))
      {:body (format "Backup of %s complete." (pr-str all-tables))})))

(s/defschema Label
  (s/both String (s/pred seq 'seq)))

(defnk $:datatype$label$POST
  {:responses {200 String}}
  [[:request
    [:uri-args datatype :- s/Keyword]
    [:body ids :- [String] label :- Label]]
   [:resources training-data]]
  (model-explorer/label! (safe-get training-data datatype) ids label {:source "explorer" :date (millis)})
  {:body (format "Added label %s!" label)})

(defnk $:datatype$unlabel$POST
  {:responses {200 String}}
  [[:request
    [:uri-args datatype :- s/Keyword]
    [:body ids :- [String] label :- Label]]
   [:resources training-data]]
  (model-explorer/unlabel! (safe-get training-data datatype) ids label)
  {:body (format "Removed label %s!" label)})

(defnk $:datatype$models$save$POST
  {:responses {200 String}}
  [[:request body :- (map-keys s/optional-key
                               (assoc model-explorer/Model :training-data-params String))]
   [:resources model-store training-data list-data model-graph]]
  (letk [model (-> body
                   (update-in-when [:query] read-string)
                   (update-in-when [:training-data-params] read-string))
         [id data-type] model
         full-model (merge (or (model-explorer/model model-store data-type id)
                               {:model-info {:archived? false}
                                :trainer-params {}
                                :training-data-params {:training-labels {}}})
                           model)]
    (log/infof "Saving %s model with id %s" data-type id)
    (model-explorer/put-model! model-store full-model)
    (log/infof "Retraining %s model with id %s" data-type id)
    (models/retrain! model-graph training-data list-data full-model)
    {:body "Model is saved and retrained."}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; non-AJAX mutating handlers

(defnk $:datatype$label$batch$POST
  {:responses {302 s/Any}}
  [[:request
    [:uri-args datatype :- s/Keyword]
    [:body where :- String label :- Label]
    :as req]
   [:resources training-data list-data]]
  (let [store (safe-get training-data datatype)
        data (query (list-data datatype) (query/query->fn where) (query/sort->fn ":updated"))]
    (log/infof "Labeling %s datums with %s" (count data) label)
    (model-explorer/label! store (map #(safe-get-in % [:item :id]) data) label {:source "explorer-batch" :date (millis) :where where}))
  (templates/redirect-back req))

(defnk $:datatype$label$archive$POST
  {:responses {302 s/Any}}
  [[:request
    [:uri-args datatype :- s/Keyword]
    [:body label :- Label]
    :as req]
   [:resources training-data archive-bucket]]
  (let [store (safe-get training-data datatype)
        data (model-explorer/data store label)]
    (log/infof "Archiving %s datums with %s" (count data) label)
    (bucket/put archive-bucket (str label "-" (millis))
                (for [d data] {:id (safe-get d :id) :label label :label-data (safe-get-in d [:labels label])}))
    (model-explorer/unlabel! store (mapv :id data) label))
  (templates/redirect-back req))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; HTML page handlers

(defnk $experiments$GET
  {:responses {200 String}}
  [[:resources experiments-bucket]]
  (templates/html5
   [:h4 "experiments"]
   [:ul
    (for [k (reverse (bucket/keys experiments-bucket))]
      [:li [:a {:href (str "experiments/" k)} k]])]))

(defnk $experiments$:id$GET
  {:responses {200 String}}
  [[:request [:uri-args id :- String]] [:resources experiments-bucket]]
  (handlers/html-response (bucket/get experiments-bucket id)))


(defn datatype-tag-widgets [training-data current-datatype current-page]
  (let [valid-subpages ["data" "labels" "features" "models"]]
    (list
     (templates/autocomplete-list
      "existing-labels"
      (model-explorer/labels (safe-get training-data current-datatype)))
     [:div {:style {:float "right" :margin-top "10px"}}
      [:input {:type "button"
               :class "btn btn-default btn-xs"
               :value "backup!"
               :onclick (templates/ajax {:url "/backup" :body "{}" :description "backing up all tables"})}]]
     (templates/tab-widget
      (map name (keys training-data))
      #(when-not (= (name current-datatype) %) (format "/%s/%s" % (if ((set valid-subpages) current-page) current-page "data"))))
     (templates/tab-widget
      valid-subpages
      #(when-not (= current-page %) (format "/%s/%s" (name current-datatype) %))))))

(defn feed [list-data datatype
            query-params start limit where order]
  (let [q (query/query->fn where)
        s (query/sort->fn order)
        result-set (query (list-data datatype) q s)]
    [:div
     [:form {:class "row" :action "" :method "get"}
      [:div {:class "col-md-7"}
       [:label "Where"]
       [:textarea {:class "codemirror-clj" :name "where" :cols "70" :rows 3} where]]
      [:div {:class "col-md-4"}
       [:label "Order"]
       [:textarea {:class "codemirror-clj" :name "order" :cols "40" :rows 3} order]]
      [:div {:class "col-md-1"}
       (for [[qp-key qp-val] (dissoc query-params :where :order :start)]
         [:input {:type "hidden" :name (name qp-key) :value (str qp-val)}])
       [:input {:type "submit" :class "form-control"}]]]
     (templates/paginated-list
      start limit #(assoc query-params :start % :limit limit)
      result-set
      (fnk [item score]
        (templates/labeled-datum item)
        #_[:div {:style {:float "left" :width "100%"}}
           [:div {:style {:float "right" :width "100px"}} score (pr-str (q item))]
           (templates/labeled-datum item)]))
     [:hr] ;; batch label
     [:form {:action (format "/%s/label/batch" (name datatype)) :method "post" :style {:display "inline"}}
      [:input {:type :hidden :name "where" :value where}]
      [:input (merge
               (templates/awesomplete-params "existing-labels")
               {:type "text" :name "label" :value ""})]
      [:input {:type "submit"
               :value (format  "BATCH LABEL %s DATUMS" (count result-set))
               :class "form-control"}]]]))

(defnk $:datatype$data$GET
  {:responses {200 String}}
  [[:request
    [:uri-args datatype :- s/Keyword]
    [:query-params {start :- long 0} {limit :- long 50} {where "{}"} {order ":updated"} :as query-params]]
   [:resources training-data list-data]]
  (templates/html5
   (datatype-tag-widgets training-data datatype "data")
   (feed list-data datatype query-params start limit where order)))

(defnk $:datatype$data$:id$GET
  {:responses {200 String}}
  [[:request [:uri-args datatype :- s/Keyword id :- String]]
   [:resources training-data list-data]]
  (let [datum (pci/safe-singleton (filter #(= id (:id %)) (list-data datatype)))
        rels (sort-by first (dissoc datum :id :type :updated :created :datum :note :labels :auto-labels))]
    (templates/html5
     (datatype-tag-widgets training-data datatype "datum")
     (templates/labeled-datum datum)
     [:h5 "linked entities"]
     [:ul {:id "tabs" :class "nav nav-tabs" :data-tabs "tabs"}
      (for [[i [rel]] (indexed rels)]
        [:li
         (when (zero? i) {:class "active"})
         [:a {:href (str "#" (name rel)) :data-toggle "tab"} (name rel)]])]
     [:div {:id "my-tab-content" :class "tab-content"}
      (for [[i [rel data]] (indexed rels)]
        [:div {:class (str "tab-pane" (when (zero? i) " active")) :id (name rel)}
         (for [d data]
           [:div {:class "row"}
            [:div {:class "labeled-datum"}
             (model-explorer/render-html (:type d) (with-meta (:datum d) {:labeled d}))
             [:div {:class "clearfix"}]]])])])))

(defnk $:datatype$labels$GET
  {:responses {200 String}}
  [[:request [:uri-args datatype :- s/Keyword]]
   [:resources training-data]]
  (let [data-store (safe-get training-data datatype)]
    (templates/html5
     (datatype-tag-widgets training-data datatype "labels")
     (html-gen/row-major-table
      {:class "table table-striped"}
      (cons (map #(vector :strong %) ["label" "num-examples" "actions"])
            (for [l (model-explorer/labels data-store)]
              [l
               (templates/link
                "data"
                {:where (format "{:labels #(contains? %% \"%s\")}" l)}
                (count (model-explorer/data data-store l)))
               [:form {:action (format "/%s/label/archive" (name datatype)) :method "post"}
                [:input {:type "hidden" :name "label" :value l}]
                [:input {:type "submit" :value "archive" :class "form-control"}]]]))))))

(defnk $:datatype$features$GET
  {:responses {200 String}}
  [[:request
    [:uri-args datatype :- s/Keyword]
    [:query-params
     {where :- String ""} {label0 :- String ""} {label1 :- String ""}
     {threshold :- long 5}]]
   [:resources training-data list-data]]
  (templates/html5
   (datatype-tag-widgets training-data datatype "features")
   [:div {:class "row"}
    [:form {:action "" :method "get"}
     [:div {:class "col-md-6"}
      [:label "Features"]
      [:textarea {:class "codemirror-clj" :name "where" :cols "70" :rows 3} where]]
     [:div {:class "col-md-5"}
      [:label "Labels"]
      (for [[i v] (indexed [label0 label1])]
        [:input (merge (templates/awesomplete-params "existing-labels")
                       {:type "text" :size 40 :name (str "label" i) :value v :class "form-control"})])]
     [:div {:class "col-md-1"}
      [:input {:type "submit" :class "form-control"}]]]]
   (when (seq where)
     (let [all-data (list-data datatype)
           data (map-from-keys (fn [l] (filter #(contains? (:labels %) l) all-data)) [label0 label1])
           query-fn (query/query->fn where)
           overlap (->> data
                        vals
                        (map (fn->> (map #(safe-get % :id)) set))
                        (apply set/intersection)
                        count)
           total-counts (map-vals count data)
           pfrequencies (fn [xs]
                          (let [b (bucket/bucket {})]
                            (parallel/do-work 1000 nil #(bucket/update b % (fnil inc 0)) xs)
                            (for [[k v] (bucket/seq b)
                                  :when (>= v threshold)]
                              [k v])))
           feature-counts (->> (for [[label datums] data
                                     features (parallel/map-work 100 32 query-fn datums)
                                     [fks fm] features
                                     fk (keys fm)]
                                 [fks fk label])
                               pfrequencies
                               map/unflatten)
           format-percent (fn [n d] (if (= n 0) "0" (format "%3.3f%% (%s)" (/ n d 0.01) n)))
           present-counts (fn [feature-counts]
                            (let [t1 (get total-counts label0)
                                  n1 (get feature-counts label0 0)
                                  t2 (get total-counts label1)
                                  n2 (get feature-counts label1 0)]
                              {label0 (format-percent n1 t1)
                               label1 (format-percent n2 t2)
                               :favors (if (> (/ n1 t1) (/ n2 t2)) label0 label1)
                               :mutual-information (math/mutual-information
                                                    {[0 0] n1
                                                     [0 1] (- t1 n1)
                                                     [1 0] n2
                                                     [1 1] (- t2 n2)})}))
           scale-color (fn [from to scale]
                         (apply format "rgb(%s,%s,%s)" (map #(long (+ (* scale %1) (* (- 1 scale) %2))) from to)))]
       [:div
        [:div {:class "row"}
         [:h3 "Most informative binary features"]
         (str/join "; " (for [[k c] total-counts] (format "%s datums labeled %s" c k)))
         (when (pos? overlap)
           (str "WARNING: %s datums with both labels" overlap))]
        [:hr]
        [:table
         {:width "100%" :class "table"}
         (html-gen/table-rows
          [:feature-type :feature-k label0 label1 :mutual-information]
          (->> (for [[feature-type inner] feature-counts
                     :let [feature-name (query/print-feature-name feature-type)]
                     m (->> inner
                            (parallel/map-work 10 32 #(assoc (present-counts (val %))
                                                        :feature-type feature-name
                                                        :feature-k (pr-str (key %)))))]
                 m)
               (sort-by #(- (double (:mutual-information %))))
               (map (fnk [mutual-information :as row]
                      (assoc row
                        :mutual-information (format "%2.4f" mutual-information)
                        :mi-num mutual-information)))
               (take 10000))
          (fn [e] (if (keyword? e) (name e) [:div {:style {:color (if (= e label0) "#a00" "#0a0")}} e]))
          (fnk [favors mi-num]
            {:style {:background-color (scale-color (if (= favors label0) [256 150 150] [150 256 150]) [256 256 256] (max 0 (min 1 (+ 1.2 (* 0.1 (math/log2 mi-num))))))}}))]]))))

(defnk $:datatype$models$GET
  {:responses {200 String}}
  [[:request
    [:uri-args datatype :- s/Keyword]]
   [:resources training-data model-store]]
  (templates/html5
   (datatype-tag-widgets training-data datatype "models")
   (html-gen/row-major-table
    {:class "table table-striped"}
    (let [dummy-empty-model {:id nil :query "" :training-data-params {} :data-type datatype}]
      (cons (map #(vector :strong %) ["model id" "query" "training-labels" "train"])
            (for [model (conj (vec (model-explorer/all-models model-store datatype))
                              dummy-empty-model)]
              (templates/model-row model)))))))

(defnk $:datatype$models$:model-id$GET
  {:responses {200 String}}
  [[:request
    [:uri-args datatype :- s/Keyword model-id :- String]
    [:query-params {start :- long 0} {limit :- long 50}
     {label :- String ""}
     {where "{}"} {order (format ":auto-labels (get \"%s\" 0)" label)} :as query-params]]
   [:resources training-data model-store list-data model-graph]]
  (templates/html5
   (datatype-tag-widgets training-data datatype "model")
   (letk [[date [:train-info train-evaluation evaluation] model-info] (models/get-model model-graph datatype model-id)]
     [:div
      [:div {:style {:float "right" :margin-top "10px"}}
       [:button {:type "button"
                 :class "btn btn-default"
                 :value "Retrain!"
                 :onclick (str
                           (templates/notification :info "Retraining ... page will reload when done.")
                           (templates/ajax
                            {:url (format "/%s/models/save" (name datatype))
                             :body {:data-type (name datatype) :id model-id}
                             :on-success "function() { location.reload(); }"
                             :description "retraining"}))}]]
      [:center
       [:h2 (str "model: " model-id)]
       (str (new-time/pretty-ms-ago date))]
      [:div {:align "center"}
       (templates/confusion-table evaluation train-evaluation model-id)]
      [:div "Labels: "
       (for [lbl (models/labels model-info)]
         (templates/link "" (assoc query-params :label lbl) lbl))]])
   (feed list-data datatype query-params start limit where order)))


(set! *warn-on-reflection* false)
