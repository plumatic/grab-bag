(ns model-explorer.templates
  "Html templates"
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.json :as json]
   [web.handlers :as handlers]
   [garden.core :as garden]
   [garden.units :as units]
   [plumbing.math :as math]
   [plumbing.parallel :as parallel]
   [plumbing.new-time :as new-time]
   [plumbing.time :as time]
   [web.data :as web-data]
   [classify.classify :as classify]
   [model-explorer.core :as model-explorer]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Shared page structure

(def css
  (garden/css
   [[:.nav-tabs
     {:margin {:bottom (units/px 24)}}]

    [:.labeled-datum
     {:padding [[(units/px 24)]]
      :border-bottom "1px solid #eee"}

     [:.awesomplete
      {:display "block"}]

     [:.datum-label
      {:margin {:top (units/px 3)}}]]

    [:.CodeMirror
     {:height (units/px 50)
      :border "1px solid #ccc"
      :border-radius (units/px 3)}]]))

(defn html5 [& body]
  (handlers/html5
   [:head
    [:link {:rel "stylesheet" :href "http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css"}]
    [:script {:src "https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js"}]
    [:script {:src "http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"}]

    ;; notifications
    [:script {:src "/assets/notify.min.js"}]

    ;; autocomplete for labels
    [:script {:src "https://cdn.rawgit.com/LeaVerou/awesomplete/75834f2e6b9130a9bc1d1b732b71eafaca9ae72b/awesomplete.min.js"}]
    [:link {:rel "stylesheet" :href "https://cdn.rawgit.com/LeaVerou/awesomplete/75834f2e6b9130a9bc1d1b732b71eafaca9ae72b/awesomplete.css"}]
    ;; syntax highlighting
    [:script {:src "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.5.0/codemirror.min.js"}]
    [:script {:src "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.5.0/mode/clojure/clojure.min.js"}]
    [:link {:rel "stylesheet" :href "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.5.0/codemirror.min.css"}]
    [:script
     "console.log(\"Loading code mirror...\");
$(function() {
  $('.codemirror-clj').each(function(i, el) {
    console.log(\"Loading code mirror on\", el);
    CodeMirror.fromTextArea(el, {mode: \"clojure\"});
  })
})"]
    [:link {:rel "stylesheet" :href "/assets/style.css"}]]
   [:body
    [:div {:class "container"}
     body]]))

(s/defn raw-notification
  "JS to pop up a notification.  see http://notifyjs.com/, uses notify.min.js above."
  [level :- (s/enum :success :info :warn :error)
   js :- String]
  (format "$.notify(%s, '%s');" js (name level)))

(s/defn notification [level text]
  (raw-notification level (format "\"%s\"" (.replace text "\"" "\\\""))))

(defn this->that [f]
  (format "(function() {that = this; %s})();" f))

(defnk ajax
  [url
   {body nil}
   {request-method (if body :post :get)}
   {description ""}
   {on-success (format "function(data,textStatus,jqXHR) { %s }"
                       (notification :success (str "Succeeded " description)))}
   {on-error (format "function(djqXHR,textStatus,errorThrown) { %s }"
                     (notification :error (str "Failed " description)))}]
  (format
   "$.ajax({type: '%s',
            url: '%s',
            data: %s,
            contentType: 'application/json; charset=utf-8',
            success: %s,
            error: %s});"
   (.toUpperCase (name request-method))
   url
   (when body (if (string? body) body (str "'" (json/generate-string body) "'")))
   on-success
   on-error))

(defn interpolate-js
  "Take a piece of data with placeholder strings, jsonify and replace with values in
   the map.  Allows generating Javascript object constructors that include code."
  [template values]
  (->> values
       (reduce
        (fn [^String s [k v]]
          (.replace s (pr-str k) v))
        (json/generate-string template))
       (format "JSON.stringify(%s)")))

(defn link [uri qps body]
  [:a {:href (str uri "?" (web-data/map->query-string qps))} body])

(defn redirect-back [req]
  (handlers/url-redirect (get-in req [:headers "referer"])))

(defn datum-url [type id]
  (format "/%s/data/%s" (name type) (web-data/url-encode id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Widgets

(defn glyphicon
  [icon]
  [:span {:class (str "glyphicon glyphicon-" (name icon)) :aria-hidden "true"}])

(def notification-test-widget
  "Widget to show off the notifications."
  (for [level [:success :info :warn :error]]
    [:a {:href "#" :onclick (notification level (name level))} (name level)]))

(defn tab-widget
  "Simple widget to switch between pages.  click-url returns nil for current page."
  [pages click-url]
  [:ul {:class "nav nav-tabs"}
   (for [p pages]
     (let [u (click-url p)]
       [:li (when-not u {:class "active"}) [:a {:href (or u "#")} p]]))])

(defn page-widget
  "Widget for paging through 'items'.  returns [html-widget data-subset]."
  [start limit new-start->new-params items]
  (let [datum-link (fn [n] (str "?" (web-data/map->query-string (new-start->new-params n))))]
    [[:div (format "(%s items) Page:" (count items))
      (let [cp (quot start limit)
            np (quot (+ (count items) (dec limit)) limit)
            pgs (->> (for [oom [1 10 100 1000 10000]
                           i (range (* -2 oom) (* 3 oom) oom)]
                       (min (dec np) (max 0 (+ cp i))))
                     distinct
                     sort)]
        (for [[p nextp] (partition-all 2 1 pgs)]
          [:span
           (if (= p cp)
             [:strong (str p)]
             [:a {:href (datum-link (* p limit))}
              (str p)])
           (when (and nextp (not= nextp (inc p))) "...")]))]
     (->> items
          (drop start)
          (take limit))]))

(defn paginated-list [start limit new-start->new-params all-data render-row-fn]
  (let [[pager data] (page-widget start limit new-start->new-params all-data)]
    [:div
     [:hr]
     pager
     [:hr]
     (mapv render-row-fn data)
     [:hr]
     pager]))

(defn autocomplete-list [list-name options]
  [:datalist
   {:id list-name}
   (for [t options]
     [:option t])])

(defn awesomplete-params [list-name]
  ;; for some reason, have to press exs before enter.
  {:class "awesomplete form-control"
   :list list-name
   :data-minchars "1"
   :data-maxitems "20"
   :data-autofirst "true"})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Item templates

(defn nice-twitter-date [d]
  (new-time/pretty-ms (- (millis) (new-time/parse-date "EEE MMM dd HH:mm:ss ZZZZ yyyy" d))))

(defn tweeter [tweeter]
  (letk [[id id_str name screen_name profile_image_url
          location description created_at
          followers_count friends_count
          favourites_count statuses_count lang
          time_zone protected] tweeter]
    [:div {:class "col-md-4"}
     [:div {:class "media"}
      [:div {:class "media-left"}
       [:a {:href (datum-url :tweeter id_str)}
        [:img
         {:src profile_image_url :class "img-rounded" :width 48 :style {:float "left" :margin-right "5px"}}]]]
      [:div {:class "media-body"}
       [:div
        [:a {:href (str "https://twitter.com/" screen_name) :target "_blank"}
         [:strong name]]
        [:span {:class "text-muted"}
         (str "@" screen_name)]
        (when protected
          [:span
           {:class "label label-default"}
           (glyphicon :lock)
           "PROTECTED"])]
       [:div {:class "text-muted"}
        [:ul {:class "list-unstyled"}
         (for [[icon text] [[:time (nice-twitter-date created_at)]
                            [:user (format "%s followers, %s following" followers_count friends_count)]
                            [:comment (str statuses_count " tweets")]
                            [:map-marker (str/join " / " (remove str/blank? [location time_zone]))]]
               :when (not (str/blank? text))]
           [:li [:small (glyphicon icon) text]])]]
       [:small description]]]]))

(defn tweet [tweet]
  (letk [[id id_str {retweeted_status nil} user retweet_count favorite_count created_at text] tweet]
    [:div {:class "tweet-item"}
     (tweeter user)
     [:div {:class "col-md-5"}
      [:div {:class "well well-sm" :style {:font-family "sans-serif" :font-size "16px" :cursor "pointer"}
             :onclick (format "window.location='%s'" (datum-url :tweet id_str))}
       text]
      [:ul
       {:class "list-inline text-muted"}
       [:li
        [:a {:href (format "https://twitter.com/%s/status/%d" (safe-get user :screen_name) id)
             :class "text-muted"
             :target "_blank"}
         (glyphicon :time)
         (nice-twitter-date created_at)]]
       [:li
        {:class (str (when retweeted_status "text-success"))}
        (glyphicon :retweet)
        retweet_count]
       [:li
        (glyphicon :star)
        favorite_count]]]
     [:div {:class "clearfix"}]]))

(defn truncate-url [raw-url] (.toLowerCase ^String (subs raw-url 0 (min 255 (count raw-url)))))

(defn url [url-datum]
  (letk [[url] url-datum]
    [:div
     [:a {:href (datum-url :url (truncate-url url))}
      url
      (when-let [shares (:linkedfrom (:labeled (meta url-datum)))]
        [:span {:style {:font-size "10px" :font-color "#444"}}
         (format "(%s shares)" (count shares))])]
     [:a {:href url} (glyphicon :link)]]))

(defn ad-impression [ad-impression]
  (letk [[id url referer user-id user-device user-os user-browser supports-cookies? geo date tag-id size] ad-impression]
    [:div
     [:a {:href (datum-url :ad-impression id)}
      url referer user-id user-device user-os user-browser supports-cookies? (pr-str geo) date tag-id (pr-str size)]]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Higher-level templates

;; TODO: clean up this javascript mess.
(defn labeled-datum [ld]
  (letk [[id type datum labels auto-labels] ld
         input-id (str "input" id)]
    [:div {:class "row"}
     [:div {:class "labeled-datum"}
      [:div {:class "col-md-3"}
       [:input (merge
                (awesomplete-params "existing-labels")
                {:id input-id :type "text" :name "label" :size "25"
                 :onkeypress (format "if (event.keyCode == 13) %s;"
                                     (ajax
                                      {:url (format "/%s/label" (name type))
                                       :body (interpolate-js
                                              {:ids [id] :label "$l"}
                                              {"$l" "this.value"})
                                       :on-success (format "function(data,textStatus,jqXHR) { %s ; e = document.getElementById('%s'); n = document.createElement('SPAN'); n.className = 'data-label'; n.appendChild(document.createTextNode(e.value));  e.parentNode.appendChild(n); e.value = '';}"
                                                           (raw-notification :success "data")
                                                           input-id)
                                       :description "adding label" } ;;raw "'adding label ' + this.value"
                                      ))})]
       (for [l (keys labels)
             :let [div-id (str "label" id l)]]
         [:button
          {:id div-id
           :class "datum-label data-label btn btn-info btn-xs"
           :onclick (ajax
                     {:url (format "/%s/unlabel" (name type))
                      :body {:ids [id] :label l}
                      :on-success (format "function(data,textStatus,jqXHR) { %s ; e = document.getElementById('%s'); e.parentNode.removeChild(e);}"
                                          (raw-notification :success "data")
                                          div-id)
                      :description (str "removing label " l)})}
          l (glyphicon :remove)])
       (for [[l v] auto-labels]
         [:button {:class "datum-label auto-label btn btn-default btn-xs"
                   :type "button"
                   :onclick (format "document.getElementById('%s').value = '%s';" input-id l)}
          (format "%s (%2.2f)" l v)])]
      (model-explorer/render-html type (with-meta datum {:labeled ld}))
      [:div {:class "clearfix"}]]]))

(defn model-row [model]
  (letk [[id data-type] model
         div-id (format "retrain-button-%s" id)
         id-fn (fn [field] (str div-id "_" (name field)))
         elem (fn [field]
                (let [data (safe-get model field)]
                  [:input
                   {:id (id-fn field)
                    :style {:width "100%"}
                    :value (pr-str data)}]))]
    [(if id
       [:span [:a {:href (format "/%s/models/%s" (name data-type) id)} id]]
       (elem :id))
     (elem :query)
     (elem :training-data-params)
     (let [button-id (str div-id "_button")]
       [:button
        {:class "retrain btn btn-default"
         :type "button"
         :id button-id
         :onclick
         (str
          (format "e = document.getElementById('%s'); e.innerHTML = 'Training...'; e.enabled=false;" button-id)
          (ajax
           {:url (format "/%s/models/save" (name data-type))
            :body (interpolate-js
                   {:data-type data-type
                    :training-data-params "$tdp"
                    :id (or id "$id")
                    :query "$q"}
                   {"$q" (format "document.getElementById('%s').value" (id-fn :query))
                    "$id" (format "document.getElementById('%s').value" (id-fn :id))
                    "$tdp" (format "document.getElementById('%s').value"
                                   (id-fn :training-data-params))})
            :on-success (format "function(data,textStatus,jqXHR) { %s ; e = document.getElementById('%s'); e.innerHTML = 'Trained';}"
                                (raw-notification :success "data")
                                button-id)
            :on-error (format "function() { %s ; e = document.getElementById('%s'); e.innerHTML = 'Save and Train!';}"
                              (notification :error "Failed to train.")
                              button-id)
            }))}
        "Save and Train!"])]))



(def +table-style+
  {:border "1" :cellspacing "0" :cellpadding "5"})

(s/defn confusion-table
  [evaluation :- classify/ClassificationEvaluation
   train-evaluation :- classify/ClassificationEvaluation
   tag :- String]
  (let [labels (-> evaluation keys aconcat distinct sort)]
    [:table
     +table-style+
     (cons
      [:tr {:border "0"}
       (for [h (concat [:gold "# training examples"]
                       (map str labels))]
         [:td
          {:style {:border "none"}}
          [:div
           {:style {:height "200px"
                    :width "30px"
                    :white-space "nowrap"
                    :position "relative"
                    :z-index "-100"
                    :transform "translate(75px, 150px) rotate(315deg)"}}
           [:strong (name h)]]])]
      (for [gold labels]
        [:tr
         (let [[raw-confusions :as both-confusions] (for [data [evaluation train-evaluation]]
                                                      (->> labels
                                                           (keep (fn [guess]
                                                                   (when-let [v (get data [gold guess])]
                                                                     [guess v])))
                                                           (into {})))
               [norm-confusions norm-train-confusions] (for [conf both-confusions]
                                                         (->> conf
                                                              classify/confusion-weights
                                                              math/normalize-vals))]
           (concat
            (for [s [gold (str (long (sum second (mapcat val raw-confusions))))]]
              [:td [:strong s]])
            (for [l labels]
              (let [conf (get norm-confusions l 0)
                    percent (format "%3.0f%%" (/ conf 0.01))]
                [:td {:style {:text-align "right"}}
                 (if (= l gold)
                   [:font {:color "red"} percent]
                   (when (pos? conf) [:font {:color "black"} percent]))
                 (when-let [train-conf (get norm-train-confusions l)]
                   [:div {:style {:font-size "50%" :font-color "#cccccc"}}
                    (format "(%3.0f%%)" (/ train-conf 0.01))])]))))]))]))
