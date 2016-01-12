(ns cljs-request.core
  (:require
   [clojure.string :as str]
   [dommy.core :as dommy :include-macros true]))

(defn map->query-string
  "{:foo \"bar\" :lorem \"ipsum\"} => foo=bar&lorem=ipsum"
  [query-params]
  (->> query-params
       (sort-by first)
       (map  (fn [[k v]]
               (str (name k) "=" (js/encodeURIComponent v))))
       (str/join "&")))

(defn query-string->map
  "foo=bar&lorem=ipsum => {:foo \"bar\" :lorem \"ipsum\"}"
  [query-string]
  (->> (str/split query-string #"&")
       (map (fn [kv] (let [[k v] (str/split kv #"=")]
                       (when k [(keyword k) (js/decodeURIComponent v)]))))
       (into {})))

(defn parse-url
  [url]
  (let [a (-> (dommy/create-element "a")
              (dommy/set-attr! "href" url))
        absolute? (>= (.indexOf url "//") 0)
        query-string (-> a .-search (str/replace "?" ""))]
    (merge
     (when absolute?
       {:protocol (-> a .-protocol (str/replace ":" ""))
        :host (.-hostname a)
        :port (int (if-not (identical? (.-port a) "") (.-port a) 80))})
     {:absolute? absolute?
      :path (.-pathname a)
      :query-string query-string
      :query-params (query-string->map query-string)
      :hash (-> a .-hash (str/replace "#" ""))})))

(defn build-url
  [{:keys [protocol host port path query-params hash]}]
  (let [query-string (map->query-string query-params)]
    (str
     (when protocol (str protocol "://"))
     host
     (when (and host port) (str ":" port))
     path
     (when-not (str/blank? query-string) (str "?" query-string))
     (when-not (str/blank? hash) (str "#" hash)))))

(defn merge-url [url opts]
  (build-url (merge (parse-url url) opts)))

(defn page-query-params []
  (-> js/window.location.search
      (.substring 1)
      query-string->map))

(def default-request-opts
  {:request-method :get
   :headers {:content-type "application/json"}
   :with-credentials? true
   :async? true})

(defn parse-response-body [body]
  (-> body
      js/JSON.parse
      (js->clj :keywordize-keys true)))

(defn parse-response [xhr]
  (let [status (.-status xhr)
        type (int (/ status 100))]
    {:xhr xhr
     :text (.-responseText xhr)
     :body (-> xhr .-responseText parse-response-body)
     :status status
     :status-type type
     :info (= 1 type)
     :ok (= 2 type)
     :client-error (= 4 type)
     :server-error (= 5 type)
     :error (contains? #{4 5} type)}))

(defn request*
  [opts & [cb]]
  (let [{:keys [request-method headers body
                with-credentials? async?]
         :as opts}
        (merge default-request-opts opts)
        xhr (js/XMLHttpRequest.)
        url (build-url opts)]
    (.open xhr (-> request-method name str/upper-case) url async?)
    (set! (.-withCredentials xhr) with-credentials?)
    (when cb
      (set! (.-onload xhr)
            #(-> xhr parse-response (assoc :request opts) cb)))
    (doseq [[k v] headers]
      (.setRequestHeader xhr (name k) v))
    (.send xhr (when body (-> body clj->js js/JSON.stringify)))
    xhr))

(defn jsonp-request*
  [opts & [cb]]
  (let [fn-name (gensym "fn")
        url (build-url (assoc-in opts [:query-params :callback] fn-name))
        script (-> (dommy/create-element "script")
                   (dommy/set-attr! "src" url))]
    (aset js/window fn-name
          (fn [res]
            (dommy/remove! script)
            (cb (js->clj res :keywordize-keys true))))
    (dommy/append! js/document.head script)))

(defn form-request*
  [form opts & [cb]]
  (let [{:keys [request-method enctype]
         :as opts}
        (merge default-request-opts
               {:enctype "multipart/form-data"}
               opts)
        url (build-url opts)
        frame-name (gensym "frame-")
        frame (-> (dommy/create-element "iframe")
                  (dommy/set-attr! "name" frame-name))]
    (assert form "Must supply a form to form-request")
    (dommy/toggle! frame false)
    (dommy/append! js/document.body frame)
    (dommy/listen-once!
     frame :load
     (fn []
       (-> frame .-contentDocument .-body dommy/text
           parse-response-body
           cb)
       (js/setTimeout
        #(dommy/remove! frame)
        0)))
    (-> form
        (dommy/set-attr!
         :method (-> request-method name str/upper-case)
         :action url
         :target frame-name
         :enctype enctype)
        .submit)))
