(ns web.client
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.error :as err]
   [plumbing.json :as json]
   [plumbing.logging :as log]
   [html-parse.parser :as parser]
   [web.data :as data])
  (:import
   [java.io InputStream]
   [java.net URI]
   [java.util Scanner]
   [java.util.concurrent TimeUnit]
   [javax.net.ssl SSLContext TrustManager]
   [org.apache.commons.io IOUtils]
   [org.apache.http Header HttpEntityEnclosingRequest
    HttpRequest HttpResponse]
   [org.apache.http.client HttpClient]
   [org.apache.http.client.methods HttpDelete HttpGet HttpHead
    HttpPost HttpPut
    HttpUriRequest]
   [org.apache.http.client.params ClientPNames CookiePolicy]
   [org.apache.http.conn.scheme PlainSocketFactory Scheme
    SchemeRegistry]
   [org.apache.http.conn.ssl SSLSocketFactory]
   [org.apache.http.entity ByteArrayEntity]
   [org.apache.http.impl.client AbstractHttpClient
    DefaultHttpClient
    DefaultHttpRequestRetryHandler
    DefaultRedirectStrategy]
   [org.apache.http.impl.conn.tsccm ThreadSafeClientConnManager]
   [org.apache.http.params CoreConnectionPNames
    CoreProtocolPNames HttpParams]))

;; http://en.wikipedia.org/wiki/List_of_HTTP_status_codes#3xx_Redirection
(def +url-redirects+ #{301 302 303 307 308})


(defn host
  "Some weird urls crash the web.client url parser, so use a regex instead."
  [^String url]
  (second (re-find #"//([^/]+)" (.toLowerCase url))))

(def +disallowed-character-regex+
  "A regular expression matching characters that can never appear unencoded
   in a url.
   See https://www.ietf.org/rfc/rfc3986.txt, section 2."
  #"[^:/?#@!$&'()*+,;%=a-zA-Z0-9._~\-]")

(defn percent-encode-char [c]
  (->> (.getBytes c "utf-8")
       (map #(format "%%%X" %))
       (apply str)))

(defn encode-illegal-characters
  "Replace with their percent-encoded forms any characters in the given string
   that are not allowed unencoded in a URL. This includes things like all non-
   ascii characters and some punctuation characters, but not structural components
   of a url such as percents and slashes. The intended purpose is to preprocess
   urls from external sources that may not exactly follow the standards, without
   destroying the structure of those urls."
  [url]
  (str/replace url +disallowed-character-regex+ percent-encode-char))

(defn parse-url [^String url]
  (let [url-parsed (URI. (.replace url " " "%20"))
        ref (.getRawFragment url-parsed)
        uri (.getRawPath url-parsed)
        query (.getRawQuery url-parsed)]
    {:scheme (.getScheme url-parsed)
     :host (or (.getHost url-parsed)
               (.getRawAuthority url-parsed)) ;; handle grabbag://create_account
     :port (let [p (.getPort url-parsed)]
             (when (> p 0) (long p)))
     :uri (if (and ref (.startsWith ref "!"))
            (str uri "#" ref)
            uri)
     :query-string query}))

(defn strip-punc [^String s]
  (let [strip (some identity (map #(.endsWith s %)
                                  [";" ":" "." ","]))]
    (if (not strip) s
        (.substring s 0 (- (.length s) 1)))))

(defn charset-headers [headers]
  (err/-?> (headers "content-type")
           (str/split #"=")
           second
           strip-punc
           str/trim))

(defn charset-http-equiv [meta]
  (if-let [content (first (filter #(= "Content-Type"
                                      (:http-equiv (parser/attr-map %)))
                                  meta))]
    (-> content
        parser/attr-map
        :content
        (str/split #"=")
        last
        str/trim)))

(defn charset-html5 [meta]
  (if-let [content (first (filter #(:charset (parser/attr-map %))
                                  meta))]
    (-> content
        parser/attr-map
        :charset
        str/trim)))

(defn charset
  "Get charset from meta tag."
  [{:keys [headers body]}]
  (or (charset-headers headers)
      (let [root (parser/dom body)
            meta (-> root
                     parser/head
                     (parser/elements "meta"))]
        (or (charset-http-equiv meta)
            (charset-html5 meta)))
      "UTF-8"))

(defn encoded [{:keys [^java.io.InputStream body] :as resp}]
  (let [bytes (IOUtils/toByteArray body)
        ^String cs (charset (assoc resp :body (String. bytes "UTF-8")))]
    (String. bytes cs)))

(defn close-response [resp]
  (when (instance? java.io.InputStream (:body resp))
    (.close ^java.io.InputStream (:body resp))
    true))

(defn consume-response [consume {:keys [body] :as resp}]
  (cond
   (instance? java.io.InputStream body)
   (with-open [^java.io.InputStream body body]
     (consume resp))
   (string? body)
   (consume resp)))

(defn output-coercion
  [as {:keys [headers ^java.io.InputStream body] :as resp}]
  (if (not (instance? java.io.InputStream body))
    resp
    (try
      (assoc resp
        :body (case (or as :string)
                :input-stream body
                :byte-array (IOUtils/toByteArray body)
                :string (let [^String h (headers "content-type")
                              render-string? (or (not h)
                                                 (.contains h "html")
                                                 (.contains h "xml")
                                                 (.contains h "text")
                                                 (.contains h "json")
                                                 (.contains h "x-www-form-urlencoded")
                                                 (.contains h "charset"))]
                          (if render-string?
                            (encoded resp)
                            body))))
      (finally (when-not (= as :input-stream) (.close body))))))

(defn input-coercion
  [{:keys [body] :as req}]
  (if (string? body)
    (-> req (assoc :body (data/utf8-bytes body)
                   :character-encoding "UTF-8"))
    req))

(defn decompress
  [resp]
  (case (get-in resp [:headers "content-encoding"])
    "gzip"
    (update-in resp [:body]
               (fn [^java.io.InputStream is]
                 (when is (java.util.zip.GZIPInputStream. is))))
    "deflate"
    (update-in resp [:body]
               (fn [^java.io.InputStream is]
                 (when is (java.util.zip.InflaterInputStream. is))))
    resp))


(defn wrap-accept-encoding
  [{:keys [accept-encoding] :as req}]
  (if accept-encoding
    (-> req (dissoc :accept-encoding)
        (assoc-in [:headers "Accept-Encoding"]
                  (str/join ", " (map name accept-encoding))))
    req))

(defn query-params
  [{:keys [query-params] :as req}]
  (if query-params
    (-> req (dissoc :query-params)
        (assoc :query-string (data/map->query-string query-params)))
    req))

(defn basic-auth [req]
  (if-let [[user password] (:basic-auth req)]
    (-> req
        (dissoc :basic-auth)
        (assoc-in [:headers "Authorization"]
                  (str "Basic "
                       (data/base64-encode (data/utf8-bytes (str user ":" password))))))
    req))

(defn content-type-value [type]
  (if (keyword? type)
    (str "application/" (name type))
    type))

(defn accept
  [{:keys [accept] :as req}]
  (if accept
    (-> req (dissoc :accept)
        (assoc-in [:headers "Accept"]
                  (content-type-value accept)))
    req))

(defn content-type [req]
  (if (not (:content-type req))
    req
    (update-in req [:content-type] content-type-value)))

(defn header-content-type [req]
  (->> req
       :headers
       (keep (fn [[k v]] (when (= "content-type" (str/lower-case k)) v)))
       first))

(defn ensure-parsed-url [req]
  (cond
   (string? req) (parse-url req)
   (:url req) (-> req
                  (merge (parse-url (:url req)))
                  (dissoc :url))

   :default req))

(defn path-redirect-strategy
  "Returns a RedirectStrategy that stores the redirect path in the provided
   atom as a nested vector of the form [[status-code-0 url-0] ... [status-code-n url-n]]."
  [a]
  (proxy [DefaultRedirectStrategy] []
    (^HttpUriRequest getRedirect [^org.apache.http.HttpRequest request
                                  ^org.apache.http.HttpResponse response
                                  ^org.apache.http.protocol.HttpContext context]
      (let [^HttpUriRequest redirect-req (proxy-super
                                          getRedirect
                                          request response context)
            status (-> response
                       .getStatusLine
                       .getStatusCode
                       str
                       keyword)
            redirect-url (-> redirect-req
                             .getRequestLine
                             .getUri)]
        (swap! a conj [status redirect-url])
        redirect-req))))

(def default-params
  {ClientPNames/COOKIE_POLICY CookiePolicy/BROWSER_COMPATIBILITY
   ClientPNames/HANDLE_REDIRECTS true
   ClientPNames/MAX_REDIRECTS (Integer. 10)
   ClientPNames/ALLOW_CIRCULAR_REDIRECTS true
   ClientPNames/REJECT_RELATIVE_REDIRECT false})

;; CoreConnectionPNames.SO_TIMEOUT
;; CoreProtocolPNames.USER_AGENT
;; CoreConnectionPNames.SOCKET_BUFFER_SIZE
;; CoreConnectionPNames.CONNECTION_TIMEOUT
(defn config-client
  [^AbstractHttpClient client
   {:keys [params num-retries ^RedirectStrategy redirect-strategy timeout
           connect-timeout socket-timeout]
    :or {num-retries 1 timeout 30000}}]
  (let [^HttpParams client-params (.getParams client)]
    (doseq [[pk pv] params]
      (.setParameter client-params pk pv))
    (.setIntParameter client-params CoreConnectionPNames/CONNECTION_TIMEOUT
                      (int (or connect-timeout timeout)))
    (.setIntParameter client-params CoreConnectionPNames/SO_TIMEOUT
                      (int (or socket-timeout timeout))))
  (doto client
    (.setRedirectStrategy redirect-strategy)
    (.setHttpRequestRetryHandler (DefaultHttpRequestRetryHandler. (int num-retries) true))))


(defn pooled-http-client
  "A threadsafe, single client using connection pools to various hosts."
  ([] (pooled-http-client {:ttl 120
                           :max-total-conns 1000
                           :max-per-route 1000
                           :params default-params
                           :redirect-strategy (DefaultRedirectStrategy.)}))
  ([{:keys [ttl max-total-conns max-per-route]}]

     (let [psf (PlainSocketFactory/getSocketFactory)
           ssf (SSLSocketFactory/getSocketFactory)
           schemes (doto (SchemeRegistry.)
                     (.register (Scheme. "http" psf 80))
                     (.register (Scheme. "https" ssf 443)))
           mgr (doto (ThreadSafeClientConnManager. schemes (long ttl) TimeUnit/SECONDS)
                 (.setMaxTotal max-total-conns)
                 (.setDefaultMaxPerRoute max-per-route))]
       (config-client (DefaultHttpClient. mgr)
                      {:params default-params
                       :redirect-strategy (DefaultRedirectStrategy.)}))))

(def no-op-trust-manager
  (reify  javax.net.ssl.X509TrustManager
    (^void checkClientTrusted [this ^"[Ljava.security.cert.X509Certificate;" xcs ^String s])
    (^void checkServerTrusted [this ^"[Ljava.security.cert.X509Certificate;" xcs ^String s])
    (getAcceptedIssuers [this])))

(defn wrap-ignore-ssl [^HttpClient client]
  (let [ctx (doto (SSLContext/getInstance "TLS")
              (.init nil ^"[Ljavax.net.ssl.TrustManager;"
                     (into-array TrustManager [no-op-trust-manager])
                     nil))
        ssf (doto (SSLSocketFactory. ctx)
              (.setHostnameVerifier (SSLSocketFactory/ALLOW_ALL_HOSTNAME_VERIFIER)))
        ccm (.getConnectionManager client)
        sr (.getSchemeRegistry ccm)]
    (.register sr (Scheme. "https" ssf 443))
    (DefaultHttpClient. ccm (.getParams client))))

(defn basic-http-client
  ([] (basic-http-client {:params default-params
                          :redirect-strategy (DefaultRedirectStrategy.)}))
  ([conf]
     (config-client (DefaultHttpClient.) conf)))

(defn params-with-user-agent [user-agent]
  (assoc default-params
    CoreProtocolPNames/USER_AGENT
    user-agent))

(def spoofed-http-user-agent "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_6) AppleWebKit/534.24 (KHTML, like Gecko) (Contact: backend@example.com)")

(defn spoofed-http-client [& [user-agent]]
  (wrap-ignore-ssl
   (basic-http-client
    {:timeout 20000
     :params
     (params-with-user-agent (or user-agent spoofed-http-user-agent))})))

(def iphone-http-user-agent "Mozilla/5.0 (iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3")

(defn iphone-http-client []
  (wrap-ignore-ssl
   (basic-http-client
    {:timeout 20000
     :params
     (params-with-user-agent iphone-http-user-agent)})))

(defn- parse-headers
  "TODO: this is buggy for headers that can be repeated like set-cookie"
  [^HttpResponse http-resp]
  (into {} (map (fn [^Header h] [(.toLowerCase (.getName h)) (.getValue h)])
                (iterator-seq (.headerIterator http-resp)))))

(def +host->strip-fns+
  "map of host -> function that should be applied to decide if we  keep a query paramter. * is default and is applied to all hosts"
  {"www.bbc.com" #(= % "ocid")
   "*" (fn [^String qp] (or (.startsWith qp "utm_") (.startsWith qp "fb_") (.equalsIgnoreCase qp "mbid")))})

(defn keep-query-parameters [qm host]
  (->> qm
       keys
       (remove (get +host->strip-fns+ "*"))
       (remove (get +host->strip-fns+ host (constantly nil)))))

(defn strip-query-map [qm host]
  (into (sorted-map)
        (select-keys qm (keep-query-parameters qm host))))

(defn- default-port [scheme]
  ({:http 80 :https 443} (keyword scheme)))

(defn build-url [{:keys [scheme,
                         host
                         port
                         uri,
                         query-string]}]
  (str (if (keyword? scheme) (name scheme) (or scheme "http"))
       "://" host
       (when (and port (not= port (default-port scheme)))
         (str ":" port))
       uri
       (when (and query-string
                  (not (.isEmpty ^String query-string)))
         (str "?" query-string))))

(defn add-query-params
  "Safely add more query params to a URL. Properly handles urls both with and without query-params already."
  [url added-query-params]
  (-> url
      parse-url
      (update-in [:query-string] (fn-> data/query-string->map
                                       (merge added-query-params)
                                       data/map->query-string))
      build-url))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; URL resolving routines. TODO: These are good candidates for moving into own namespace (url-resolve)

(defn clean-url-query-string [url]
  (build-url
   (letk [[host query-string :as parsed-url] (parse-url url)]
     (if-not query-string
       parsed-url
       (update-in parsed-url [:query-string]
                  (fn [qs]
                    (-> qs
                        (str/replace "amp;" "")
                        (data/query-string->map false)
                        (strip-query-map host)
                        data/map->query-string)))))))

(defn relative-url [^String orig-url ^String redirect-url]
  (-> (java.net.URI.
       (if (<= (.lastIndexOf orig-url "/") 7)
         (str orig-url "/") ;; urls with no trailing slash get fucked up.
         orig-url))
      (.resolve redirect-url)
      (.toString)))

(defn resp->resolved-url [resp]
  (let [{:keys [redirects status url headers]} resp]
    (clean-url-query-string
     (or
      (->> redirects
           (filter (fn [[redirect-sym url]] (contains?
                                             +url-redirects+
                                             (Integer/parseInt (name redirect-sym)))))
           last
           second)
      url))))

(defn ^Scanner scanner [x]
  (cond (instance? String x) (Scanner. ^String x)
        (instance? InputStream x) (Scanner. ^InputStream x)
        :else (throw (RuntimeException. (str "can't make a scanner from " (class x))))))

(def ^java.util.regex.Pattern canon-url-regex  #"</head>|<link\s+rel=[\"']canonical[\"']\s+href=[\"']([^\"']+)[\"']|<meta\s+property=[\"']og:url[\"']\s+content=[\"']([^\"']+)[\"']")
(def ^java.util.regex.Pattern meta-refresh-url-regex #"<noscript>\s*.+URL=([^\"']+)")

(defn body->canonical-url [body max-readahead]
  (let [s (scanner body)
        read-ahead (int (or max-readahead 0))]
    (if (.findWithinHorizon s canon-url-regex read-ahead)
      (let [m (.match s)]
        (or (.group m 1) (.group m 2)))
      (when (.findWithinHorizon s meta-refresh-url-regex read-ahead)
        (let [m (.match s)]
          (.group m 1))))))

(defn resp->canonical-url [{:keys [url body] :as fetched} & [max-readahead]]
  "Look at a response and determine the canonical URL. Takes into account various redirect schemes:
   301, 303, 307, meta-refreshes and what not"
  (let [^String o (or (consume-response
                       (fn [body]
                         (when-let [resolved (body->canonical-url (:body body) max-readahead)]
                           (clean-url-query-string (relative-url url resolved))))
                       fetched)
                      (resp->resolved-url fetched))]
    (String. o)))

;;; END URL resolving routines.

(defn add-headers [^HttpRequest http-req
                   {:keys [headers content-type character-encoding do-not-close]}]
  (when content-type
    (.addHeader http-req "Content-Type"
                (if-not character-encoding
                  content-type
                  (str content-type
                       "; charset=" character-encoding))))
  (when-not do-not-close
    (.addHeader http-req "Connection" "close"))
  (doseq [[header-n header-v] headers]
    (.addHeader http-req header-n header-v))
  http-req)

(defn add-request-body [^HttpEntityEnclosingRequest http-req
                        body]
  (when body
    (.setEntity http-req (ByteArrayEntity. ^bytes body)))
  http-req)

(defn create-request [{:keys [request-method headers
                              content-type character-encoding body]
                       :as components}]
  (let [^String url (build-url (merge components
                                      (when-let [u (:url components)] (parse-url u))))]
    [url
     (->  (case request-method
            :get    (HttpGet. url)
            :head   (HttpHead. url)
            :put    (HttpPut. url)
            :post   (HttpPost. url)
            :delete (HttpDelete. url))
          (add-headers components)
          (add-request-body body))]))

(defn request
  "Executes the HTTP request corresponding to the given Ring request map and
   returns the Ring response map corresponding to the resulting HTTP response."
  ([config]
     (request (basic-http-client) config))
  ([^AbstractHttpClient http-client
    {:keys [request-method
            headers content-type character-encoding body]
     :as components}]
     (let [[http-url ^HttpRequest http-req]
           (create-request components)
           redirects (atom [])
           ^RedirectStrategy redirect-strategy
           (path-redirect-strategy redirects)]
       (.setRedirectStrategy http-client redirect-strategy)
       (try
         (let [^HttpResponse http-resp (.execute http-client http-req)]
           {:status (.getStatusCode (.getStatusLine http-resp))
            :headers (parse-headers http-resp)
            :body  (when-let [ent (.getEntity http-resp)]
                     (.getContent ent))
            :url http-url
            :redirects @redirects})
         (catch Exception e
           (.abort http-req)
           (throw e))))))

(def gzip ["gzip" "deflate"])

(defn build-request [method url-or-req]
  (-> url-or-req
      ensure-parsed-url
      (merge {:accept-encoding
              (when-not (and (map? url-or-req) (:no-gzip? url-or-req))
                gzip)
              :request-method method})
      content-type
      basic-auth
      wrap-accept-encoding
      accept
      query-params
      basic-auth
      input-coercion))

(defn fetch
  ([req]
     (fetch (:request-method req (if (:body req) :post :get)) req))
  ([method url]
     (fetch #(basic-http-client)
            method
            url))
  ([get-client method url-or-req]
     (let [{:keys [as] :as req } (build-request method url-or-req)]
       (->> req
            (request (get-client))
            decompress
            (output-coercion as)))))

(defn post-fn
  ([content-type serialize-fn]
     (post-fn content-type serialize-fn nil))
  ([content-type serialize-fn accept]
     (fn post-it!
       ([url data]
          (post-it! (basic-http-client) url data))
       ([client url data]
          (fetch (constantly client) :post
                 {:url url
                  :headers (assoc-when {"Content-Type" content-type}
                                       "X-Accept" accept)
                  :body (serialize-fn data)})))))

(def json-post (post-fn "application/json" json/generate-string "application/json"))
(def serialized-post (post-fn data/+serialized-content-type+ data/encode-serialized-content data/+serialized-content-type+))

(defnk safe-body [body status :as resp]
  (when-not (<= 200 status 299)
    (log/throw+ {:resp resp
                 :message
                 (format "non-2XX resp: %s %s" status
                         (or (err/?debug "error parsing" (json/parse-string body)) body))}))
  body)

(def json
  "Convert the response into keywordized json, or throw an error for non-200"
  (fn-> safe-body (json/parse-string true)))

(defnk json-response? [headers]
  (when-let [^String content-type (or (get headers "Content-Type") (get headers "content-type"))]
    (.startsWith content-type "application/json")))

(s/defn rest-request :- {:status long :body s/Any s/Any s/Any}
  "For use in tests. Perform an api request against the local server.
   Note: response body may need to be extracted and parsed with client/json."
  ([m :- {:port long :uri String s/Keyword s/Any}]
     (rest-request nil m))
  ([get-client m :- {:port long :uri String s/Keyword s/Any}]
     (let [req (merge {:host "localhost"}
                      (update-in-when m [:body] json/generate-string)
                      {:headers (assoc-when
                                 (:headers m)
                                 "Content-Type"
                                 (when (and (not (header-content-type m)) (:body m))
                                   "application/json"))})
           method (if (:body m) :post :get)
           resp (if get-client
                  (fetch get-client method req)
                  (fetch method req))]
       (if (json-response? resp)
         (update resp :body #(json/parse-string % true))
         resp))))

(def spoofed-http-get (partial fetch spoofed-http-client :get))

(defn expand-url [client url]
  (resp->resolved-url (fetch client :head url)))

(defn canonical-url [client url & [max-chars]]
  (resp->canonical-url (fetch client :get {:url url :as :input-stream}) max-chars))

(defn safe-canonical-url [^String url fetchable-url? & [max-chars]]
  (when (fetchable-url? url)
    (let [url (.replace url " " "%20")
          cann-url (canonical-url spoofed-http-client url (or max-chars 10000))]
      (when (fetchable-url? cann-url)
        cann-url))))

(defn fetch-redirect [url]
  ((juxt :status (fn-> :headers (get "location")))
   (fetch
    (partial basic-http-client {:params {"http.protocol.handle-redirects" false}})
    :head url)))
