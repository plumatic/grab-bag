(ns web.middleware
  "Some default middlewares that are common to multiple services"
  (:use plumbing.core)
  (:require
   [clojure.java.io :as io]
   [ring.middleware.params :as params]
   [plumbing.auth :as auth]
   [plumbing.json :as json]
   [plumbing.logging :as log]
   [plumbing.timing :as timing]
   [web.client :as client]
   [web.data :as data]
   [web.handlers :as handlers]
   [web.server :as server])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream File
    InputStream]
   [java.util.zip GZIPOutputStream]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error handling middleware

(defn sort-map [m ks]
  (->> m
       (sort-by #(or (first (positions #{(first %)} ks)) 1000))
       (apply concat)
       (apply array-map)))

(defn convert-error
  "Return a response for an error.  Normally hides details of
   the error, unless verbose? is true (for tests, etc)."
  [req t verbose?]
  (let [m (ex-data t)
        error-id (auth/rand-str 16)]
    (log/log (get m :log-level :error)
             t
             {:request-info (-> req
                                (select-keys [:request-method :uri :query-string :body :headers])
                                (update-in-when [:user] select-keys [:handle :id]))
              :error-code (name error-id)})
    (-> (:response m)
        (update-in [:status] #(or % (get m :status 500)))
        (update-in [:body :message] #(or % (get m :client-message "Unexpected error")))
        (update-in [:body] merge
                   {:error-code (name error-id)}
                   (when verbose?
                     {:explain (sort-map (log/throwable->map t)
                                         [:type :message :data :class :file :method :line :outer-class
                                          :nesting-depth :data-chain :stack-trace])}))
        (assoc-when "Content-Type" (when verbose? "application/json; charset=UTF-8")))))

(defn format-error-middleware [verbose? handler]
  "Middleware to interpret plumbing.logging/throw+ exceptions instead of treating them
   as unknown errors"
  (fn [req]
    (try
      (handler req)
      (catch Throwable t
        (convert-error req t verbose?)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Supporting head requests

(defn head-to-get-fallback-middleware [handler]
  (fn [req]
    (let [resp (handler req)]
      (if (and (= (:request-method req) :head) (#{404 405} (:status resp)))
        (let [get-resp (handler (assoc req :request-method :get))]
          (if (= 200 (:status get-resp))
            (assoc get-resp :body nil)
            resp))
        resp))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Admin middleware

(defn admin-middleware
  "Middleware for use in Grabbag web services like dashboard and tagger that checks
   that the user has a grabbag cookie associated with a prod admin user."
  [handler]
  (let [whitelist (atom #{})]
    (fn [req]
      (let [cookies (get-in req [:headers "cookie"])]
        (if (or (@whitelist cookies)
                (-> (client/fetch :get {:url "http://api.example.com/2.0/user"
                                        :headers {"Cookie" cookies}})
                    :body
                    (json/parse-string true)
                    :access-level
                    (= "admin")))
          (do (swap! whitelist conj cookies) (handler req))
          (handlers/url-redirect "http://example.com"))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Gzip middleware

(defn gzipped-response [resp]
  (let [body (resp :body)
        bout (ByteArrayOutputStream.)
        out (GZIPOutputStream. bout)
        resp (assoc-in resp [:headers "content-encoding"] "gzip")]
    (io/copy (resp :body) out)
    (.close out)
    (if (instance? InputStream body)
      (.close ^InputStream body))
    (assoc resp :body (ByteArrayInputStream. (.toByteArray bout)))))

(defn gzip-response [req resp]
  (let [req-headers (map-keys (fn [^String s] (.toLowerCase s)) (:headers req))
        {:keys [body,status,headers]} resp
        resp-headers (map-keys (fn [^String s] (.toLowerCase s)) headers)]
    (if (and (or (not status) (= status 200))
             (not (resp-headers "content-encoding"))
             (or
              (and (string? body) (> (count body) 200))
              (instance? InputStream body)
              (instance? File body)))
      (let [accepts (req-headers "accept-encoding" "")
            match (re-find #"(gzip|\*)(;q=((0|1)(.\d+)?))?" accepts)]
        (if (and match (not (#{"0" "0.0" "0.00" "0.000"} (match 3))))
          (gzipped-response resp)
          resp))
      resp)))

(defn gzip-middleware [handler]
  (fn [req]
    (gzip-response req (handler req))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Simple json middleware

(defn content-type?
  "by content-type, determine whether this request matches regex"
  [regex req]
  (boolean
   (when (contains? req :body) ;; handle null properly
     (when-let [^String type (:content-type req)]
       (seq (re-find regex type))))))

(def json-request? (partial content-type? #"^application/(vnd.+)?json"))
(def form-request? (partial content-type? #"^application/x-www-form-urlencoded"))

(defn decode-inputs [req]
  (let [encoding (or (:character-encoding req) "UTF-8")]
    (-> req
        (assoc :query-params (if-let [qs (:query-string req)]
                               (keywordize-map (#'params/parse-params qs encoding))
                               {}))
        (?> (json-request? req)
            (assoc :body (json/parse-string (:body req) true)))

        (?> (form-request? req)
            (assoc :body (data/query-string->map (:body req) true))))))

(defn encode-output [req resp]
  (if (or (get-in resp [:headers "Content-Type"])
          (get-in resp [:headers "content-type"]))
    resp
    (handlers/->json-response resp)))

(defn simple-default-middleware
  "Dead simple json in/out middleware with no support for cookies, cors,
   jsonp, etc.  Assumes the body has already been slurped
   by slurp-body-middleware."
  [handler]
  (fn [req]
    (->> req
         decode-inputs
         handler
         (merge {:status 200})
         (encode-output req)
         (gzip-response req))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Outermost middlewares to parse body, catch errors

(defn slurp-body-middleware
  "Turn the request body into a String, the only thing that should be done
   outside outermost error checking middleware."
  [handler]
  (fnk [request-method :as req]
    (handler (if (= request-method :post) (update req :body slurp) (dissoc req :body)))))


(defn req-resp-logger
  "Simple implementation of log-fn for outermost middleware."
  [req resp duration]
  (letk [[uri query-string request-method] req
         [{status nil}] resp]
    (log/infof "REQUEST %s in %4d ms on %s %s%s (%s)"
               status (long duration)
               (.toUpperCase (name request-method))
               uri (if query-string (str "?" query-string) "")
               (server/client-ip req))))

(defn outermost-middleware
  "Catch-all middleware that converts errors to 500's, logs, and times the request and response.
   log-fn is called with request, response, and duration."
  [log-fn handler]
  (fn [req]
    (try (let [[resp duration-ms] (timing/get-time-pair
                                   (try (handler req)
                                        (catch Throwable t t)))
               error? (instance? Throwable resp)
               status (if error? 500 (:status resp 200))]
           (log-fn req resp duration-ms)
           (if error?
             (if (instance? org.mortbay.jetty.EofException resp)
               {:status 400
                :message "Unexpected end of request body"}
               (do (log/errorf resp "Error in outermost middleware for request: %s"
                               (select-keys req [:request-method :uri :query-string :body :headers]))
                   {:status 500
                    :body "Unexpected error."}))
             resp))
         (catch Throwable t
           (log/errorf t "Error inside outermost middleware")
           {:status 500 :body "Unexpected error."}))))
