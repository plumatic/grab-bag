(ns web.handlers
  "Helpers for constructing responses; should really be called web.responses."
  (:use plumbing.core)
  (:require
   [ring.util.mime-type :as mime-type]
   [schema.core :as s]
   [plumbing.html-gen :as html-gen]
   [plumbing.json :as json]
   [plumbing.logging :as log]
   [web.client :as client]
   [web.data :as data]))

(s/defn content-response
  [content-type :- String
   status :- s/Int
   body :- (s/either String java.io.InputStream)]
  {:headers {"content-type" content-type}
   :status status
   :body body})

(defn response-fn
  ([content-type]
     (response-fn content-type identity))
  ([content-type serialize-fn]
     (fn res
       ([body] (res 200 body))
       ([status body] (content-response content-type status (serialize-fn body))))))

(def html-response (response-fn "text/html; charset=UTF-8"))
(def json-response (response-fn "application/json; charset=UTF-8" json/generate-string))
(def edn-response (response-fn "application/edn" pr-str))
(def serialized-response (response-fn data/+serialized-content-type+ data/encode-serialized-content-streaming))

(defn html5 [& content] (html-response (html-gen/render [:html content])))

(defn ->json-response [res]
  (-> res
      (assoc-in [:headers "Content-Type"] "application/json; charset=UTF-8")
      (update-in [:body] #(json/generate-string %))))

(def mime-types
  {"ttf"  "application/x-font-ttf"
   "otf"  "application/x-font-opentype"
   "woff" "application/x-font-woff"
   "eof"  "application/vnd.ms-font-object"
   "map"  "application/octet-stream"
   "cljs" "application/octet-stream"})

(defn resource-response [^java.util.regex.Pattern whitelist ^String path]
  (if-let [is (and (re-find whitelist path)
                   (= -1 (.indexOf path ".."))
                   (ClassLoader/getSystemResourceAsStream path))]
    (content-response (mime-type/ext-mime-type path mime-types) 200 is)
    (let [msg "Not found"]
      (log/throw+ {:message msg :client-message msg :status 404 :path path}))))

(defn url-redirect [url]
  {:status 302
   :headers {"Location" url}
   :body ""})

(defn permanent-redirect [url]
  {:status 301
   :headers {"Location" url}
   :body ""})
