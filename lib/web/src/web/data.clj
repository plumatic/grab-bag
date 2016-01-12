(ns web.data
  (:use plumbing.core)
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [plumbing.classpath :as classpath]
   [plumbing.serialize :as serialize])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream File
    InputStream]
   [java.net URLDecoder URLEncoder]
   [java.util.zip GZIPOutputStream]
   [org.apache.commons.codec.binary Base64]
   [org.apache.commons.io IOUtils]))


(defn utf8-bytes
  "Returns the UTF-8 bytes corresponding to the given string."
  [#^String s]
  (.getBytes s "UTF-8"))

(defn utf8-string
  "Returns the String corresponding to the UTF-8 decoding of the given bytes."
  [#^"[B" b]
  (String. b "UTF-8"))

(defn url-encode
  "Returns an UTF-8 URL encoded version of the given string for form encoding -- spaces go to +."
  [unencoded]
  (URLEncoder/encode unencoded "UTF-8"))

(defn url-query-encode
  "The java.net.URLEncoder class encodes for application/x-www-form-urlencoded, but OAuth
and query params requires RFC 3986 encoding."
  [s]
  (-> (java.net.URLEncoder/encode s "UTF-8")
      (.replace "+" "%20")
      (.replace "*" "%2A")
      (.replace "%7E" "~")))

(defn url-decode
  "Returns an UTF-8 URL encoded version of the given string."
  [encoded]
  (URLDecoder/decode encoded "UTF-8"))

(defn host
  "Some weird urls crash the web.client url parser, so use a regex instead."
  [^String url]
  (second (re-find #"//([^/]+)" (.toLowerCase url))))

(defn subdomain-split
  "Split a hostname approximately into seq of subdomains and reserved domain.
   e..g, foo.bar.baz.co.uk --> ['foo' 'bar' 'baz.co.uk']"
  [^String host]
  (let [segments (.split host "\\.")
        tlds #{"com" "net" "org" "edu" "gov"}
        slds (conj tlds "co")
        has-secondary? (and (> (count segments) 2)
                            (not (tlds (last segments)))
                            (slds (last (butlast segments))))
        [subdomains reserved] ((juxt drop-last take-last) (if has-secondary? 3 2) segments)]
    (conj (vec subdomains) (str/join "." reserved))))

(defn map->body [m]
  (->> m
       (map (fn [[k v]] (str (name k) "=" v)))
       (str/join "&")))

(defn map->query-string [m]
  (->> m
       (map (fn [[k v]] (str (url-query-encode (name k)) "="
                             (url-query-encode (str v)))))
       (str/join "&")))

(defn body->map [^String s]
  (if (empty? s) {}
      (let [pieces (-> s (.replaceAll "#.*$" "") (.split  "&"))]
        (->> pieces
             (map #(vec (.split ^String % "=")))
             (into {})
             (map-keys keyword)))))

(defn split-query-kv [^String piece]
  (into []
        (map url-decode
             (let [idx (.indexOf piece "=")]
               (if (< idx 0)
                 ;; people fuck up query strings a lot
                 [piece ""]
                 [(String. (.substring piece 0 idx)) (String. (.substring piece (inc idx)))])))))

(defn query-string->map
  ([^String s]
     (query-string->map s true))
  ([^String s keywordize?]
     (if (empty? s) {}
         (let [pieces (-> s (.replaceAll "#.*$" "") (.split  "&"))]
           (->> pieces
                (map split-query-kv)
                (into {})
                (?>> keywordize? (map-keys keyword)))))))

(defn base64-encode
  "Encode an array of bytes into a base64 encoded string."
  [unencoded]
  (utf8-string (Base64/encodeBase64 unencoded)))

(defn base64-decode
  "Encode an array of bytes into a base64 encoded string."
  [encoded]
  (if (string? encoded)
    (Base64/decodeBase64 ^String encoded)
    (Base64/decodeBase64 ^bytes encoded)))

(defn resource->base64 [resource-filename]
  (-> resource-filename
      classpath/stream-from-classpath
      IOUtils/toByteArray
      base64-encode))

(def +serialized-content-type+ "application/chin-city-rest")

(defn encode-serialized-content-streaming [data]
  (serialize/clj->input-stream data))

(defn encode-serialized-content [data]
  (serialize/serialize serialize/+default+ data))

(defn decode-serialized-content [body]
  (serialize/deserialize-and-close-stream body))

(defnk decode-serialized-resp
  "Decode serialized with +serialized-content-type+.  Must be used with :as :input-stream.

   (current output coercion won't read the body at all without some :as op or other:
    https://github.com/plumatic/grab-bag/blob/master/grabbag/web/src/web/client.clj#L148)"
  [body headers]
  (when-not (= (headers "content-type") +serialized-content-type+)
    (throw (IllegalArgumentException. (str "Invalid content-type " (headers "content-type")))))
  (decode-serialized-content body))

(defn jsonp-str
  ([json-str]
     (jsonp-str "jsonp" json-str))
  ([^String callback json-str]
     (str (.. callback (replace "\n" "") (replace "\r" "")) "(" json-str ")")))
