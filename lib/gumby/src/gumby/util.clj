(ns gumby.util
  (:use plumbing.core)
  (:require
   [schema.core :as s])
  (:import
   [org.elasticsearch.action ActionRequestBuilder
    ActionResponse]))

(set! *warn-on-reflection* true)

(defn ^"[Ljava.lang.String;" str-array
  "Returns Java String[] given coll of Strings of Keywords"
  [coll]
  (->> coll
       (map #(if (instance? clojure.lang.Named %) (name %) (str %)))
       (into-array String)))

(defn to-clj
  "Turns Response payloads into Clojure data. Eagerly walks input."
  [x]
  (cond
   (instance? java.util.HashMap x)
   (for-map [[k v] x] (keyword (name k)) (to-clj v))
   (instance? java.util.ArrayList x)
   (mapv to-clj x)
   (instance? Integer x)
   (long x)
   :else x))

(s/defn do-action! :- ActionResponse
  "Executes an action (ie *RequestBuilder) to get the response synchronously.
   Optionally accepts timeout in milliseconds and timeout value.
   Similar to deref (@)."
  ([action :- ActionRequestBuilder]
     (-> action .execute .actionGet))
  ([action :- ActionRequestBuilder timeout-ms :- long timeout-val]
     (try (-> action .execute (.actionGet timeout-ms))
          (catch java.util.concurrent.TimeoutException e
            timeout-val))))

(set! *warn-on-reflection* false)
