(ns plumbing.json
  (:use plumbing.core)
  (:require
   [clojure.string :as str])
  (:import
   [java.io StringReader StringWriter Writer]
   [com.fasterxml.jackson.core JsonFactory JsonParser$Feature]
   [plumbing JsonExt]))

(set! *warn-on-reflection* true)

(def ^JsonFactory factory
  (doto (JsonFactory.)
    (.configure JsonParser$Feature/ALLOW_UNQUOTED_CONTROL_CHARS true)
    (.configure JsonParser$Feature/ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER true)))

;; clj-json doesn't expose jackson generator for options
(defn generate-to-writer
  [obj ^Writer writer]
  (let [generator (doto (.createJsonGenerator factory writer)
                    (.setHighestNonEscapedChar 8191))]
    (JsonExt/generate generator nil obj)
    (.flush generator)))

(defn pretty-print [^String string]
  (-> string
      (.replaceAll "," ", ")
      (.replaceAll "}," "},\n")
      (.replaceAll ":" ": ")
      (.replaceAll ",  " ", ")))

(defn ^String generate-string* [obj]
  (let [sw (StringWriter.)]
    (generate-to-writer obj sw)
    (.toString sw)))

(defn ^String generate-string [obj & [pretty-print?]]
  (-> (generate-string* obj)
      (?> pretty-print? pretty-print)))

(defn parse-string [string & [keywords]]
  (JsonExt/parse
   (.createJsonParser factory (StringReader. string))
   true (or keywords false) nil))

(defn generate-literal-map
  "Like generate-string, but assumes the values are already valid js literal strings."
  [m]
  (->> m
       (map (fn [[k v]]
              (format "\"%s\": %s" (name k) v)))
       (str/join ",")
       (format "{%s}")))

(set! *warn-on-reflection* false)
