(ns tubes.string-utils
  (:require
   [clojure.string :as string]
   [tubes.core :as tubes]))

(defn title-case [s]
  (apply str (string/upper-case (first s)) (rest s)))

(defn first-name [name-string]
  (first (string/split name-string " ")))

(defn possessive [n]
  (if (= \s (.charAt n (dec (.-length n))))
    (str n "'")
    (str n "'s")))