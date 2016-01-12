(ns tubes.cookies
  (:refer-clojure :exclude [set get remove])
  (:require [goog.net.cookies]))

(defn set [k v & [{:keys [max-age]}]]
  (.set goog.net.cookies
        (name k)
        (js/JSON.stringify (clj->js v))
        (or max-age -1)))

(defn get [k]
  (.get goog.net.cookies (name k)))

(defn remove [k & [{:keys [path domain]}]]
  (.remove goog.net.cookies (name k) path domain))
