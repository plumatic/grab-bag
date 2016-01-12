(ns domain.docs.core
  (:use plumbing.core)
  (:require
   potemkin
   [clojure.data :as data]
   [schema.core :as s]))

(defn record-reader [from-map-fn & [example]]
  "given an f, and an example of that function's input
   returns a 'safer' version of f, which verifies that
   inputs match the structure of the provided example"
  (let [example (or example (from-map-fn {}))
        record-keys (set (keys example))]
    (fn [data]
      (when-not (nil? data)
        (when-not (= (count data) (count record-keys))
          (throw (RuntimeException. (format "Record has wrong set of keys: %s"
                                            (data/diff (set (keys data)) record-keys)))))
        (from-map-fn data)))))

(defn record-writer [example]
  (let [record-keys (keys example)]
    (fn [rec]
      (when-not (nil? rec)
        (when-not (instance? (class example) rec)
          (throw (RuntimeException. (format "Record is of class %s, not %s"
                                            (class rec) (class example)))))
        (for-map [k record-keys] k (k rec))))))

(potemkin/definterface+ IShare
  (^String distinct-by-key [this] "Only one action with a key per type should be kept on a doc.")
  (^long date [this]))

(defn vconcat [& args]
  (vec (apply concat args)))

(s/defn new-shares [old-shares :- [IShare] received-shares :- [IShare]]
  (let [old-keys (set (map distinct-by-key old-shares))]
    (remove #(contains? old-keys (distinct-by-key %)) received-shares)))

(defn concat-shares [shares1 shares2]
  (vconcat shares1 (new-shares shares1 shares2)))

(s/defn merge-shares
  "if there are multiple non-distinct shares, keeps the new one not the old one."
  [old-shares :- [IShare] received-shares :- [IShare]]
  (vec (distinct-by distinct-by-key (concat received-shares old-shares))))

(s/defschema ActionClientType
  "Record client the action is done on, because action behaviors are very different per-platform.
   Currently should be :web, :iphone, or :android, but leaving as Keyword to ease addition of
   future clients."
  s/Keyword)
