(ns plumbing.html-gen
  (:use plumbing.core)
  (:require
   [clojure.string :as str]))


(defn style [m]
  (str
   (->> (sort m)
        (map (fn [[k v]] (str (name k) ":" v)))
        (str/join ";"))
   ";"))


(defn emit-attribute [k v]
  (cond (or (string? v) (keyword? v)) (str "\"" (.replaceAll ^String (name v) "\"" "&quot;") "\"")
        (integer? v) (str v)
        (and (= k :style) (map? v)) (emit-attribute k (style v))
        :else (throw (RuntimeException. (str "Can't emit attribute: " (pr-str [k v]))))))

(defn emit-attributes [attr]
  (str/join " " (map (fn [[k v]] (str (name k) "=" (emit-attribute k v))) attr)))

(defn emit-open-tag
  ([tag] (emit-open-tag tag nil))
  ([tag attr]
     (str "<" (name tag)
          (when attr (str " "(emit-attributes attr)))
          ">")))

(defn emit-close-tag
  [t] (str "</" (name t) ">"))

(declare render)

(defn render-helper [form]
  (cond
   (string? form) form
   (keyword? form) (emit-open-tag form)
   (coll? form)
   (when-not (empty? form)
     (let [tag (first form)]
       (if (coll? tag) (apply render form)
           (let [attr (when (map? (second form)) (second form))
                 tail (if (map? (second form)) (rest (rest form)) (rest form))]
             (str (emit-open-tag tag attr)
                  (apply render tail)
                  (emit-close-tag tag))))))
   :else (str form)))

(defn ^String render [& forms]
  (str/join " " (map render-helper forms)))

(defn link [href anchor & [target]]
  [:a (assoc-when {:href href} :target target) anchor])

(defn dekeywordize [k] (if (keyword? k) (name k) k))

(defn table-rows
  "Make table rows like in print-table. Values in rows should be markup.
   Optional format-header fn converts a column key into markup for the table header."
  [ks rows & [format-header row-style]]
  (cons [:tr
         (for [key ks]
           [:th ((or format-header dekeywordize) key)])]
        (for [row rows]
          [:tr
           (if row-style (row-style row) {})
           (for [key ks]
             [:td (get row key "")])])))

(defn table
  ([rows] (table (sort (distinct (mapcat keys rows))) rows))
  ([ks rows] [:table (table-rows ks rows)]))

(defn row-major-rows [rows]
  (doall
   (for [row rows]
     [:tr
      (doall
       (for [col row]
         [:td (dekeywordize col)]))])))

(defn row-major-table
  ([rows] (row-major-table {} rows))
  ([attributes rows]
     [:table attributes (row-major-rows rows)]))
