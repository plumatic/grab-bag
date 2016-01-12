(ns plumbing.string
  (use plumbing.core)
  (require
   [schema.core :as s]
   [clojure.string :as str]))

(defn truncate [^String s ^long l]
  "Truncate s, elipsising at the first space beyond l characters.  Note that the result
   can thus be (much) longer than l.  TODO: figure out why it was written this way and
   rewrite to the expected definition if possible."
  (let [sl (count s)]
    (if (<= sl l)
      s
      (let [idx (.indexOf s (int \ ) l)]
        (if (>= idx 0) (str (.substring s 0 idx) "...") s)
        #_(str (.substring s 0 (if (>= idx 0) idx l)) "...")))))

(s/defn truncate-to :- String
  "Truncate s, elipsising at the last space under max-length characters.
   Result is guaranteed not to exceed `max-length` characters"
  [s :- String max-length :- long]
  (doto
      (if (<= (count s) max-length)
        s
        (let [ellipses "..."
              ellided-max-len (- max-length (count ellipses))]
          (if-not (pos? ellided-max-len)
            (subs s 0 max-length)
            (let [idx (.lastIndexOf s (int \ ) ellided-max-len)
                  max-idx (if (> idx 0)
                            idx
                            ellided-max-len)]
              (str (subs s 0 max-idx) ellipses)))))
    (-> count (<= max-length) (assert "String exceeds bound"))))

(defn and-list-vec
  "make a vector representing a list with comma separation and an and."
  [xs]
  (if (= (count xs) 1) (vec xs)
      (conj
       (vec
        (interleave
         (vec xs)
         (conj (vec (repeat (- (count xs) 2) ", ")) (if (> (count xs) 2) ", and " " and "))))
       (last xs))))

(defn and-list [xs]
  (if (= (count xs) 1) (first xs)
      (str/join
       (and-list-vec xs))))

(defn camel->lisp [^String s]
  (->> (.replaceAll s " " "-")
       (partition-by #(Character/isUpperCase (int %)))
       (partition-all 2)
       (map #(apply str (aconcat %)))
       (str/join "-")
       (.toLowerCase)))

(defn from-tsv
  "Convert a tab-separated table with header represented as a sequence of strings
   into a sequence of maps.  Converts header strings from camel to lisp-case keywords
   e.g. [\"FooBar\tBaz\" \"1\t2\" \"4\t6\"] -->
        [{:foo-bar \"1\" :baz \"2\"} {:foo-bar \"4\" :baz \"6\"}]"
  [rows]
  (let [[head & data] (map (fn [r] (map #(.trim ^String %) (.split ^String r "\t"))) rows)
        ks (map (comp keyword camel->lisp) head)]
    (for [d data] (zipmap ks d))))

(s/defn shard
  "Break string into k roughly equal parts, at delimiters (newline by default)"
  ([s k] (shard s k \newline))
  ([s :- String k :- s/Int delim :- Character]
     (->> (range 0 (count s) (/ (count s) k))
          next
          (map #(inc (.indexOf s (int delim) (int %))))
          (cons 0)
          distinct
          (partition-all 2 1)
          (map #(apply subs s %)))))
