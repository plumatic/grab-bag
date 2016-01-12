(ns plumbing.core-incubator
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [schema.core :as s]
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.map :as map])
  (:import
   [java.util ArrayList Collection Collections Random]))

(def maps (comp set (partial map)))

(defn filter-vals
  "map-vals : map :: filter-vals : filter"
  [f m]
  (for-map [[k v] m :when (f v)] k v))

(defn assoc-in-when
  "Like assoc but only assocs when value is truthy"
  [m ks v]
  (if v (assoc-in m ks v) m))

(defn partition-evenly
  "Partition xs into k parts as evenly as possible."
  [k xs]
  (let [xs (vec xs)
        part-size (/ (count xs) k)]
    (mapv (fn [start]
            (let [istart (int start)]
              (subvec xs (int start) (int (+ start part-size)))))
          (range 0 (count xs) part-size))))

(defn pprint-str [x]
  (with-out-str (pprint/pprint x)))

(defn rand-long
  "Returns a random long between 0 (inclusive) and n (exclusive).
Contains only 48 bits of randomness"
  [n]
  (long (rand n)))

(defn shuffle-by
  "Like shuffle, but uses the passed in Random object"
  [^Random random ^Collection coll]
  (let [al (ArrayList. coll)]
    (Collections/shuffle al random)
    (clojure.lang.RT/vector (.toArray al))))

(defn distribute-by
  "Rearrange xs in so that any range has roughly equal proportions of values (f x).
   Values of xs with the same (f x) remain in the same order."
  [f xs]
  (let [n (count xs)]
    (->> xs
         (group-by f)
         (sort-by (fn [[k kxs]] [(- (count kxs)) k]))
         (mapcat (fn [[_ kxs]]
                   (map vector (range 0 n (/ n (count kxs))) kxs)))
         (sort-by first)
         (map second))))

(defn count-realized
  "Count how many elements of this sequence have been realized."
  [xs]
  (->> xs
       (iterate rest)
       (take-while #(and (or (not (instance? clojure.lang.IPending %)) (realized? %))
                         (seq %)))
       count))

(defn rand-nth-by
  "Like rand-nth, but uses passed in Random object."
  [^Random random ^java.util.List coll]
  (.get coll (.nextInt random (.size coll))))

(defn xor [x y]
  (or (and x (not y))
      (and y (not x))))

(defmacro spy [x & [context]]
  `(let [context# ~context
         x# ~x
         file# ~*ns*
         line# ~(:line (meta &form))]
     (println "SPY" (format "%s:%d(%s)" file# line# (class x#)) (str context#))
     (pprint/pprint x#)
     x#))

(def pkeep
  (comp #(remove nil? %) pmap))

(defn ensure-keys
  "ensures that the keys are present in the map, and sets them to the default value if either
   not present or is currently in the map but has a value of nil."
  [m keys & [default-value]]
  (reduce
   (fn [m key]
     (update-in m [key] #(or % default-value)))
   m keys))

(defn safe-select-keys [m keyseq]
  (let [res (select-keys m keyseq)]
    (if (= (count res) (count keyseq))
      res
      (throw (RuntimeException. (format "Missing required keys: %s from %s" (set (remove (partial contains? res) keyseq)) (keys m)))))))

(defn safe-singleton [xs]
  "Throw if xs has != 1 element, and otherwise return it."
  (when (empty? xs) (throw (RuntimeException. "safe-singleton got empty seq")))
  (when (next xs) (throw (RuntimeException. "safe-singleton got >1 elt")))
  (first xs))

(defn group-map-by
  "Like group-by, but applies v-fn to the values of the map.
   (= {1 [2 3] 4 [5]} (group-map-by first second [[1 2] [1 3] [4 5]]))"
  [k-fn v-fn s]
  (->> s
       (group-by k-fn)
       (map-vals #(mapv v-fn %))))

(defn index-by
  "Like group-by, but maps from key to the single unique value that
   maps to that key."
  [f xs]
  (->> xs
       (group-by f)
       (map-vals safe-singleton)))

(defn map-fsm
  "Map with state.  f takes [state input] and returns [state output]"
  [f init-state s]
  (lazy-seq
   (when-let [[in & more] (seq s)]
     (let [[next-state out] (f [init-state in])]
       (cons out (map-fsm f next-state more))))))

(def keepv (comp vec keep))

(defn constantly-fnk [m]
  (pfnk/fn->fnk
   (fn [_] m)
   [{s/Any s/Any}
    (map/map-leaves (constantly s/Any) m)]))

(defn merge-all-with
  "Like merge-with, but takes a default value, and guarantees to call
   the fn on all output keys, using the default value as necessary.
   Useful for, e.g., subtracting probability distributions represented
   as maps."
  [f default & maps]
  (for-map [k (distinct (mapcat keys maps))]
    k
    (apply f (map #(get % k default) maps))))

(defmacro when-class-exists [class-sym & body]
  "execute body only if a class is in the classpath"
  (try (resolve class-sym)
       `(do ~@body)
       (catch ClassNotFoundException e nil)))

(defn ex-causes
  "Returns a lazy sequence of throwables in .getCause chain of e"
  [e]
  (take-while some? (iterate #(.getCause ^Throwable %) e)))

(defn some-cause
  "Returns first throwable in .getCause chain of e that is instance of throwable-class
   if any, otherwise nil"
  [^Class throwable-class ^Throwable e]
  (some #(instance? throwable-class %) (ex-causes e)))

(defn distinct-by-fast [f xs]
  (let [s (java.util.HashSet.)]
    (for [x xs
          :let [id (f x)]
          :when (not (.contains s id))]
      (do (.add s id)
          x))))
