(ns plumbing.repl
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [plumbing.parallel :as parallel])
  (:import
   [clojure.lang Compiler$C]
   [java.lang.reflect Constructor Method Modifier]
   [java.util.regex Pattern]))


;; Missing old stuff copied from contrib

(defn expression-info
  "Uses the Clojure compiler to analyze the given s-expr.  Returns
  a map with keys :class and :primitive? indicating what the compiler
  concluded about the return value of the expression.  Returns nil if
  not type info can be determined at compile-time.

  Example: (expression-info '(+ (int 5) (float 10)))
  Returns: {:class float, :primitive? true}"
  [expr]
  (let [fn-ast (Compiler/analyze Compiler$C/EXPRESSION `(fn [] ~expr))
        expr-ast (.body (first (.methods fn-ast)))]
    (when (.hasJavaClass expr-ast)
      {:class (.getJavaClass expr-ast)
       :primitive? (.isPrimitive (.getJavaClass expr-ast))})))


;; from cc string
(defn- re-partition
  "Splits the string into a lazy sequence of substrings, alternating
  between substrings that match the patthern and the substrings
  between the matches.  The sequence always starts with the substring
  before the first match, or an empty string if the beginning of the
  string matches.

  For example: (re-partition #\"[a-z]+\" \"abc123def\")

  Returns: (\"\" \"abc\" \"123\" \"def\")"
  [^Pattern re string]
  (let [m (re-matcher re string)]
    ((fn step [prevend]
       (lazy-seq
        (if (.find m)
          (cons (.subSequence string prevend (.start m))
                (cons (re-groups m)
                      (step (+ (.start m) (count (.group m))))))
          (when (< prevend (.length string))
            (list (.subSequence string prevend (.length string)))))))
     0)))
(defn- sortable [t]
  (apply str (map (fn [[a b]] (str a (format "%04d" (Integer. b))))
                  (partition 2 (concat (re-partition #"\d+" t) [0])))))

(defn- param-str [m]
  (str " (" (str/join
             "," (map (fn [[c i]]
                        (if (> i 3)
                          (str (.getSimpleName c) "*" i)
                          (str/join "," (replicate i (.getSimpleName c)))))
                      (reduce (fn [pairs y] (let [[x i] (peek pairs)]
                                              (if (= x y)
                                                (conj (pop pairs) [y (inc i)])
                                                (conj pairs [y 1]))))
                              [] (.getParameterTypes m))))
       ")"))

(defn- member-details [m]
  (let [static? (Modifier/isStatic (.getModifiers m))
        method? (instance? Method m)
        ctor?   (instance? Constructor m)
        text (if ctor?
               (str "<init>" (param-str m))
               (str
                (when static? "static ")
                (.getName m) " : "
                (if method?
                  (str (.getSimpleName (.getReturnType m)) (param-str m))
                  (str (.getSimpleName (.getType m))))))]
    (assoc (bean m)
      :sort-val [(not static?) method? (sortable text)]
      :text text
      :member m)))

(defn show
  "With one arg prints all static and instance members of x or (class x).
  Each member is listed with a number which can be given as 'selector'
  to return the member object -- the REPL will print more details for
  that member.

  The selector also may be a string or regex, in which case only
  members whose names match 'selector' as a case-insensitive regex
  will be printed.

  Finally, the selector also may be a predicate, in which case only
  members for which the predicate returns true will be printed.  The
  predicate will be passed a single argument, a map that includes the
  :text that will be printed and the :member object itself, as well as
  all the properies of the member object as translated by 'bean'.

  Examples: (show Integer)  (show [])  (show String 23)  (show String \"case\")"
  ([x] (show x (constantly true)))
  ([x selector]
     (let [c (if (class? x) x (class x))
           members (sort-by :sort-val
                            (map member-details
                                 (concat (.getFields c)
                                         (.getMethods c)
                                         (.getConstructors c))))]
       (if (number? selector)
         (:member (nth members selector))
         (let [pred (if (ifn? selector)
                      selector
                      #(re-find (re-pattern (str "(?i)" selector)) (:name %)))]
           (println "=== " (Modifier/toString (.getModifiers c)) c " ===")
           (doseq [[i m] (map-indexed vector members)]
             (when (pred m)
               (printf "[%2d] %s\n" i (:text m)))))))))

(defn- pad-to [s1 s2]
  (concat s1 (repeat (- (count s2) (count s1)) nil)))

(defn compare-rankings [s1 s2 & [key-fn limit max-width]]
  (let [truncated (java.util.ArrayList.)
        truncate (fn [s]
                   (let [s (str s)]
                     (if (or (not max-width) (<= (count s) max-width))
                       s
                       (do (.add truncated s)
                           (.substring s 0 max-width)))))
        key-fn (or key-fn identity)
        s1i (for-map [[i x] (indexed s1)] (key-fn x) i)
        s2i (for-map [[i x] (indexed s2)] (key-fn x) i)]
    (->> (map (fn [x1 x2 i] {:ldelta (if-let [j (s2i (key-fn x1))] (- j i) nil)
                             :left (truncate x1) :right (truncate x2)
                             :rdelta (if-let [j (s1i (key-fn x2))] (- j i) nil)})
              (pad-to s1 s2) (pad-to s2 s1) (iterate inc 0))
         (take (or limit 100))
         (pprint/print-table))
    (when (seq truncated)
      (println "Key:")
      (doseq [x (distinct truncated)]
        (println x)))))


;; adapted from clojure.contrib.java-utils

(defn method-wall-hacker [class method-name params]
  (let [m (-> class
              (.getDeclaredMethod (name method-name) (into-array Class params))
              (doto (.setAccessible true)))]
    (fn [obj & args]
      (.invoke m obj (into-array Object args)))))

(defn wall-hack-method
  "Calls a private or protected method.
   params is a vector of class which correspond to the arguments to the method
   obj is class for static methods, the instance object otherwise
   the method name is given as a symbol or a keyword (something Named)"
  [obj method-name params & args]
  (apply (method-wall-hacker (if (class? obj) obj (class obj)) method-name params) obj args))

(defn field-wall-hacker [class field-name]
  (let [m (-> class
              (.getDeclaredField (name field-name))
              (doto (.setAccessible true)))]
    (fn [o] (.get m o))))

(defn wall-hack-field [o field-name]
  ((field-wall-hacker (class o) field-name) o))

(let [hacker (field-wall-hacker String :value)]
  (defn is-substring? [^String s]
    (not= (count s) (count (hacker s)))))


(def +locals+ (atom nil))
(def +done+ (atom false))

(defmacro ghetto-debugger
  "Non-thread-safe, ghetto debugger that pauses execution and lets you
   call (locals) to see locals, resuming when you call (resume)"
  []
  `(do (println "Pausing at" ~*file* ~(meta &form))
       (reset! +locals+ ~(for-map [s (keys &env)]
                           `'~s s))
       (reset! +done+ false)
       (parallel/wait-until #(deref +done+) 100000 10)
       (reset! +locals+ nil)))

(defn locals [] @+locals+)

(defn resume [] (reset! +done+ true))