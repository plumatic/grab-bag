(ns plumbing.organize-ns
  (:use plumbing.core)
  (:require
   [clojure.java.io :as java-io]
   [clojure.pprint :as pprint]
   [clojure.walk :as walk]
   [clojure.set :as set]
   [clojure.string :as str]
   [lazytest.tracker :as tracker]
   [plumbing.io :as io]
   [plumbing.error :as err])
  (:import
   [clojure.lang RT]
   [java.io PushbackReader StringReader File]))

;; Portions ripped off from slamhound, under EPL.

;; From slamhound.asplode
(defn- ns->map [ns-form]
  (let [[better-be-ns ns-name maybe-doc & clauses] ns-form
        ns-meta (meta ns-name)
        [ns-meta clauses] (if (string? maybe-doc)
                            [(assoc ns-meta :doc maybe-doc) clauses]
                            [ns-meta (cons maybe-doc clauses)])]
    (assert (= better-be-ns 'ns))
    (apply merge-with concat
           {:meta ns-meta :name ns-name}
           (for [[f & r] clauses]
             {f r}))))

(defn str->forms [^String src]
  (let [rdr (-> src StringReader. PushbackReader.)]
    (take-while #(not= ::done %)
                (repeatedly #(read rdr false ::done)))))

(defn- read-ns [rdr]
  (let [rdr (PushbackReader. rdr)]
    [(ns->map (read rdr))
     (slurp rdr)]))

;; From slamhound.stitch

(def ^{:private true} ns-clauses [:use :require :import])

(def ^{:private true} ns-clauses-to-preserve [:refer-clojure :gen-class :load])


(declare +our-ns-order+)

(defn ns-topo-score [ns-sym]
  (if (.startsWith ^String (name ns-sym) "clojure")
    -2
    (or (+our-ns-order+ (symbol (first (.split ^String (name ns-sym) "\\."))))
        -1)))

(defn- sort-clause [xs]
  (sort-by
   (fn [x]
     (let [n (if (coll? x) (first x) x)]
       [(ns-topo-score n) (str n)]))
   xs))

(defn- map->ns [ns-map]
  `(~'ns ~(:name ns-map)
     ~@(if-let [doco (:doc (:meta ns-map))] ; avoid inserting nil
         [doco])
     ~@(for [clause-type (concat ns-clauses-to-preserve ns-clauses)
             :when (seq (clause-type ns-map))]
         (cons clause-type (sort-clause (clause-type ns-map))))))

(defn- prettify [ns-form]
  (let [s (with-out-str
            (pprint/with-pprint-dispatch pprint/code-dispatch
              (pprint/pprint ns-form)))]
    (-> ^String s
        (.replace "(ns\n " "(ns")
        (.replace ":require " ":require\n    ")
        (.replace "            " "    ")
        (.replace ":import " ":import\n    ")
        (.replace "           " "    ")
        (.replace "] [" "]\n    ["))))

(defn- map->ns-str [ns-map]
  (-> ns-map map->ns prettify))


;; From slamhound.regrow

(defn ns-load-error [ns-map body & [form]]
  (let [sandbox-ns `slamhound.sandbox#
        ns-form (map->ns (assoc ns-map :name sandbox-ns))]
    (binding [*ns* (create-ns sandbox-ns)]
      (try
        (eval `(do ~ns-form ~@body ~form))
        (catch Exception e
          e)
        (finally
          (remove-ns (.name *ns*)))))))

(defn missing-symbol [^Exception e]
  (when-let [^String ss
             (second (re-find #"Unable to resolve \w+: ([-_\w\$\?!\*\>\<]+)" (.getMessage e)))]
    (assert (not (some #{\/} ss)))
    (let [ss (.trim ss)]
      (assert (seq ss))
      (symbol ss))))

(def ns-loads? (complement ns-load-error))

;; Mine

(defn resolve-filename [^String f]
  (if (.startsWith f "/")
    f
    (.getFile (.getResource (RT/baseLoader) f))))

(defn ns-sym->filename [ns-sym]
  (let [pubs (-> ns-sym the-ns ns-publics)
        metas (map (comp meta val) pubs)
        ns-metas (filter #(when-let [ns (:ns %)] (= (ns-name ns) ns-sym)) metas)
        f (first (keep :file ns-metas))]
    (when-not f
      (throw (RuntimeException. (format "Couldn't find file for ns %s" ns-sym))))
    (assert f)
    (resolve-filename f)))

(def ^:dynamic *messages* nil)
(defn message! [& args]
  (swap! *messages* conj (apply format args)))

(defn prune-clause-helper
  "Remove unneeded elts and message about them "
  [loads? reconstruct message! elts]
  (let [elts (set elts)
        [kill keep] ((juxt filter remove) #(loads? (reconstruct (disj elts %))) elts)]
    (doseq [e kill]
      (message! e))
    (reconstruct keep)))


(defmulti prune-clause (fn [type loads? entry] type))

(defmethod prune-clause :use [_ loads? e]
  (if (and (coll? e) (= (second e) :only))
    (prune-clause-helper
     loads?
     #(vector (first e) :only (vec (sort %)))
     #(message! "Removed superflous :use :only %s from %s" % e)
     (nth e 2))
    e))

(defn map->clause [c]
  (->> c
       (filter (fn [[k v]] (or (not (coll? v)) (seq v))))
       (sort-by key)
       (apply concat)))

(declare require-as)
(defmethod prune-clause :require [_ loads? e]
  (if (symbol? e)
    e
    (let [[req-ns-sym & clause-data] e
          clause-map (apply hash-map clause-data)
          gold-as-sym (require-as req-ns-sym)]
      (assert (symbol? req-ns-sym))
      (when-let [as-sym (:as clause-map)]
        (assert (symbol? as-sym))
        (when-not (.endsWith ^String (name as-sym) ^String (name gold-as-sym))
          (message! "WARNING, PLEASE FIX! Bad alias of %s: %s should be %s."
                    req-ns-sym as-sym gold-as-sym)))
      (if-let [refers (:refer clause-map)]
        (prune-clause-helper
         loads?
         #(into [req-ns-sym] (map->clause (assoc clause-map :refer (vec (sort %)))))
         #(message! "Removed superflous :refer %s from %s" % e)
         refers)
        e))))

(defmethod prune-clause :import [_ loads? e]
  (if (coll? e)
    (prune-clause-helper
     loads?
     #(vec (cons (first e) (sort %)))
     #(message! "Removed superflous :import %s from %s" % e)
     (set (next e)))
    e))

(def +allowed-unused-use+ '#{plumbing.core})

(defn remove-stowaways [ns-map body known-used-aliases clause]
  (let [all-clauses (set (clause ns-map))]
    (->> (reduce
          (fn [cur-clauses entry]
            (let [rem-clauses (disj cur-clauses entry)
                  loads?      #(ns-loads?
                                (assoc ns-map clause
                                       (concat (when % [%]) rem-clauses))
                                body)]
              (if (and (not (or (and (= clause :require)
                                     (or (symbol? entry)
                                         (contains? known-used-aliases (last entry)))) ;; for speec
                                (and (= clause :use)
                                     (+allowed-unused-use+ entry))))
                       (loads? nil))
                (do (message! "Removed superflous %s %s" clause entry)
                    rem-clauses)
                (conj rem-clauses (prune-clause clause loads? entry)))))
          all-clauses
          all-clauses)
         vec
         (assoc ns-map clause))))

;;;;; Top-level -- just simplifying ns declaration

(defn known-utilized-namespace-aliases [ns-body]
  (let [a (atom #{})]
    (walk/postwalk
     #(do (when (symbol? %)
            (when-let [ns (namespace %)]
              (swap! a conj (symbol ns))))
          %)
     ns-body)
    @a))

(defn organized-ns-decl-data [ns-sym]
  (let [f (ns-sym->filename ns-sym)
        [ns-map body-str] (-> f java-io/reader read-ns)
        body (str->forms body-str)
        e (ns-load-error ns-map body `(known-utilized-namespace-aliases '~body))]
    (when (instance? Exception e)
      (println (missing-symbol e))
      (println "ASDFSDF")
      (throw (RuntimeException. "Namespace does not seem to load as-is :(" ^Throwable e)))
    (set! *warn-on-reflection* false)
    (binding [*messages* (atom [])]
      (let [new-ns-map (reduce
                        #(remove-stowaways %1 body e %2)
                        ns-map ns-clauses)]
        {:ns-sym ns-sym
         :ns-file f
         :old-ns-map ns-map
         :new-ns-map new-ns-map
         :new-ns-str (map->ns-str new-ns-map)
         :body-forms body
         :body-str body-str
         :change-messages @*messages*}))))

(defn nrepl-organized-ns [form]
  (assert (= 'ns (first form)))
  (assert (symbol? (second form)))
  (let [data (organized-ns-decl-data (second form))]
    (if-let [msgs (seq (:change-messages data))]
      (apply str (:new-ns-str data)
             "\n" (map #(format ";; %s\n" %) msgs))
      (:new-ns-str data))))

(defn organize-ns-decl! [ns-sym]
  (println "Organizing namespace" ns-sym "at file" (ns-sym->filename ns-sym))
  (let [d (organized-ns-decl-data ns-sym)]
    (if-let [msgs (:change-messages d)]
      (doseq [m msgs] (println m))
      (println "Nothing to remove; just sorting clauses"))
    (spit (:ns-file d)
          (str (:new-ns-str d) "\n\n" (.trim ^String (:body-str d)) "\n"))))

(defn all-namespace-symbols [& [prefix-str]]
  (when prefix-str (assert (string? prefix-str)))
  (let [prefix-str ^String (or prefix-str "")]
    (->> (all-ns)
         (map ns-name)
         (filter #(.startsWith ^String (name %) prefix-str)))))

(defn organize-all-nss-decls! [& [^String prefix-str]]
  (let [all-nss (all-namespace-symbols prefix-str)]
    (println "Operating on namespaces:" all-nss)
    (println "\n")
    (doseq [ns-sym all-nss]
      (organize-ns-decl! ns-sym)
      (println))))


;;;;;; Crazy shit -- try to fix uses and requires
(defn pwd [] (System/getProperty "user.dir"))

(defn ^String projects-file [^String dir]
  (str dir "/projects.clj"))

(defn ^String find-repo-root [^String starting-dir]
  (cond (nil? starting-dir) nil ;;(throw (RuntimeException. "No enclosing repo setup found"))
        (.exists (File. (projects-file starting-dir))) starting-dir
        :else (recur (.getParent (File. starting-dir)))))

(def repo-root (find-repo-root (pwd)))

(def repo-config (when repo-root (-> repo-root projects-file slurp read-string)))

(def read-project-file
  (memoize
   (fn [project-name]
     (let [project-dir (get-in repo-config ['internal-dependencies project-name])]
       (when-not project-dir
         (throw (RuntimeException. (str "Could not find project folder for " project-name))))
       (let [project-clj (apply str (conj (if (.isAbsolute (File. ^String project-dir))
                                            [project-dir]
                                            [repo-root "/" project-dir]
                                            ) "/project.clj"))
             [_ project-name version & kvs] (read-string (slurp project-clj))]
         (assoc (apply hash-map kvs)
           :project-name project-name
           :version version))))))

(defn topological-sort [child-map]
  (when (seq child-map)
    (let [sinks (set/difference
                 (set (mapcat val child-map))
                 (set (map key (filter (comp seq val) child-map))))
          child-map (apply dissoc (map-vals (partial remove sinks) child-map) sinks)
          sources (apply set/difference
                         (set (keys child-map))
                         (map set (vals child-map)))]
      (assert (some seq [sources sinks]))
      (concat (sort sinks)
              (topological-sort (apply dissoc child-map sources))
              (sort sources)))))

(def +our-ns-order+
  (->> (get repo-config 'internal-dependencies)
       keys
       (map-from-keys (fn-> read-project-file :internal-dependencies))
       topological-sort
       (map-indexed (fn [i n] [n i]))
       (into {})))

(def +whitelist-use+
  '#{plumbing.core
     clojure.test})

(def +custom-require-as+
  '{clojure.string str
    clojure.tools.logging log
    hiphip.double dbl
    hiphip.float flt
    hiphip.long lng
    flop.array fa
    lein-repo.project lrp
    lein-repo.core lrc
    plumbing.error err
    plumbing.logging log
    schema.core s
    plumbing.core-incubator pci
    clojure.java.jdbc.deprecated jdbc
    oauth.client oauth})

(defn can-use? [user-ns-sym used-ns-sym]
  (assert (symbol? user-ns-sym))
  (assert (symbol? used-ns-sym))
  (or (contains? +whitelist-use+ used-ns-sym)
      (and (.endsWith   ^String (name user-ns-sym) "test")
           (.startsWith ^String (name user-ns-sym) ^String (name used-ns-sym)))))

(defn require-as [ns-sym]
  (assert (symbol? ns-sym))
  (or (+custom-require-as+ ns-sym)
      (let [segments (.split ^String (name ns-sym) "\\.")]
        (symbol
         (last
          (if (= (last segments) "core")
            (drop-last segments)
            segments))))))

(defn remove-from-ns-map [ns-map k from]
  (let [from-l (k ns-map)]
    (assert (some #{from} from-l))
    (assoc ns-map k (remove #{from} from-l))))

(defn add-to-ns-map [ns-map k to]
  (update-in ns-map [k] conj to))

(defn replace-in-ns-map [ns-map k from to]
  (-> ns-map
      (remove-from-ns-map k from)
      (add-to-ns-map k to)))

(defn cleanup-missing-symbol [[ns-map body-str]]
  (let [e (ns-load-error ns-map (str->forms body-str))]
    (when e
      (or (missing-symbol e)
          (throw e)))))

(defn verify-load [p]
  (assert (not (cleanup-missing-symbol p)))
  p)

(defn fix-require [ns-sym [ns-map ^String body-str :as p] req]
  (if-not (coll? req)
    p
    (let [[req-ns-sym as as-sym] req
          gold-as-sym (require-as req-ns-sym)]
      (assert (symbol? req-ns-sym))
      (assert (symbol? as-sym))
      (assert (= as :as))
      (if (= gold-as-sym as-sym)
        p
        (do
          (message!
           "Replacing alias %s of %s with %s"
           as-sym req-ns-sym gold-as-sym)
          (verify-load
           [(replace-in-ns-map ns-map :require req [req-ns-sym :as gold-as-sym])
            (.replace body-str (str as-sym "/") (str gold-as-sym "/"))]))))))

(defn fix-use [ns-sym [ns-map ^String body-str :as p] use]
  (let [bare? (not (coll? use))
        used-ns-sym (if bare? use (first use))]
    (if (can-use? ns-sym used-ns-sym)
      p
      (let [req-as-sym (require-as used-ns-sym)
            new-ns-map (-> ns-map
                           (remove-from-ns-map :use use)
                           (add-to-ns-map :require [used-ns-sym :as req-as-sym]))]
        (verify-load
         [new-ns-map
          (if-not bare?
            (let [onlys (nth use 2)]
              (message! "Replacing :use %s :only %s with require :as %s"
                        used-ns-sym onlys req-as-sym)
              (assert (= (nth use 1) :only))
              (assert (every? symbol? onlys))
              (reduce
               (fn [^String b s]
                 (.replace b (str s) (str req-as-sym "/" s)))
               body-str
               onlys))
            (do
              (message! "Replacing bare :use %s with require :as %s..."
                        used-ns-sym req-as-sym)
              (loop [^String body-str body-str]
                (if-let [s (cleanup-missing-symbol [new-ns-map body-str])]
                  (do (message! "Caught bad symbol %s" s)
                      (assert (symbol? s))
                      (recur (.replace body-str (str s) (str req-as-sym "/" s))))
                  body-str))))])))))


;; tasks:
;; require :as -> canonical as
;; use :only -> require :as
;; base use -> require :as

(defn assert-distinct! [syms]
  (doseq [[s c] (frequencies syms)]
    (when (> c 1) (throw (RuntimeException. (str "Got duplicate namespace " s))))))

(defn cleanup-ns-data [ns-sym]
  (let [{:keys [new-ns-map body-str ns-file old-ns-map change-messages] :as decl-data}
        (organized-ns-decl-data ns-sym)

        uses (:use new-ns-map)
        reqs (:require new-ns-map)]
    (assert-distinct!
     (map #(if (coll? %) (first %) %) (concat uses reqs)))
    (binding [*messages* (atom [])]
      (try
        (let [[newer-ns-map newer-body]
              (reduce
               (partial fix-use ns-sym)
               (reduce
                (partial fix-require ns-sym)
                [new-ns-map body-str]
                reqs)
               uses)]
          (assoc decl-data
            :final-ns-map newer-ns-map
            :final-body-str newer-body
            :final-ns-str (map->ns-str newer-ns-map)
            :change-messages (concat change-messages @*messages*)))
        (catch Exception e
          (println "Failed to cleanup namespace" ns-sym ", last task" (last @*messages*))
          (throw e))))))

(defn cleanup-ns! [ns-sym]
  (println "Organizing namespace" ns-sym "at file" (ns-sym->filename ns-sym))
  (let [d (cleanup-ns-data ns-sym)]
    (if-let [msgs (seq (:change-messages d))]
      (doseq [m msgs] (println m))
      (println "No real changes; just sorting clauses"))
    (spit (:ns-file d)
          (str (:final-ns-str d) "\n\n" (.trim ^String (:final-body-str d)) "\n"))))

(defn cleanup-all-nss! [& [prefix-str-or-ns-sym-seq]]
  (let [all-nss (if (coll? prefix-str-or-ns-sym-seq)
                  prefix-str-or-ns-sym-seq
                  (all-namespace-symbols prefix-str-or-ns-sym-seq))]
    (assert (every? symbol? all-nss))
    (println "Cleaning up namespaces:" all-nss)
    (println "\n")
    (doseq [ns-sym all-nss]
      (try
        (cleanup-ns! ns-sym)
        (catch Exception e
          (.printStackTrace e)
          (println "Failed on ns" ns-sym (.getMessage e))))
      (println))))

(defn cleanup-all-dirs! [dirs]
  (println "Doing cleanup on dirs" dirs)
  (let [nss ((tracker/tracker (map #(java.io.File. ^String %) dirs) 0))]
    (println "Doing initial require of namespaces" nss)
    (apply require nss)
    (cleanup-all-nss! nss)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; for finding all requires and descendents for a given ns.
;;; not super general, only works on local code cos it looks directly at the file and infers
;;; the file location from the args.

(defn- ns->filename [base-dir the-ns prefix]
  (let [filename (-> the-ns
                     name
                     (.replaceAll "\\." "/")
                     (.replaceAll "-" "_")
                     (subs (count prefix))
                     (str ".cljs"))]
    (str base-dir filename)))

(defn- get-requires
  "find the required nses from the file contents"
  [file-contents-string prefix]
  (->> file-contents-string
       read-string
       (keep #(when (and (seq? %) (= :require (first %))) (drop 1 %)))
       aconcat
       (map #(if (seq %) (first %) %))
       (filter #(.startsWith (name %) prefix))))

;; example usage: (crawl-requires "~/grabbag/client/webapp/src/cljs/webapp" 'webapp.v2.core "webapp"
(declare crawl-requires)
(defn crawl-requires  [base-dir the-ns prefix & [require-counts]]
  (when-let [file-contents (err/silent slurp (ns->filename base-dir the-ns prefix))]
    (reduce
     (fn [existing an-ns]
       (merge-with
        (fnil + 0 0)
        existing
        {an-ns 1}
        (when-not (contains? existing an-ns)
          (crawl-requires base-dir an-ns prefix))))
     require-counts
     (get-requires file-contents prefix))))

(defn read-ns-form [file]
  "Reads and returns the first form from a file, the ns form"
  (with-open [rdr (PushbackReader. (java-io/reader file))]
    (read rdr)))

(defn namespaces-in-dir
  "Returns a sequence of namespaces defined in clj(s) files in directory including subdirs"
  [base-dir]
  (->> (io/list-files-recursively base-dir)
       (filter #(re-find #"\.cljs?$" (.getName %)))
       (map (comp second read-ns-form))
       (filter (complement nil?))))

(defn unused-namespaces [base-dir the-ns prefix]
  (let [defined (set (namespaces-in-dir base-dir))
        used (-> (crawl-requires base-dir the-ns prefix)
                 (keys)
                 (set))]
    (set/difference defined used)))
