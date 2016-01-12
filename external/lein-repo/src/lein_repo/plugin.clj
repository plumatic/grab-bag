(ns lein-repo.plugin
  (:use [clojure.java.io :only [copy file]])
  (:require [clojure.xml :as xml]
            [clojure.set :as set]
            [clojure.pprint :as pprint]
            [clojure.java.shell :as shell]
            leiningen.core.project)
  (:import [java.io File]
           [java.util HashMap Stack Queue]
           [java.util.jar Manifest JarEntry JarOutputStream]))

(set! *warn-on-reflection* true)

;; Finding repo and its files
(def lein-version "0.2.1")

(defn pwd [] (System/getProperty "user.dir"))

(defn ^String projects-file [^String dir]
  (str dir "/projects.clj"))

(defn ^String find-repo-root [^String starting-dir]
  (cond (nil? starting-dir) nil ;;(throw (RuntimeException. "No enclosing repo setup found"))
        (.exists (File. (projects-file starting-dir))) starting-dir
        :else (recur (.getParent (File. starting-dir)))))

(def repo-root (find-repo-root (pwd)))

(defn index-by [k m]
  (into {} (for [[k elts] (group-by k m)]
             [k (do (assert (= 1 (count elts)) (str "duplicate projects.clj deps: " k))
                    (first elts))])))

(def repo-config
  (when repo-root
    (-> repo-root
        projects-file
        slurp
        read-string
        (update-in ['external-dependencies] #(index-by first %)))))

;; for jason
(def read-project-file
  (memoize
   (fn [project-name]
     (let [project-dir (get-in repo-config ['internal-dependencies project-name])]
       (when-not project-dir
         (throw (RuntimeException. (str "Could not find project folder for " project-name))))
       (let [project-clj (apply str (conj (if (.isAbsolute (File. ^String project-dir))
                                            [project-dir]
                                            [repo-root "/" project-dir]
                                            ) "/project.clj"))]
         (leiningen.core.project/read project-clj))))))

(defn topological-sort [child-map]
  (when (seq child-map)
    (let [sources (apply set/difference
                         (set (keys child-map))
                         (map set (vals child-map)))]
      (assert (seq sources))
      (concat (topological-sort (apply dissoc child-map sources))
              sources))))

(defn ordered-projects [project]
  (topological-sort
   (into {}
         (for [p (:internal-dependencies project)]
           [p (:internal-dependencies (read-project-file p))]))))

(defn distinct-deps
  "Distinctify deps, keeping the sole version of any repeated deps or
   the version marked with ^:force in projects, and otherwise
   throwing if there are multiple required versions."
  [deps]
  (for [[dep specs] (group-by first deps)]
    (or (when (= 1 (count (distinct specs)))
          (first specs))
        (when-let [forced (seq (distinct (filter #(:force (meta %)) specs)))]
          (assert (= 1 (count forced)) (str "Multiple forced specs: " forced))
          (first forced))
        (throw (RuntimeException.
                (str "\nCONFLICTING DEP: " dep " has "
                     (count specs) " versions required: "
                     (vec specs)))))))

(defn subdirs [^String d]
  (let [f (java.io.File. d)]
    (when (.isDirectory f)
      (for [^java.io.File s (.listFiles f)
            :when (.isDirectory s)]
        (.getAbsolutePath s)))))


;; TODO: fail on exotic lein commands

(declare middleware)
(def read-middlewared-project-file (memoize #(middleware (read-project-file %))))

(defn merge-with-keyfn
  ([merge-fns] {})
  ([merge-fns m] m)
  ([merge-fns m1 m2]
     (into {}
           (for [k (distinct (concat (keys m1) (keys m2)))]
             [k
              (cond (not (contains? m1 k)) (get m2 k)
                    (not (contains? m2 k)) (get m1 k)
                    :else ((merge-fns k) (m1 k) (m2 k)))])))
  ([merge-fns m1 m2 & more]
     (reduce (partial merge-with-keyfn merge-fns) m1 (cons m2 more))))

(def concat-distinct
  (comp distinct concat))

(declare merge-projects)

(def project-merge-fns
  {:dependencies concat-distinct
   :external-dependencies concat-distinct
   :resource-paths concat-distinct
   :source-paths concat-distinct
   :java-source-paths concat-distinct
   :dev-dependencies concat-distinct
   :repositories merge
   :plugins concat-distinct
   :repl-options (fn [p1 p2] (merge-with concat-distinct p1 p2))
   :profiles (fn [p1 p2] (merge-with merge-projects p1 p2))

   ;; cljx specific
   :prep-tasks concat-distinct
   :cljx (fn [a b] {:builds (concat-distinct (:builds a) (:builds b))})})

(defn fix-cljx
  "Cljx paths are not properly normalized like other source paths; manually ensure they are
   absolute."
  [p]
  (if (and (:cljx p)
           (let [^String path (-> p :cljx :builds first :output-path)]
             (not (.startsWith path "/"))))
    (let [target ^String (:target-path p)
          dir (subs target 0 (- (count target) 6))]
      (assert (.endsWith target "target"))
      (update-in p [:cljx :builds]
                 (partial
                  mapv
                  (fn [m]
                    (-> m
                        (update-in [:output-path] #(str dir %))
                        (update-in [:source-paths] (fn [x] (mapv #(str dir %) x))))))))
    p))

(defn normalize-project [p]
  (-> p
      fix-cljx
      (select-keys (keys project-merge-fns))
      (update-in [:repositories] (partial into {}))))

(defn merge-projects [& projects]
  (->> projects
       (map normalize-project)
       (apply merge-with-keyfn project-merge-fns)))

(declare test-all-project*)

(defn middleware [my-project]
  (cond (not repo-root)
        my-project

        (:mega? my-project) ;; include everything
        (-> (test-all-project* (dissoc my-project :mega?))
            (update-in [:dependencies] (fn [d] (->> d (concat (:dependencies my-project)) distinct-deps)))
            (merge (select-keys my-project [:jvm-opts])))

        :else
        (let [subprojects  (keep read-middlewared-project-file (:internal-dependencies my-project))
              my-augmented-project (apply merge-projects my-project subprojects)
              {:keys [dependencies java-source-paths external-dependencies]} my-augmented-project
              deps (->> dependencies
                        (concat (get repo-config 'required-dependencies))
                        (concat (for [dep external-dependencies]
                                  (let [spec (get-in repo-config ['external-dependencies dep])]
                                    (assert spec (str "Missing external dep " dep))
                                    spec)))
                        distinct-deps)]
          ;; Services need a deploy suck
          (when (:service? my-project)
            (spit (str (:root my-project) "/crane_deploy.clj")
                  (format "(ns crane-deploy (:use service.core))(deploy-suck %s)" (:name my-project))))
          (-> my-project
              (merge my-augmented-project)
              (assoc
                  :dependencies deps
                  :jvm-opts (with-meta (or (get my-project :jvm-opts) ["-Xmx4000m" "-XX:MaxPermSize=256m"]) {:replace true})
                  :direct-source-paths (:source-paths my-project)
                  :test-selectors
                  {:fast '(fn [m#] (not (some m# [:system :bench :integration :flaky :perf :slow])))
                   :unit '(fn [m#] (not (some m# [:system :bench :integration :flaky :perf])))
                   :all '(fn [m#] true)})
              (update-in [:filespecs] concat
                         (when (:service? my-project)
                           [{:type :bytes
                             :path "crane_deploy.clj"
                             :bytes (slurp (str (:root my-project)
                                                "/crane_deploy.clj"))}]))))))

(def +blacklisted-projects+
  ;; Clojurescript doesn't play nice with riemann-client currently.
  #{'cljs-request 'tubes 'web-frontend 'om-tools})

(defn all-internal-deps []
  (remove
   +blacklisted-projects+
   (keys (repo-config 'internal-dependencies))))

(def all-source-project
  (delay
   (let [internal-deps (all-internal-deps)]
     (-> (read-project-file (first internal-deps))
         (assoc :internal-dependencies internal-deps :test-paths [])
         (update-in [:dependencies] conj ['lein-repo lein-version])
         middleware
         (assoc :root repo-root
                :eval-in :subprocess
                :jvm-opts ^:replace ["-Xmx600m" "-XX:MaxPermSize=256m"])))))


(defn test-all-project* [base]
  (let [internal-deps (all-internal-deps)
        internal-projs (map read-project-file internal-deps)
        test-paths (distinct (mapcat :test-paths internal-projs))]
    (-> base
        (assoc :internal-dependencies internal-deps
               :test-paths test-paths)
        (update-in [:dependencies] conj ['lein-repo lein-version])
        middleware
        (update-in [:source-paths] concat test-paths)
        (dissoc :warn-on-reflection)
        (assoc :root repo-root
               :eval-in :subprocess
               :jvm-opts ^:replace ["-Xmx1000m" "-XX:+UseConcMarkSweepGC" "-XX:+CMSClassUnloadingEnabled" "-XX:MaxPermSize=512M"]))))

(def test-all-project
  (delay (test-all-project*
          (read-project-file (first (all-internal-deps))))))

(defn ordered-source-and-test-paths [& [no-tests?]]
  (let [mega-project @test-all-project
        blacklist []]
    (->> (ordered-projects mega-project)
         (mapcat (fn [p] (let [f (read-project-file p)]
                           (concat (when-not no-tests? (:test-paths f)) (:source-paths f)))))
         (remove (fn [^String n] (some #(>= (.indexOf n ^String %) 0) blacklist))))))

(set! *warn-on-reflection* false)
