(ns lein-repo.test
  (:require
   [clojure.test :as ct]
   [clojure.tools.namespace.find :as namespace-find]
   [lazytest.tracker :as tracker]))


;; Import all clojure.test stuff
(doseq [v (keys (ns-publics (find-ns 'clojure.test)))]
  (when-not (or (#{"deftest" "deftest-"} (name v))
                (.startsWith ^String (name v) "*"))
    (let [ct-sym (symbol "clojure.test" (name v))]
      (eval `(alter-meta!
              (def ~v @(var ~ct-sym))
              #(merge % (meta (var ~ct-sym))))))))


(def ^:dynamic *time-tests* false)
(def ^:dynamic *report-ns-results* false)

(def test-labels #{:slow :unit :system :integration :flaky :bench :perf})


(defmacro with-out-strs [& body]
  `(let [w# (java.io.StringWriter.)]
     (binding [*out* w *err* w]
       ~@body
       (str w#))))

;; Following functions adapted from clojure.test
(defn test-vars [filter-fn ns]
  (let [once-fixture-fn (ct/join-fixtures (::ct/once-fixtures (meta ns)))
        each-fixture-fn (ct/join-fixtures (::ct/each-fixtures (meta ns)))]
    (once-fixture-fn
     (fn []
       (doseq [v (vals (ns-interns ns))]
         (when (and (:test (meta v)))
           (assert (<= (count (filter (meta v) [:unit :system :integration])) 1))
           (let [weird-keys
                 (remove (into test-labels #{:ns :name :file :line :test :column})
                         (keys (meta v)))]
             (when (seq weird-keys)
               (println "WARNING: got unexpected metadata keys" weird-keys "for" (meta v))))
           (if (filter-fn (meta v))
             (if *time-tests*
               (let [start (System/currentTimeMillis)]
                 (each-fixture-fn (fn [] (ct/test-var v)))
                 (let [elapsed (- (System/currentTimeMillis) start)]
                   (when (> elapsed 20)
                     (println elapsed "\tms for test" v))))
               (each-fixture-fn (fn [] (ct/test-var v))))
             (ct/inc-report-counter [:skip (filter (meta v) test-labels)]))))))))

(defn skip-map [report]
  (into {}
        (for [[k v] report
              :when (and (vector? k) (= (first k) :skip))]
          [(vec (second k)) v])))

(defn test-ns [filter-fn ns]
  (binding [ct/*report-counters* (ref ct/*initial-report-counters*)]
    (let [ns-obj (the-ns ns)]
      (do-report {:type :begin-test-ns, :ns ns-obj})
      (test-vars filter-fn ns-obj)
      (let [rep @ct/*report-counters*
            {:keys [test pass fail error]} rep
            skips (skip-map rep)]
        (when *report-ns-results*
          (println (format "%s pass, %s fail, %s error, %s skipped %s"
                           (- test fail error) fail error
                           (reduce + (map val skips))
                           (if (seq skips)
                             (str "with tags " skips) "")))))
      (do-report {:type :end-test-ns, :ns ns-obj}))
    @ct/*report-counters*))

(defn io-filter [includes excludes]
  #(and (every? % includes)
        (not-any? % excludes)))

(defn run-tests [& args]
  (let [[filter namespaces] (if (fn? (first args))
                              [(first args) (next args)]
                              [(constantly true) args])
        summary (assoc (apply merge-with + (map (partial test-ns filter) namespaces))
                  :type :summary)]
    (println ".")
    (let [skips (skip-map summary)]
      (when-not (empty? skips)
        (println (format "%s tests skipped with tags %s"
                         (reduce + (vals skips))
                         skips))))
    (do-report summary)
    (.flush *out*)
    summary))

(defn run-all-tests
  ([] (run-all-tests (constantly true)))
  ([filter-fn]
     (apply run-tests filter-fn (all-ns))))

(defn run-all-unit-tests []
  (run-all-tests (io-filter #{:unit} #{:flaky :perf})))


(def filter-selectors
  {:fast      [#{} #{:flaky :perf :system :integration :bench :slow}]
   :unit      [#{} #{:flaky :perf :system :integration :bench}]
   :all       [#{} #{:bench}]
   :unlabeled [#{} #{:unit :flaky :perf :system :integration :bench :slow} true]})

(defn run-tests-selector [selector nss]
  (let [[pos neg time?] (filter-selectors selector)]
    (binding [*time-tests* time?]
      (apply run-tests (io-filter pos neg) nss))))


(defn selector-filter [selector]
  (let [[pos neg] (get filter-selectors selector)]
    (when-not pos (throw (RuntimeException. (str "Unknown selector " selector))))
    (io-filter pos neg)))

(defn run-until-success [f]
  (while
      (try (f)
           false
           (catch Exception e
             (.printStackTrace e)
             (println "Waiting for files to load correctly.")
             (Thread/sleep 1000)
             true))))



(defn watch-tests [dirs &
                   {:keys [preprocess-nss test-selector skip-system-tests?]
                    :or {preprocess-nss (partial filter #(.endsWith ^String (name %) "-test"))
                         test-selector :all
                         skip-system-tests? false}}]
  (when-not (contains? filter-selectors test-selector)
    (throw (RuntimeException. (format "%s is not a valid test selector -- choose one of %s"
                                      test-selector (vec (keys filter-selectors))))))
  (let [run-tests #(run-tests-selector %1 (preprocess-nss %2))
        track (comp sort
                    (if skip-system-tests?
                      (partial remove #(>= (.indexOf ^String (name %) "system-test") 0))
                      identity)
                    (tracker/tracker (map #(java.io.File. %) dirs) 0))
        init (track)
        cmds (str "\nPress enter to force a full re-test, or enter a new selector type from "
                  (keys filter-selectors))]
    (println "Doing initial require of namespaces:" init)
    (comment
      (doseq [n init]
        (println "requiring" n)
        (require n)))
                                        ;(println (all-ns))
    (run-until-success #(apply require init))
    (println "running initial tests")
    (run-tests test-selector (preprocess-nss init))
    #_ (println cmds)

    (while true
      (Thread/sleep 500)
      (when-let [changed (seq (track))]
        (dotimes [i 100] (println ".\n"))
        (println "Reloading affected namespaces:" changed)
        (run-until-success #(apply require :reload changed))
        (if-let [tests (seq (preprocess-nss changed))]
          (do (apply println ".\nRerunning tests" tests)
              (run-tests test-selector tests)
              (println "Kicking off full reload in background...")
              (future #(do (apply require :reload-all init) (println "Done reloading."))))
          (println "No tests seem to be affected.  Shame on you."))
        #_ (println cmds)))))


;; lazytest is getting extraordinarily slow, use tools.namespace here for now.
(defn test-dirs [dirs &
                 {:keys [preprocess-nss test-selector skip-system-tests?]
                  :or {preprocess-nss (partial filter #(.endsWith ^String (name %) "-test"))
                       test-selector :all}}]
  (when-not (contains? filter-selectors test-selector)
    (throw (RuntimeException. (format "%s is not a valid test selector -- choose one of %s"
                                      test-selector (vec (keys filter-selectors))))))
  (println "Testing with selector " test-selector " on dirs:"  dirs)
  (let [namespaces (sort (mapcat #(namespace-find/find-namespaces-in-dir (java.io.File. %)) dirs))]
    ;; (doseq [i init] (println "requiring" i) (require i))
    (apply require namespaces)
    (run-tests-selector test-selector (preprocess-nss namespaces))))
