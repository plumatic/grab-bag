(ns classify.libsvm
  "Experimental.
   Output data in libsvm format for consumption by liblinear/xgboost/etc.
   Currently only useful for human consumption / comparison with error rate from
   linear models; if we see success there, then will need to be further developed."
  (:use plumbing.core)
  (:require
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [plumbing.math :as math]
   [classify.features :as features]
   [classify.index :as index]))


(set! *warn-on-reflection* true)

(defn libsvm-data-line
  "Assumes labels have already been transformed/formatted.  Uses 0-based XGBoost format."
  [[datum label w]]
  (assert (= (double (or w 1.0)) 1.0))
  (apply str
         (if (instance? Boolean label) (if label 1 0) label)
         (for [[i d] (sort-by key datum)
               :when (not (zero? d))]
           (format " %d:%s" i (math/compact-str d)))))

(defn libsvm-data-file
  [i-data]
  (str/join "\n" (map libsvm-data-line i-data)))

(defn libsvm-feature-map
  [p-index & [format-feature]]
  (->> p-index
       (map-indexed (fn [i f]
                      (format "%s\t%s\tq" i ((or format-feature identity) f))))
       (str/join "\n")))

(defn xgboost-conf-file
  [opts]
  (->> opts
       (map (fn [[k v]] (format "%s = %s" k (pr-str v))))
       (str/join "\n")))

(defn libsvm-data-files
  "Write data files for libsvm / xgboost.  Optional feature-set yields nicer names if data
   is already pre-indexed."
  [train-data test-data index-data-opts & [feature-set]]
  (let [[i-data p-index] (index/convert-and-index-data train-data index-data-opts)
        i-test-data (for [[d l] test-data]
                      [(index/convert-preds d p-index (:normalize? index-data-opts)) l])]
    {:train-data.txt (libsvm-data-file i-data)
     :test-data.txt (libsvm-data-file i-test-data)
     :features.txt (libsvm-feature-map
                    p-index
                    (when feature-set
                      (fn [i]
                        (let [[t k] (features/value-of feature-set i)]
                          (.replaceAll (str (name t) "_" k) " " "_")))))}))

(def +default-binary-opts+
  "Opts for binary, labels should be 0/1.  See https://github.com/dmlc/xgboost/tree/master/demo"
  {;; general params
   "booster" 'gbtree
   "objective" 'binary:logistic

   ;; tree booster params
   "eta" 1.0 ;; step size shrinkage
   "gamma" 1.0 ;; min loss reduction to partition
   "min_child_weight" 1 ;; min sum of instance weight (hessian) in child
   "max_depth" 3

   ;; task params
   "num_round" 2
   "save_period" 0 ;; only save last model
   "data" "train-data.txt"
   "eval[test]" "test-data.txt"
   "test:data" "test-data.txt"
   "eval_train" 1 ;; eval training data on each round
   })

(def +default-regression-opts+
  (assoc +default-binary-opts+
    "objective" 'reg:linear))

(defn write-xgboost! [path data-files]
  (doseq [[p f] data-files]
    (spit (str path "/" (name p)) f)))

(defn run-cmd! [c]
  (let [{:keys [exit] :as res} (apply shell/sh c)]
    (assert (zero? exit))
    res))

(defn run-xgboost [xgboost-path conf train-data test-data index-data-opts & [feature-set]]
  (let [d "/tmp/xgboost/"] ;io/with-test-dir [d "/tmp/xgboost/"]
    (shell/with-sh-dir d
      (write-xgboost!
       d
       (assoc (libsvm-data-files train-data test-data index-data-opts feature-set)
         :job.conf (xgboost-conf-file conf)))
      (let [base-cmd [xgboost-path "job.conf"]
            o1 (run-cmd! base-cmd)]
        (run-cmd! (concat base-cmd
                          ["task=dump"
                           (format "model_in=%04d.model" (safe-get conf "num_round"))
                           "fmap=features.txt" "name_dump=model.txt"]))
        (assoc (select-keys o1 [:out :err])
          :model (slurp (str d "model.txt")))))))

(set! *warn-on-reflection* false)
