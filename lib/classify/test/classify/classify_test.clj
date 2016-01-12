(ns classify.classify-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [flop.map :as map]
   [classify.features :as features]
   [classify.features-test :as features-test]
   [classify.learn :as learn]
   [classify.classify :as classify]))

(defn raw-data [& doc-args-and-labels]
  (assert (even? (count doc-args-and-labels)))
  (for [[d l] (partition 2 doc-args-and-labels)]
    [d l]))

(def +feature-set+ (features/feature-set [features-test/starts-with-letter-t]))

(defn featurized-data [& raw-data-args]
  (classify/featurize-data +feature-set+ {} (apply raw-data raw-data-args)))

(deftest featurize-data-test
  (is-= [[{:lexical {"testy" 1.0 "title" 1.0}} true]
         [{:lexical {"texty" 1.0}} false]]
        (map (juxt (comp (partial features/fv->nested-features +feature-set+) first) second)
             (classify/featurize-data
              +feature-set+ {}
              (raw-data
               {:title "a testy title" :text "no nonsense"} true
               {:title "an apple" :text "texty"} false)))))

(deftest keywise-merge-with-test
  (is-= {:a 18 :b 4 :c 16}
        (classify/keywise-merge-with
         #(apply + (map (partial * 2) %&))
         {:a 1
          :b 2}
         {:c 3}
         {:a 3
          :c 5}
         {:a 5})))

(deftest confusion-test
  (is-= {[true true] 2.0
         [true false] 1.0
         [false false] 3.0}
        (map-vals
         (partial sum second)
         (learn/evaluate-all
          classify/classification-evaluator
          (classify/indexed-troves->model (map-vals map/map->trove {true {0 1 1 -2} false {}}))
          (for [[wm l] {{0 4} true
                        {0 4 1 1} true
                        {0 4 1 3} true
                        {1 1} false
                        {0 -1} false
                        {0 -2} false}]
            [(map/map->fv (map-vals double wm)) l])))))

(def +dummy-data-args+ (apply concat (for [i (range 10)] [{:title (str "title" (mod i 4)) :text (str "text" (mod i 6))} (odd? i)])))
(def +dummy-data+ (apply featurized-data +dummy-data-args+))

(deftest train-and-eval-smoke-test
  (letk [[model] (learn/train-and-eval
                  (classify/maxent-trainer {:pred-thresh 1})
                  classify/classification-evaluator
                  +dummy-data+
                  +dummy-data+)]
    (let [datum {:title "title1 title2" :text "text1"}
          score (fn [fs m d]
                  (classify/posteriors m (features/feature-vector fs {} d)))
          old-scores (score +feature-set+ model datum)
          model-data (classify/model->nested-weight-maps +feature-set+ model)
          new-feature-set (features/feature-set [features-test/starts-with-letter-t])
          new-model (classify/nested-weight-maps->model new-feature-set model-data)
          new-scores (score new-feature-set new-model datum)]
      (is-= old-scores new-scores)
      (is (= {true {:lexical 8} false {}} (map-vals (partial map-vals count) model-data))))))

(deftest ^:slow estimate-perf-and-train-model-smoke-test
  (is (learn/estimate-perf-and-train-model
       {:trainer (classify/maxent-trainer {:pred-thresh 1})
        :evaluator classify/classification-evaluator
        :data +dummy-data+})))

(deftest multiclass-smoke-test
  (let [data [[{:title "titlea" :text "texta"} :a]
              [{:title "titleb" :text "textb"} :b]
              [{:title "titlec" :text "textc"} :c]]
        featurized (apply featurized-data (aconcat data))]
    (letk [[model evaluation] (learn/train-and-eval
                               (classify/maxent-trainer {:pred-thresh 1})
                               classify/classification-evaluator
                               (aconcat (repeat 3 featurized))
                               featurized)]
      (is-= {[:a :a] 1.0
             [:b :b] 1.0
             [:c :c] 1.0}
            (map-vals (partial sum second) evaluation))
      (is-= [:a :b :c] (map #(learn/label model (first %)) featurized)))))

(deftest em-smoke-test
  "Test that we can learn by transferring from features on labeled data
   to features on unlabeled data through shared features."
  (let [labeled (featurized-data
                 {:title "titlea" :text ""} true
                 {:title "titleb" :text ""} false)
        unlabeled (map
                   first
                   (featurized-data
                    {:title "titlea" :text "transfera"} :ignore
                    {:title "testa" :text "transfera"} :ignore
                    {:title "titleb" :text "transferb"} :ignore
                    {:title "testb" :text "transferb"} :ignore))
        model ((classify/em-trainer
                (classify/maxent-trainer {:pred-thresh 0 :sigma-sq 10.0})
                unlabeled
                {:print-progress false
                 :posteriors->weights (classify/hard-em-weighter 1.0 0.6)})
               labeled)
        weights (safe-get-in (classify/model->nested-weight-maps +feature-set+ model) [true :lexical])]
    (is (every? #(> (weights %) 0.5) ["titlea" "transfera" "testa"]))
    (is (every? #(< (weights %) -0.5) ["titleb" "transferb" "testb"]))))

(use-fixtures :once validate-schemas)
