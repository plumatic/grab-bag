(ns classify.features-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.index :as index]
   [flop.map :as map]
   [classify.features :as features]))

(def identity-feature
  (features/feature-fn :identity features/identity-index nil))

(def default-feature
  (features/feature-fn :default features/default-index nil))

(def custom-feature
  (features/feature-fn :custom #(index/static [:a :b :c]) nil))

(defn test-feature-set []
  (features/feature-set [identity-feature default-feature custom-feature]))

(deftest basic-indexing-test
  (let [fs (test-feature-set)]
    (is (= 2560 (features/index-of! fs :identity 10)))
    (is (= 256 (features/index-of! fs :identity 1)))
    (is (= [:identity 10] (features/value-of fs 2560)))

    (is (= 1 (features/index-of! fs :default "a")))
    (is (= 257 (features/index-of! fs :default "b")))
    (is (= [:default "b"] (features/value-of fs 257)))

    (is (= 258 (features/index-of! fs :custom :b)))
    (is (= [:custom :b] (features/value-of fs 258)))))

(def +test-nested-features+
  {:identity (map/map->trove {1 1.0 2 2.0 3 3.0})
   :default {"a" 1.0 [:b] 2.0}
   :custom {:c 9.0}})

(deftest nested-features->trove-test
  (let [fs (test-feature-set)
        t (features/nested-features->trove fs +test-nested-features+)]
    (is (= 6 (.size t)))
    (is-= (features/trove->nested-features fs t)
          (update-in +test-nested-features+ [:identity] map/trove->map))))

(deftest fv-remove-types!-test
  (let [fs (test-feature-set)
        t (features/nested-features->trove fs +test-nested-features+)
        fv (map/trove->fv t)]
    (is (= 6 (.size fv)))
    (features/fv-remove-types! fs fv #{:identity})
    (is (= 3 (.size fv)))
    (is-= (features/trove->nested-features fs (map/fv->trove fv))
          (dissoc +test-nested-features+ :identity))))

(deftest fv-type-filter-fn-test
  (let [fs (test-feature-set)
        t (features/nested-features->trove fs +test-nested-features+)
        fv (map/trove->fv t)
        filtered ((features/fv-type-filter-fn fs #{:default :custom}) fv)]
    (is (= 6 (.size fv)))
    (is (= 3 (.size filtered)))
    (is-= (features/trove->nested-features fs (map/fv->trove filtered))
          (dissoc +test-nested-features+ :identity))))

(def starts-with-letter-t
  (features/feature-fn
   :lexical
   features/default-index
   (fn [global-context doc]
     ;; for each string field, for each
     (->> (map #(% doc) [:title :text])
          (mapcat #(clojure.string/split % #"\s"))
          (filter #(.startsWith ^String % "t"))
          frequencies))))

(def test-lexical-doc
  {:title "a title goes here"
   :text "some texty text"})

(defn legit-lexical! [nested-fv]
  (is (= 1.0 (safe-get-in nested-fv [:lexical "title"])))
  (is (= 1.0 (safe-get-in nested-fv [:lexical "texty"])))
  (is (= 1.0 (safe-get-in nested-fv [:lexical "text"]))))

(defmacro legit-fv! [fs fv size validators]
  `(let [fv# ~fv
         data# (features/fv->nested-features ~fs fv#)]
     (is (= ~size (count fv#)))
     (doseq [v# ~validators]
       (v# data#))))

(deftest simple-lexical-feat-test
  (let [fs (features/feature-set [starts-with-letter-t])
        fv (map/map->fv {})]
    (features/fv-put-features! fs {} fv test-lexical-doc starts-with-letter-t)
    (legit-fv! fs fv 3 [legit-lexical!])))

(deftest unnormalized-feature-vector-for-doc-test
  (let [fs (features/feature-set [starts-with-letter-t])]
    (legit-fv! fs (features/feature-vector fs {} test-lexical-doc) 3 [legit-lexical!])
    (is-= (features/fv->nested-features
           fs
           (features/feature-vector fs {} test-lexical-doc))
          {:lexical {"text" 1.0 "texty" 1.0 "title" 1.0}})))

(deftest nested-map-dot-product-test
  (is (= 0.0 (double
              (features/nested-map-dot-product
               {:a {:a1 1 :a2 4}}
               {:a {:a3 1 :a4 2}}))))
  (is (= 42.0 (double
               (features/nested-map-dot-product
                {:a {:a1 1 :a2 4}
                 :b {:b1 3 :b2 6}
                 :c {:c1 5}
                 :d {:d1 10}}
                {:a {:a3 1 :a2 -1}
                 :b {:b3 1 :b2 6}
                 :c {:c1 2}})))))
