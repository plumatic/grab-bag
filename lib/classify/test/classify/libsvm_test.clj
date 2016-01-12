(ns classify.libsvm-test
  (:use clojure.test plumbing.test plumbing.core)
  (:require
   [plumbing.index :as plumbing-index]
   [flop.map :as map]
   [classify.libsvm :as libsvm]
   [classify.features :as features]
   [classify.features-test :as features-test]))

(deftest libsvm-data-line-test
  (is-= "1 0:1 3:2.002"
        (libsvm/libsvm-data-line [(map/map->fv {3 2.002 0 1.0 1 0.0}) 1]))
  (is-= "z"
        (libsvm/libsvm-data-line [(map/map->fv {1 0.0}) "z"])))

(deftest libsvm-feature-map-test
  (is-= "0\tabc\tq\n1\tdef\tq"
        (libsvm/libsvm-feature-map (plumbing-index/static ["abc" "def"]))))

(deftest xgboost-conf-file-test
  (is-= "test = 1\nfoo = \"bar\"\nbaz = hi"
        (libsvm/xgboost-conf-file
         (array-map
          "test" 1
          "foo" "bar"
          "baz" 'hi))))

(deftest libsvm-data-files-test
  (testing "non-indexed"
    (is-= {:features.txt "0\ta\tq" :train-data.txt "1 0:1" :test-data.txt "0 0:2"},
          (libsvm/libsvm-data-files [[{"a" 1} 1]] [[{"b" 1 "a" 2.0} 0]] {})))
  (testing "indexed"
    (let [fs (features/feature-set [features-test/starts-with-letter-t])
          fv (map/map->fv {})]
      (features/fv-put-features! fs {} fv features-test/test-lexical-doc features-test/starts-with-letter-t)
      (is-= {:features.txt "0\tlexical_title\tq\n1\tlexical_texty\tq\n2\tlexical_text\tq"
             :train-data.txt "1 0:1 1:1 2:1"
             :test-data.txt ""}
            (libsvm/libsvm-data-files [[fv 1]] [] {} fs)))))

(defn run-xgboost-smoke-test []
  (println
   (libsvm/run-xgboost
    "/Users/w01fe/sw/xgboost/xgboost"
    libsvm/+default-binary-opts+
    (aconcat
     (repeat 10
             [[{"alpha" 1} 1]
              [{"alpha" 1 "beta" 1} 0]
              [{"alpha" 1 "gamma" 1} 0]
              [{"alpha" 1 "beta" 1 "gamma" 1} 1]]))
    [[{"beta" 1 "gamma" 1} 1]]
    {}
    nil)))
