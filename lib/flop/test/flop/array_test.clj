(ns flop.array-test
  (:use clojure.test)
  (:require
   [hiphip.double :as dbl]
   [plumbing.repl :as repl-utils]
   [flop.array :as fa]
   [flop.math :as fm])
  (:import
   java.util.Random))

;; some of this stuff tests hiphip since we moved code from here to there,
;; should clean up / move tests eventually.

(deftest aget-test
  (let [d (double-array [1 2 3])]
    (is (= 1.0 (dbl/aget d 0)))
    (is (= 2.0 (dbl/aget d 1)))))

(deftest aset-test
  (let [d (double-array [1 2 3])]
    (dbl/aset d 0 42)
    (is (= 42.0 (dbl/aget d 0)))))

(deftest aclone-test
  (is (= [1.0 2.0 3.0] (seq (dbl/aclone (double-array [1 2 3]))))))

(deftest doarr-test
  (let [a (atom 0.0)
        d (double-array [1 2 3])]
    (dbl/doarr [[i v] d] (swap! a + i v))
    (is (= 9.0 @a))))

(deftest amake-test
  (is (= (map double (range 10))
         (seq (dbl/amake [i 10] i)))))

(deftest afill!-test
  (let [d (double-array [1 2 3])]
    (dbl/afill! [[i v] d] (* i v))
    (is (= [0.0 2.0 6.0] (seq d)))))


(deftest areduce-test
  (is (= 6.0
         (dbl/areduce [v (double-array [1 2 3])] r 0.0 (+ r v)))))


(deftest sum-test
  (is (= 6.0 (dbl/asum (double-array [1.0 2.0 3.0])))))


(deftest scale-in-place!-test
  (let [d (double-array [1 2 3])]
    (fa/scale-in-place! d 2.0)
    (is (= [2.0 4.0 6.0] (seq d)))))

(deftest add-in-place!-test
  (let [d (double-array 3)
        s (double-array [1 1 1])]
    (fa/add-in-place! d s 2.0 1.0)
    (is (= [3.0 3.0 3.0] (seq d)))))


(deftest interpolate-test
  (let [d1 (double-array [3 3 3])
        d2 (double-array [1 1 1])]
    (is (= [2.0 2.0 2.0] (seq (fa/interpolate d1 0.5 d2  0.5))))))


(defmacro is-double-= [expr val]
  `(do (is (= ~(repl-utils/expression-info expr) {:class Double/TYPE :primitive? true}))
       (is (= ~expr ~val))))

(deftest set-and-get []
  (let [a (double-array 10)]
    (dbl/aset a 0 1.0)
    (is-double-= (dbl/aget a 0) 1.0)
    (is-double-= (dbl/aget a 1) 0.0)))

(deftest doarr []
  (let [as (double-array [1 2 3])
        bs (double-array [4 5 6])
        x  (atom 0)]
    (dbl/doarr [a as [j b] bs]
               (swap! x + (* a b j)))
    (is (= @x 46.0))))

(deftest amake []
  (is (= (seq (dbl/amake [i 3] i)) [0.0 1.0 2.0])))

(deftest afill! []
  (let [a (double-array 3)]
    (dbl/afill! [[i _] a] (inc i))
    (is (= (seq a) [1.0 2.0 3.0]))
    (dbl/afill! [[i v] a] (- v (* i i)))
    (is (= (seq a) [1.0 1.0 -1.0]))
    (dbl/afill! [[i _] a
                 b (double-array [1 2 3])
                 c (double-array [5 6 7])]
                (+ i (* b c)))
    (is (= (seq a) [5.0 13.0 23.0]))))


(deftest amap-test []
  (is (= [0.0 2.0 6.0]
         (seq (dbl/amap [[i v] (double-array [1.0 2.0 3.0])]
                        (* i v))))))

(deftest areduce-test2 []
  (is-double-= (dbl/areduce [a (double-array [1 2])
                             b (double-array [3 4])]
                            r 1.0 (* r (+ a b)))
               24.0)
  (is-double-= (dbl/areduce [[i a] (double-array [1 2])
                             b (double-array [3 4])]
                            r (double 1.0) (* r (- (+ a b) i)))
               20.0))

(deftest asum-test []
  (is-double-= (dbl/asum [a (double-array [1 2])
                          b (double-array [3 4])]
                         (double (* a b)))
               11.0)
  (is-double-= (dbl/asum [a (double-array [1 2])
                          [i b] (double-array [3 4])]
                         (* i (* a b)))
               8.0))

(deftest sum-test []
  (is-double-= (dbl/asum (double-array[1 2 3])) 6.0))

(deftest sample-discrete []
  (= 2 (fa/sample-discrete (Random. 0) (double-array [0 0 1 0])))
  (= 73 (fa/sample-discrete (Random. 0) (double-array 100 0.01))))

(deftest log-add []
  (is-double-= (fa/log-add (double-array 2 (Math/log 0.5))) 0.0))

(deftest scale! []
  (let [a (double-array [1 2])]
    (is (= (seq (fa/scale a 2)) [2.0 4.0]))
    (is (= (seq a) [1.0 2.0]))
    (is (= (seq (fa/scale-in-place! a 2)) [2.0 4.0]))
    (is (= (seq a) [2.0 4.0]))))

(deftest add-in-place! []
  (let [a (double-array [1 2])]
    (is (= (seq (fa/add-in-place! a (double-array [1 2]) 2 3)) [6.0 9.0]))
    (is (= (seq a) [6.0 9.0]))))

(deftest multiply-in-place-test
  (let [a (double-array [1 2])
        s (double-array [3 4])]
    (is (= [3.0 8.0] (seq (fa/multiply-in-place! a s))))))


(deftest normalize! []
  (let [a (double-array [1 3])]
    (is (= (seq (fa/normalize a)) [0.25 0.75]))
    (is (= (seq a) [1.0 3.0]))
    (is (= (seq (fa/normalize! a)) [0.25 0.75]))
    (is (= (seq a) [0.25 0.75]))))

(deftest rand-dirichlet []
  (is (= (seq (fa/rand-dirichlet (Random. 3) 3 0.1))
         [0.6247522276859545 1.278027021924035E-6 0.3752464942870236])))

(deftest vectors []
  (is (= 11.0 (dbl/dot-product (double-array [1 3 4]) (double-array [3 0 2]))))
  (is (= 3.0 (fa/sup-norm (double-array [1 3 4]) (double-array [3 0 2]))))
  (is (= 7.0 (fa/l1 (double-array [1 3 4]) (double-array [3 0 2])))))


(deftest arg-max-test []
  (is (= 1 (dbl/amax-index (double-array [1 3 2])))))

(deftest log-normalize-in-place!-test
  (let [logV (double-array [1.3 -2.6 4.1])
        _ (fa/log-normalize-in-place! logV)]
    (is (fm/within 1e-4 (first logV) (Math/log 0.0572579)))
    (is (fm/within 1e-4 (second logV) (Math/log 0.0011590)))
    (is (fm/within 1e-4 (last logV) (Math/log 0.9415860)))))

(deftest approx-equal?-fn-test
  (let [tol 1e-2
        approx-equal? (fa/approx-equal?-fn tol)
        x (double-array [1.0 1.0 1.0])
        y (double-array [1.0001 1.0001 1.0001])
        z (double-array [1.0 1.0 1.1])]
    (is (approx-equal? x y))
    (is (not (approx-equal? x z)))))

(deftest discretize-test
  (let [a (double-array [0 1 3 9 27])]
    (is (= 0 (fa/discretize a 0)))
    (is (= 1 (fa/discretize a 1)))
    (is (= 2 (fa/discretize a 2)))
    (is (= 4 (fa/discretize a 10)))
    (is (= 4 (fa/discretize a 27)))
    (is (= 5 (fa/discretize a 1000))))
  (is (= 0 (fa/discretize (double-array []) 10.0))))
