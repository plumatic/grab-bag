(ns domain.docs.fitness-stats-test
  (:use clojure.test domain.docs.fitness-stats plumbing.core domain.doc-test-utils plumbing.test)
  (:require
   [schema.core :as s]))

(deftest fitness-stats-test
  (let [ofs (fitness-stats)
        fs (fitness-stats)]
    (set-facebook-counts! ofs {:like_count 3 :comment_count 14 :share_count 5})
    (Thread/sleep 2)
    (set-facebook-counts! fs {:like_count 2 :comment_count 10 :share_count 3})
    (Thread/sleep 2)
    (increment-view-count! fs 5)
    (increment-view-count! ofs 1)
    (merge-in! fs ofs)
    (is (= (facebook-total-count ofs) 22))
    (merge-in! ofs fs)
    (increment-view-count! fs 2)
    (is (= (facebook-total-count fs) 15))
    (is (= (facebook-total-count ofs) 15))
    (is (= (view-count fs) 8))
    (is (= (view-count ofs) 7))
    (let [ofs2 (clone ofs)]
      (merge-in! fs ofs)
      (is (= (facebook-total-count ofs) 15))
      (is (= (facebook-total-count fs) 15))
      (is (= (view-count fs) 15))
      (set-facebook-counts! ofs {:like_count 3 :comment_count 14 :share_count 5})
      (merge-in! fs ofs)
      (is (= (facebook-total-count fs) 22))
      (is (= (view-count fs) 22)))))
