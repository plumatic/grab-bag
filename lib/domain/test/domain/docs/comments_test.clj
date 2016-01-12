(ns domain.docs.comments-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [domain.doc-test-utils :as doc-test-utils]
   [domain.docs.comments :as comments]))

(def +handle-regex+ "[A-Za-z0-9_-]+")

(deftest extract-user-mentions-test
  (let [ee (fn [handle->user-id text]
             (map (juxt :user-id :start :end)
                  (comments/extract-user-mentions +handle-regex+ handle->user-id text)))]
    (is-= [] (ee {} ""))
    (is-= [] (ee {} "this is a test"))
    (is-= [] (ee {"test" 1} "this is a #test"))
    (is-= [[2 0 4] [2 10 14]] (ee {"foo" 2} "@foo is a @foo"))
    (is-= [[2 0 4]] (ee {"foo" 2} "@foo is a @bar"))
    (is-= [[2 0 4] [3 10 14]] (ee {"foo" 2 "bar" 3} "@foo is a @bar"))
    (is-= [[2 0 4] [3 13 20]] (ee {"foo" 2 "b_10-q" 3} "@foo* is a ??@b_10-q>"))))

(deftest extract-urls-test
  (let [ee (fn [text]
             (map (juxt :url :start :end)
                  (comments/extract-urls text)))]
    (is-= [] (ee ""))
    (is-= [] (ee "this is a #test"))
    (is-= [["http://foo.com?a=b" 0 18]
           ["http://google.com" 36 46]]
          (ee "http://foo.com?a=b is a cool url to google.com."))))

(deftest extract-entities-test
  (is-= {:user-mentions [{:user-id 2, :start 15, :end 19}],
         :urls [{:url "http://bob.com/@bob", :start 20, :end 39}]}
        (comments/extract-text-entities
         +handle-regex+
         {"bob" 2}
         "this is a test @bob http://bob.com/@bob")))

(deftest comment-round-trip-test
  (let [c (comments/->Comment 1 10 100 1000 "@this is a @test"
                              (doc-test-utils/test-text-entities [[10 0 5] [1 11 16]])
                              (doc-test-utils/test-simple-actions [1 2]) :web)]
    (is-= c (-> c comments/write-comment comments/read-comment))
    (testing "future proof and backward compatible"
      (is-= (assoc c :text-entities (comments/->TextEntities [] []))
            (-> c
                comments/write-comment
                (dissoc :text-entities)
                (assoc :a-new-field "some shit")
                comments/read-comment)))))

(def +test-comment-tree-data+
  {{:id 1 :user-id 1} [{:id 2 :user-id 2}
                       {:id 3 :user-id 3}]
   {:id 4 :user-id 4 :activity [5 6]} true
   {:id 42 :user-id 4} true
   {:id 6 :user-id 6} {{:id 7 :user-id 7} true
                       {:id 8 :user-id 8} [{:id 62 :user-id 6}]}})

(deftest empty-comments-test
  (is (empty? (seq comments/+empty-comments+)))
  (is (empty? (concat nil comments/+empty-comments+))))

(deftest comment-indexing-test
  (let [comments (doc-test-utils/test-comments +test-comment-tree-data+)]
    (is-= #{1 2 3 4 6 7 8 42 62}
          (set (map :id (seq comments))))
    (is (= {nil #{1 4 42 6}
            1 #{2 3}
            6 #{7 8}
            8 #{62}}
           (map-vals set (.parent->children comments))))
    (is-=-by #(sort-by :id (seq %))
             comments
             (-> comments comments/write-comments comments/read-comments))
    (is (= 9 (count comments)))
    (is (= 1 (:user-id (comments/get comments 1))))
    (is (= 8 (:user-id (comments/get comments 8))))
    (is-=-by #(sort-by first (map (juxt :id :user-id) %))
             (keys +test-comment-tree-data+)
             (comments/top-level comments))
    (is (empty? (comments/children comments 4)))
    (is (= #{7 8} (set (map :user-id (comments/children comments 6)))))
    (is (thrown? Throwable (comments/children comments 10)))
    (is (nil? (comments/parent comments 4)))
    (is (= 8 (:id (comments/parent comments 62))))
    (is (= 6 (:id (comments/parent comments 8))))
    (is (thrown? Throwable (comments/parent comments 10)))))

(deftest update-activity-test
  (let [comments (doc-test-utils/test-comments +test-comment-tree-data+)
        more-activity (reduce (fn [comments [comment-id user-id]]
                                (comments/update-activity
                                 comments comment-id
                                 (doc-test-utils/test-simple-comment-action user-id)))
                              comments [[8 20] [8 12] [4 7] [4 5] [8 12]])]
    (doseq [[comment-id [pre-comments post-comments]]
            {1 [nil nil]
             4 [[5 6] [5 6 7]]
             8 [nil [12 20]]}]
      (testing (str "activity for " comment-id)
        (is-=-by sort
                 pre-comments
                 (map :user-id (:activity (comments/get comments comment-id))))
        (is-=-by sort
                 post-comments
                 (map :user-id (:activity (comments/get more-activity comment-id))))))))

(deftest in-spans-test
  (let [spans [[1 3] [4 6]]]
    (is (not (comments/in-spans 0 spans)))
    (is (comments/in-spans 1 spans))
    (is (not (comments/in-spans 3 spans)))
    (is (comments/in-spans 5 spans))
    (is (not (comments/in-spans 7 spans)))
    (is (not (comments/in-spans 100 [])))))

(deftest filter-starting-spans-test
  (let [;; [012] [45]6
        s "012 456"
        span1 [0 3]
        span2 [4 6]
        spans [span1 span2]]
    (is-= "6" (comments/filter-starting-spans s spans))
    (is-= "" (comments/filter-starting-spans "     " spans))
    (is-= "" (comments/filter-starting-spans "012 45    " spans))))

(use-fixtures :once validate-schemas)
