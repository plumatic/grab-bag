(ns domain.activity.core-test
  (:use clojure.test plumbing.core plumbing.test domain.activity.core)
  (:require
   [plumbing.serialize :as serialize]
   [domain.interests.manager :as interests-manager]))

(deftest interest-counts-test
  (testing "Action list must contain view"
    (is (make [:view]))
    (is (thrown? Exception (make [:click :bookmark]))))

  (let [id0 12345
        id1 45678
        actions [:view :click :remove :bookmark :share]
        iac (make actions)]
    (testing "Incrementing and checking activity counts"
      (is-= 0.0 (activity-count iac id0 :click))
      (is-= 1.0 (increment-count! iac id0 :click 1.0))
      (is-= 1.0 (activity-count iac id0 :click)))

    (testing "Geometric decay works"

      (doseq [[view click] [[1.0 0.5]
                            [1.5 0.25]
                            [1.75 0.125]]]
        (is-=
         view
         (increment-count-geometric! iac id0 :view 1.0 0.5))

        (is-= click (activity-count iac id0 :click))))

    (testing "Accessing undeclared actions throws an exception"
      (is (thrown? Exception (increment-count! iac id0 :foo 1.0)))
      (is (thrown? Exception (activity-count iac id0 :foo))))

    (testing "Time encoding and decoding"
      (let [now (millis)]
        (with-millis now
          (is-approx-= now
                       (decode-float-time (encode-float-time))
                       1e3))))

    (testing "Last viewed time"
      (let [now (millis)]
        (with-millis now (dotimes [_ 3] (increment-count! iac id0 :view 1.0)))
        (is-=-by #(quot % 1000)
                 now
                 (last-view-date iac id0))
        (is-= 4.75 (activity-count iac id0 :view)))
      (is-= (map-from-keys (constantly 0.0) actions)
            (activity-counts iac id1))
      (is-= (for-map [a actions]
              a (case a
                  :click 0.125
                  :view 4.75
                  0.0))
            (activity-counts iac id0)))

    (testing "Explain"
      (increment-count! iac id1 :remove 1.0)
      (let [iac-map (explain iac)]
        (doseq [interest-id [id0 id1]
                action actions]
          (is-= (activity-count iac interest-id action)
                (get-in iac-map [interest-id :counts action])))
        (doseq [interest-id [id0 id1]]
          (is-= (last-view-date iac interest-id)
                (get-in iac-map [interest-id :last-view])))))

    (testing "Pruning"
      (let [iac (make actions)]
        (doseq [i (range 4)]
          (with-millis (* 1000 (quot i 2))
            (increment-count! iac i :view (inc (* 5.0 (mod i 2))))))
        (is-=-by set #{0 1 2 3} (keys (explain iac)))
        (is-=-by set #{1 2 3} (keys (explain (pruned iac 500 5))))
        (is-=-by set #{0 1 2 3} (keys (explain iac)))))))

(deftest activity-score-test
  (testing "interest delta computation"
    (let [id 123
          actions (cons :view (keys (dissoc +grabbag-action-scores+ :view)))
          iac (make actions)]
      (doseq [a actions]
        (increment-count! iac id a 1.0))
      (is-approx-= 7.95 (activity-score iac id))
      (is-approx-= 0.0 (activity-score iac (inc id))))))

(deftest difference-test
  (let [actions [:view :b :c :d :e]
        mk #(zipmap actions (map double %))
        m1 {0 (mk [7 0 3 1 3])
            1 (mk [5 2 0 0 2])
            2 (mk [9 6 4 2 9])}
        m2 {1 (mk [2 1 2 0 2])
            3 (mk [9 2 3 4 5])}
        i1 (doto (make actions) (increment-counts! m1))
        i2 (doto (make actions) (increment-counts! m2))]
    (is-= (explain
           (difference i1 i2))
          (assoc-in (explain i1) [1 :counts] (mk [3 1 -2 0 0])))))

(deftest total-test
  (let [actions [:view :b :c :d :e]
        mk #(zipmap actions (map double %))
        m1 {(interests-manager/interest-id :feed 0) (mk [7 0 3 1 3])
            (interests-manager/interest-id :feed 1) (mk [5 2 0 0 2])
            (interests-manager/interest-id :feed 2) (mk [9 6 4 2 9])
            (interests-manager/interest-id :topic 3) (mk [1 2 3 4 5])}
        i1 (doto (make actions)
             (increment-counts! m1))]
    (is-= (total-counts i1 :feed)
          (apply merge-with + (butlast (vals m1))))))

(deftest select-interests-test
  (let [actions [:view :click :remove]
        iac (make actions)]
    (increment-count! iac 41 :view 1.0)
    (doseq [[action count] {:view 1.0 :click 1.0}]
      (increment-count! iac 42 action count))
    (doseq [[action count] {:view 10.0 :remove 1.0}]
      (increment-count! iac 43 action count))
    (is-=
     {41 {:view 1.0} 43 {:view 10.0 :remove 1.0}}
     (->> (select-interests iac [41 43])
          explain
          ;; Only look at counts
          (map-vals :counts)
          ;; Prune zero counts
          (map-vals #(for-map [[k v] % :when (pos? v)] k v))))))

(deftest read-write-test
  (let [iac (make +old-actions+)]
    (with-millis 1380836609000 ;; for testing the backwards compatibility
      (increment-counts! iac {1 {:post 1.0 :view 2.0 :click 3.0}
                              2 {:post 4.0 :view 5.0 :click 6.0}
                              3 {:post 7.0 :view 8.0 :click 9.0}}))
    (is-= (explain iac)
          (-> iac
              write-iac
              read-iac
              explain))

    (testing (str "backwards compatibility: check that a serialized TLongObjectHashMap"
                  "(the old IAC format) can be read and converted to the new form")
      (is-= (explain iac)
            (->> [4 -126 83 78 65 80 80 89 0 0 0 0 1 0 0 0 1 0 0 0 -22 -27 2 -24 15 0 0 1 96 -84 -19 0 5 115 114 0 28 103 110 117 46 116 114 111 118 101 46 84 76 111 110 103 79 98 106 101 99 116 72 97 115 104 77 97 112 114 13 109 -87 120 31 46 17 3 0 0 120 114 0 19 103 110 117 46 43 0 -88 72 97 115 104 20 -102 61 121 39 -42 -61 -116 2 0 1 76 0 16 95 104 97 115 104 105 110 103 83 116 114 97 116 101 103 121 116 0 32 76 103 110 117 47 116 1 96 0 47 5 96 1 53 29 34 56 59 120 112 113 0 126 0 3 119 12 0 0 0 3 0 9 1 112 3 117 114 0 2 91 70 11 -100 -127 -119 34 -32 12 66 2 0 0 120 112 0 0 0 11 82 77 -27 1 65 9 35 8 0 65 16 13 9 74 1 0 20 64 -32 0 0 119 8 13 25 4 2 117 1 97 0 4 17 64 4 64 -96 9 24 4 64 -64 9 8 78 1 0 4 64 -128 29 64 0 1 58 64 0 13 49 0 64 17 9 74 1 0 16 63 -128 0 0 120]
                 (map byte)
                 byte-array
                 serialize/deserialize
                 read-iac
                 explain)))))

(deftest migrate-activity-counts-test
  (let [actions [:view :foo :bar :baz :bam]
        counts-vals {1 {:foo 1.0 :bar 2.0 :baz 3.0}
                     2 {:foo 3.0 :bar 1.0 :baz 2.0}}
        old-iac (make actions)
        new-iac (make (conj actions :new-key))]

    (increment-counts! old-iac counts-vals)
    (increment-counts! new-iac counts-vals)

    (testing "Unmigrated iacs counts are different"
      (is (not= (counts old-iac) (counts new-iac))))

    (testing "Migrated iac counts are the same"
      (doseq [interest-id [1 2]]
        (is-= (migrate-activity-counts
               (raw-activity-counts old-iac interest-id)
               (indexer old-iac)
               (indexer new-iac))
              (raw-activity-counts new-iac interest-id))))))

(deftest migrate-iac-test
  (let [actions [:view :foo :bar :baz :bam]
        counts-vals {1 {:foo 1.0 :bar 2.0 :baz 3.0 :view 0.0 :bam 0.0}
                     2 {:foo 3.0 :bar 1.0 :baz 2.0 :view 0.0 :bam 0.0}}
        old-iac (make actions)
        new-iac (make (conj actions :new-key))]
    (increment-counts! old-iac counts-vals)
    (migrate-iac old-iac new-iac)
    (testing "Migrate IAC"
      (migrate-iac old-iac new-iac)
      (doseq [interest-id [1 2]]
        (is
         (=
          (assoc (counts-vals interest-id) :new-key 0.0)
          (activity-counts new-iac interest-id)))))))

(use-fixtures :once validate-schemas)
