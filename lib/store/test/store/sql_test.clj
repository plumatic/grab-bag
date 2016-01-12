(ns store.sql-test
  (:use clojure.test plumbing.core plumbing.test store.sql)
  (:require
   [clojure.java.jdbc.deprecated :as jdbc]
   [clojure.string :as str]
   [plumbing.core-incubator :as pci]
   [store.bucket :as bucket]))

(defn test-pool [tables]
  (connection-pool
   (assoc h2-test-connection-spec
     :table-spec tables)))

(deftest date-format-test
  (let [date "2015-07-28 14:15:16.17"
        date2 "2015-07-01 05:15:16.17"]
    (with-open [conn-spec (test-pool nil)]
      (jdbc/with-connection conn-spec
        (is-= "2015-07-28: 14"
              (jdbc/with-query-results res
                ["select DATE_FORMAT(?,'%Y-%m-%d: %H')" date]
                (->> res (mapcat vals) first)))
        (is-= "2015-07-01: 05"
              (jdbc/with-query-results res
                ["select DATE_FORMAT(?,'%Y-%m-%d: %H')" date2]
                (->> res (mapcat vals) first)))

        (testing "unsupported format char (%a) throws"
          (is (thrown? Exception
                       (jdbc/with-query-results res
                         ["select DATE_FORMAT(?,'%a)" date]
                         (->> res (mapcat vals) first)))))
        (testing "invalid date throws"
          (is (thrown? Exception
                       (jdbc/with-query-results res
                         ["select DATE_FORMAT(?,'%a)" "not a date"]
                         (->> res (mapcat vals) first)))))))))

(deftest h2-mysql-compatability-test
  (with-open [conn-spec (test-pool
                         {"user" [[:id :serial "PRIMARY KEY"]
                                  [:name "VARCHAR(255)" :unique]]
                          "email" [[:email "VARCHAR(63)" "PRIMARY KEY" "NOT NULL"]
                                   [:user_id "BIGINT UNSIGNED"]]})
              user-b (bucket/bucket {:type :sql
                                     :connection-spec conn-spec
                                     :primary-key [:id]
                                     :table "user"})
              email-b (bucket/bucket {:type :sql
                                      :connection-spec conn-spec
                                      :primary-key [:email]
                                      :table "email"})]

    (jdbc/with-connection conn-spec
      (jdbc/do-commands
       (alter-table-cmd "email" [(add-fk-stmt ["email" :user_id :user :id "ON DELETE CASCADE"])])))

    (testing "autoincrement"
      (is-= {:id 1 :name "x"}
            (insert! user-b {:name "x"}))
      (is-= {:id 2 :name "y"}
            (insert! user-b {:name "y"})))

    (testing "uniqueness constraint is enforced"
      (is (thrown? Throwable (insert! user-b {:name "x"}))))

    (testing "foreign key constraints"
      (let [email "test@example.com"]

        (testing "happy case: constraints are satistifed"
          (bucket/put email-b email {:user_id 1})
          (is-= {:email email :user-id 1}
                (pci/safe-singleton (select conn-spec "email" {:email email}))))

        (testing "missing foreign key throws on `put`"
          (is (thrown? Exception (bucket/put email-b "test2@example.com" {:user_id 1337}))))

        (testing "deletes cascade"
          (bucket/delete user-b 1)
          (is (empty? (select conn-spec "email" {:email email}))))))))

(deftest basic-test
  (with-open [conn-spec (test-pool
                         {"test" [[:id :bigint "PRIMARY KEY"]
                                  [:name "varchar(128)" "UNIQUE"]
                                  [:location :text]]})
              b (bucket/bucket {:type :sql
                                :connection-spec conn-spec
                                :table "test"})]
    (bucket/put b 42 {:name "aria42"})

    (is (= "aria42" (safe-get (bucket/get b 42) :name)))
    (is (bucket/exists? b 42))
    (bucket/put b 43 {:name "zaria42" :location "Bizarro land"})
    (is (= (set [42 43]) (set (bucket/keys b))))
    (is (= [[42 {:name "aria42", :location nil, :id 42}]
            [43 {:name "zaria42", :location "Bizarro land", :id 43}]]
           (bucket/seq b)))
    (is (= 2 (bucket/count b)))
    (bucket/update b 43 (fn [m] (assoc m :location (str "place:" (:location m)) )))
    (is (= "place:Bizarro land" (:location (bucket/get b 43))))
    (bucket/delete b 42)
    (is (= [43] (bucket/keys b)))
    (is (= #{"test"} (jdbc/with-connection (db-connection b)
                       (all-tables))))
    (bucket/close b)))

(deftest multi-column-bucket-test
  (with-open [conn-spec (test-pool
                         {"test" [[:model_id "varchar(255)"]
                                  [:batch_id "varchar(255)"]
                                  [:data :int]
                                  ["primary key(model_id, batch_id)"]]})
              b (bucket/bucket {:type :sql
                                :connection-spec conn-spec
                                :table "test"
                                :primary-key [:model_id :batch_id]})]
    (let [is-val (fn [k vs]
                   (is-= (merge vs {:model_id (first k) :batch_id (second k)})
                         (bucket/get b k)))]
      (testing "put"
        (doto b
          (bucket/put ["model1" "batch30"] {:data 32})
          (bucket/batch-put {["model2" "batch1"] {:data 5}
                             ["model2" "batch2"] {:data 18}})
          (bucket/batch-put [{:model_id "model3"
                              :batch_id "batch4"
                              :data -30}
                             {:model_id "model3"
                              :batch_id "batch16"
                              :data 90}]))
        (is-= 5 (bucket/count b))
        (is-val ["model1" "batch30"] {:data 32})
        (is-val ["model2" "batch1"] {:data 5})
        (is-val ["model2" "batch2"] {:data 18})
        (is-val ["model3" "batch4"] {:data -30})
        (is-val ["model3" "batch16"] {:data 90})
        (testing "error checking"
          (is (thrown? AssertionError (bucket/batch-put b [{:batch_id "five" :data 5}])))
          (is
           (thrown?
            AssertionError
            (bucket/batch-put b {["model-a" "batch-4"]
                                 {:model_id "model-b"
                                  :batch_id "batch-2"
                                  :data 1}})))
          (is
           (thrown?
            AssertionError
            (bucket/batch-put b {["model-a" "batch-4"]
                                 {:model_id "model-b"
                                  :data 3}})))))
      (testing "delete"
        (doto b
          (bucket/put ["badmodel" "badbatch"] {:data 40})
          (bucket/delete ["badmodel" "badbatch"]))
        (is-= nil (bucket/get b ["badmodel" "badbatch"])))
      (testing "update"
        (doto b
          (bucket/put ["model5" "batch8"] {:data 41})
          (bucket/update ["model5" "batch8"]
                         #(update % :data (partial * 2))))
        (is-val ["model5" "batch8"] {:data 82})))))

(deftest degenerate-multi-column-test
  "vector is used for a single column pkey"
  (with-open [conn-spec (test-pool
                         {"test" [[:id :bigint "PRIMARY KEY"]
                                  [:name "varchar(128)" "UNIQUE"]
                                  [:location :text]]})]
    (let [b (bucket/bucket {:type :sql
                            :connection-spec conn-spec
                            :table "test"
                            :primary-key [:id]})]
      (bucket/put b 42 {:name "aria42"})

      (is (= "aria42" (safe-get (bucket/get b 42) :name)))
      (is (bucket/exists? b 42))
      (bucket/put b 43 {:name "zaria42" :location "Bizarro land"})
      (is (= (set [[42] [43]]) (set (bucket/keys b))))
      (is (= [[[42] {:name "aria42", :location nil, :id 42}]
              [[43] {:name "zaria42", :location "Bizarro land", :id 43}]]
             (bucket/seq b)))
      (is (= 2 (bucket/count b)))
      (bucket/update b 43 (fn [m] (assoc m :location (str "place:" (:location m)) )))
      (is (= "place:Bizarro land" (:location (bucket/get b 43))))
      (bucket/delete b 42)
      (is (= [[43]] (bucket/keys b)))
      (is (= #{"test"} (jdbc/with-connection (db-connection b)
                         (all-tables))))
      (bucket/close b))))

(deftest select-and-insert-test
  (with-open [conn-spec (test-pool
                         {"test" [[:id :serial "PRIMARY KEY"]
                                  [:email "varchar(128)" "UNIQUE"]
                                  [:text :text]]})
              b (bucket/bucket {:type :sql
                                :connection-spec conn-spec
                                :table "test"})]

    (let [r1 {:email "1@x.com" :text "text1"}
          r2 {:email "1@x.com" :text "text2"}
          r3 {:email "2@x.com" :text "text1"}]

      (testing "simple insert and select"
        (letk [[id :as r1+id] (insert! b r1)]
          (is-= r1 (dissoc r1+id :id))

          (doseq [constraint [{:email "1@x.com"}
                              {:text "text1"}
                              {:id id}
                              {:email "1@x.com" :text "text1"}]]
            (is-= r1
                  (->> constraint
                       (select conn-spec "test")
                       pci/safe-singleton
                       (<- (dissoc :id)))))))

      (testing "violate unique-key semantics"
        (is (thrown? Exception (insert! b r2))))

      (testing "multiple select"
        (is-= r3 (dissoc (insert! b r3) :id))
        (is-=-by set
                 [r1 r3]
                 (->> {:text "text1"}
                      (select conn-spec "test")
                      (map #(dissoc % :id))))))))

(deftest increment-field!-test
  (with-open [conn-spec (test-pool
                         {"test" [[:id :bigint "PRIMARY KEY"]
                                  [:name :text]
                                  [:my_int :bigint]]})
              b (bucket/bucket {:type :sql
                                :connection-spec conn-spec
                                :table "test"})]

    (testing "existing record gets incremented"
      (let [id 1
            rec {:id id :name "name" :my_int 100}]
        (insert! b rec)
        (assert (= 100 (:my_int (bucket/get b id))))
        (increment-field! b :my_int (assoc rec :my_int 23 :name "new-name"))
        (letk [[my_int name] (bucket/get b id)]
          (is-= 123 my_int)
          (testing "other fields are unchanged"
            (is-= "name" name)))))

    (testing "missing record gets inserted"
      (let [id 2
            rec {:id id :name "name2" :my_int 100}]
        (assert (nil? (bucket/get b id)))
        (increment-field! b :my_int rec)
        (is-= rec (bucket/get b id))))))

(deftest now-test
  (with-open [conn-spec (test-pool nil)]
    (jdbc/with-connection conn-spec
      (is-approx-=
       (quot (millis) 1000)
       (jdbc/with-query-results res
         ["select UNIX_TIMESTAMP(NOW())"]
         (->> res (mapcat vals) first))
       10e3))))
