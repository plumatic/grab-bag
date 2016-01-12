(ns dashboard.pages.home-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.new-time :as new-time]
   [dashboard.pages.home :as home]))

(deftest active-users-data-test
  (let [empty-monthly (map-from-keys (constantly []) [:total :d1-1 :d2-4 :d5-14 :d15-30])
        day-ms (new-time/to-millis :day)
        active-users {true {1 [10 11]
                            2 [10 12]
                            3 [10 13]
                            4 [10 14 15 16]
                            5 [10 13]
                            6 []
                            7 [10 12]
                            8 [12 13]
                            9 [10 11]}
                      false {1 [10 20]
                             2 [20 21 22]
                             3 []
                             4 [23]
                             5 [24]
                             6 [10 25]
                             7 [20 25]
                             8 [20]
                             9 [20]}}
        daily (fn [& xs] (map vector (map #(* day-ms %) (range 1 10)) xs))
        weekly (fn [& xs] (map vector (map #(* day-ms %) (range 7 10)) xs))]
    (is-= {:by-user
           (for-map [[id active] (->> (for [m (vals active-users)
                                            [d ids] m
                                            id ids]
                                        [id d])
                                      (group-by first)
                                      (map-vals (fn->> (map second) set)))]
             id
             {:active-days (set (map #(* day-ms %) active))
              :primary-client (if (>= id 20) "web" "iphone")})

           :active-users
           {"iphone" {:daily {:total (daily 2 2 2 4 2 1 2 2 2)}
                      :weekly {:total (weekly 7 6 7)
                               :d1-1 (weekly 4 3 4)
                               :d2-5 (weekly 2 2 2)
                               :d6-7 (weekly 1 1 1)}
                      :monthly empty-monthly}
            "web" {:daily {:total (daily 1 3 0 1 1 1 2 1 1)}
                   :weekly {:total (weekly 6 6 4)
                            :d1-1 (weekly 4 4 2)
                            :d2-5 (weekly 2 2 2)
                            :d6-7 (weekly 0 0 0)}
                   :monthly empty-monthly}}}
          (home/active-users-data
           (for [[ios? m] active-users
                 [date ids] m
                 id ids]
             {:user_id id
              :client (if ios? "iphone" "web")
              :date (* day-ms date)})
           9))))

(deftest signups-data-test
  (is-= (for-map [[k [total onboarded]] {"web" [[1 1 0 1 2]
                                                [1 0 0 0 1]]
                                         "iphone" [[1 0 0 0 1]
                                                   [0 0 0 0 1]]}]
          k
          (for-map [[k data] {:total total :onboarded onboarded}]
            k
            (for [[i d] (map vector (range 1 6) data)]
              [(* (new-time/to-millis 1 :day) i) d])))
        (home/signups-data
         (map-from-keys
          (constantly {:active-days #{} :primary-client "web"})
          [1 2 3 4 5])
         (for [[ios? m] {false {1 0.1
                                2 1.5
                                8 2.3
                                10 4.3
                                4 5.5
                                6 5.7}
                         true {12 1.1
                               3 5.1}}
               [id day] m]
           {:user_id id
            :created_date (long (* day (new-time/to-millis :day)))
            :client (if ios? "iphone" "web")})
         5)))

(use-fixtures :once validate-schemas)
