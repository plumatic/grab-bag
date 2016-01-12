(ns domain.interests.type.special-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [store.bucket :as bucket]
   [domain.interests.manager :as manager]
   [domain.interests.type.special :as special]))

(deftest special-interest-manager-test
  (let [f (juxt :type :key :id)]
    (doseq [[type ks] [[:personal [special/+singleton-key+]]
                       [:social [special/+singleton-key+]]
                       [:grabbag-social [special/+singleton-key+]]
                       [:suggestions ["all" "twitter" "facebook" "google"]]]
            key ks
            :let [interest {:type type :key key}
                  manager (safe-get special/+special-interest-managers+ type)]]
      (testing (str interest)
        (is-=-by f interest
                 (->> interest
                      (manager/type-index! manager)
                      (manager/type-lookup manager)))
        (let [interest (update-in interest [:key] str "JFDKLFEW")]
          (is (thrown? Throwable
                       (manager/type-index! manager interest)))
          (is (thrown? Throwable
                       (manager/type-key-index manager (:key interest))))
          (is (thrown? Throwable
                       (manager/type-lookup manager 100))))))))
