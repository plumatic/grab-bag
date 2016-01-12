(ns domain.activity.suggestions-test
  (:use clojure.test plumbing.test plumbing.core)
  (:require
   [domain.activity.core :as activity]
   [domain.activity.suggestions :as suggestions]))

(deftest email-suggestions-migration-test
  (testing "Migrating an old suggestion IAC into a new IAC with an :receive-email"
    (with-millis 2500
      (let [interest-ids [1337 42]
            sample (for-map [i interest-ids] i {:view 4.0})
            email-less-iac
            (with-redefs [suggestions/+activity-fields+ (remove #{:receive-email} suggestions/+activity-fields+)]
              (suggestions/make sample))
            email-full-iac (suggestions/make sample)]

        (doseq [interest-id interest-ids]
          (is (not=
               (activity/activity-counts email-full-iac interest-id)
               (activity/activity-counts email-less-iac interest-id)))
          (is (not=
               (seq (activity/raw-activity-counts email-full-iac interest-id))
               (seq (activity/raw-activity-counts email-less-iac interest-id)))))

        (let [after-migration-iac (suggestions/suggestion-emailed-migrate-iac email-less-iac)]

          (doseq [interest-id interest-ids]
            (is-= (activity/activity-counts email-full-iac interest-id)
                  (activity/activity-counts after-migration-iac interest-id))
            (is-= (seq (activity/raw-activity-counts email-full-iac interest-id))
                  (seq (activity/raw-activity-counts after-migration-iac interest-id)))))))))
