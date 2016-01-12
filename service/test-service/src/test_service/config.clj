{:service {:type "test-service"
           :jvm-opts "-Xmx2g -Xms2g"}
 :machine {:tags {:owner "grabbag-corp"}}
 :parameters {:foo 1
              :swank-port 6666
              :forward-ports {6666 :swank-port}}

 :envs {:publisher {:env :stage
                    :machine {:groups ["woven" "grabbag-test"]}
                    :parameters {:service-type :publisher}}

        :subscriber {:env :stage
                     :machine {:replicated? true
                               :groups ["woven" "grabbag-test-subscriber"]}
                     :parameters {:service-type :subscriber
                                  :publisher-timeout [10 :years]}}}}
