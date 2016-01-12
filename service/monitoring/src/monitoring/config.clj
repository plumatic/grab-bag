{:machine {:groups ["woven" "grabbag-catch-all-6"]
           :tags {:owner "grabbag-corp"}}
 :service {:jvm-opts "-Xmx2g -Xms2g"}
 :parameters {:forward-ports {6666 :swank-port}
              :hour-to-revive-instances 8
              :should-revive-instances? false}
 :envs {:prod {:parameters {:swank-port 6348
                            :should-revive-instances? true}}
        :stage {:parameters {:swank-port 6349}}}}
