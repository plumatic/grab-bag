{:machine {:instance-type :r3.8xlarge
           :tags {:owner "grabbag-corp"}}
 :service {:server-port 80}
 :parameters {:swank-port 6446
              :yourkit-port 10001
              :forward-ports {6666 :swank-port
                              10001 :yourkit-port}
              :email-level :fatal}
 :envs {:crunked {:env :stage
                  :machine {:instance-type :cc2.8xlarge
                            :groups ["woven" "grabbag-model-explorer-crunked"]}
                  :service {:service-name-override "model-explorer-crunked"}
                  :parameters {:num-days 0.0}}
        :test {:service {:server-port 5888}}}}
