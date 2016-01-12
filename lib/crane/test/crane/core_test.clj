(ns crane.core-test (:use clojure.test crane.core crane.ssh))


(deftest ssh-cmd-test
  (is (= "ssh -i /path/to/key  user@host"
         (ssh-cmd {:user "user"
                   :host "host"
                   :key-path "/path/to/key"}
                  "host"))))

(deftest local-cmd-test
  (is (zero? (first (run-local-cmd ["ls"])))))

(deftest join-cmd-test
  (is (= "ls -la" (join-cmd "ls -la")))
  (is (= "ls -la" (join-cmd ["ls" "-la"]))))
