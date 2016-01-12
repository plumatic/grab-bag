(ns plumbing.auth-test
  (:use clojure.test plumbing.auth plumbing.test))

(deftest rand-str-test
  (is (= 5
         (count (rand-str 5))))
  (is (= 20
         (count (rand-str 20))))
  (is (= 120
         (count (rand-str 120))))

  (is (not= (rand-str 14)
            (rand-str 14))))

(deftest secure-token-test
  (let [k (rand-str 32)
        d1 ["this is some dat!@!@E> " "asdfasdfsadf"]]
    (is (= d1 (token-data k (secure-token k d1))))
    (is (nil? (token-data k "afasdsfsd.fasdfasdlkjwel2kj3l")))
    (is (nil? (token-data k "afasd!!!!s(dfsd.fasdfas!!!!")))
    (is (nil? (token-data k (str "A" (secure-token k d1)))))
    (is (nil? (token-data "WRONG" (secure-token k d1))))))

(deftest expiring-token-test
  (let [k (rand-str 32)
        d1 ["this is some dat!@!@E> " "asdfasdfsadf"]]
    (let [data (with-millis 1000 (expiring-token k d1))]
      (with-millis 0 (is (= d1 (expiring-token-data k 500 data))))
      (with-millis 1000 (is (= d1 (expiring-token-data k 500 data))))
      (with-millis 1400 (is (= d1 (expiring-token-data k 500 data))))
      (with-millis 1600 (is (nil? (expiring-token-data k 500 data)))))
    (is (nil? (expiring-token-data k 10 "afas/dsfsd.fasdfasdlkjwel2kj3l")))
    (is (nil? (expiring-token-data k 10 (str "A" (secure-token k d1)))))
    (is (nil? (expiring-token-data "WRONG" 10 (secure-token k d1))))))

(deftest cipher-test
  (let [cipher (make-cipher {:salt "salt" :passphrase "passphrase"})
        text "hello melvin"]
    (is-= text (decrypt cipher (encrypt cipher text)))
    (is (nil? (decrypt cipher (encrypt cipher nil))))))
