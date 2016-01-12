(ns plumbing.classpath-test (:use clojure.test plumbing.classpath))


(deftest read-from-classpath-test
  (is (.startsWith (read-from-classpath "plumbing/classpath.clj") "(ns plumbing.classpath")))

(deftest ns-path-test
  (is (.endsWith
       (.getCanonicalPath (ns-path "plumbing.classpath"))
       "plumbing/src")))
