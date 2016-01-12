(ns leiningen.commit
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell])
  (:import
   [org.apache.commons.io IOUtils]))

(defn run-cmd [cmd-arr]
  (println "Running commit command: " (str/join " " cmd-arr))
  (let [pb (doto (ProcessBuilder. cmd-arr)
             (.redirectErrorStream))]
    (let [proc (.start pb)]
      (IOUtils/copy (.getInputStream proc) System/out)
      (.waitFor proc)
      (when-not (zero? (.exitValue proc))
        (printf "Error in command `%s`" (pr-str cmd-arr))))))

(defn commit [project & args]
  (doseq [cmd-arr  (:pre-commit-cmds project [["lein" "test"]])]
    (run-cmd cmd-arr))
  (run-cmd (concat ["git" "commit"] args)))
