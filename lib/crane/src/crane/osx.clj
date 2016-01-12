(ns crane.osx
  "Helpers for opening things on Mac OS X"
  (:use plumbing.core)
  (:require
   [plumbing.io :as io]
   [crane.core :as crane]))

(defn open-in-terminal! [command]
  (let [f (io/create-temp-file "sh" true)]
    (.setExecutable f true)
    (io/copy-to-file (str "#!/bin/bash\n" command) f)
    (crane/safe-run-local-cmd ["open" "-a" "Terminal.app" (.getAbsolutePath f)])))

(defn open-url! [url]
  (crane/safe-run-local-cmd ["open" url]))
