(defproject html-parse "0.0.1-SNAPSHOT"
  :description "friendly parsing with tagsoup."
  :internal-dependencies [plumbing]
  :external-dependencies [xerces/xercesImpl xalan commons-lang clj-tagsoup]
  :jvm-opts ["-server"  "-mx1800m" "-Dfile.encoding=UTF8"])
