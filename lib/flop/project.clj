(defproject flop "0.0.1-SNAPSHOT"
  :description "FIXME: write"
  :internal-dependencies [plumbing]
  :external-dependencies [trove/trove prismatic/hiphip]
  :java-source-paths ["src/jvm"]
  :jvm-opts ^:replace ["-server" "-Dfile.encoding=UTF8" "-Xmx6g"]
  :repositories  {"trove-maven" "https://maven-us.nuxeo.org/nexus/content/groups/public"})
