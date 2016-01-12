#!/bin/bash
set -ev

which lein
curl "https://raw.githubusercontent.com/technomancy/leiningen/2.4.3/bin/lein" > `which lein`
chmod 755 `which lein`
lein version

pushd "external/lein-repo/"
lein install
echo "{:user {:plugins [[lein-repo \"0.2.1\"] [com.keminglabs/cljx \"0.6.0\"]]}}" > ~/.lein/profiles.clj
popd
