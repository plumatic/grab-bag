
if [[ -z `type -t brew` ]]; then
  echo ""
  echo ">>>>>>>>>> Installing brew  (if you don't have it) <<<<<<<<<<<<<<"
  echo ">>>>>>>>>> This is interactive so pay attention!"
  echo ""
  ruby -e "$(curl -fsSkL raw.github.com/mxcl/homebrew/go)"
  brew doctor
else
  echo ">>>>>>>>>> You already have brew (good job)"
fi  

if [[ -z `type -t wget` ]]; then
  echo ">>>>>>>>>> No wget, installing"
  brew install wget  
else
  echo ">>>>>>>>>> You already have wget (good job)"
fi  

if [[ ! -e /usr/local/lib/libzmq.3.dylib ]]; then
  echo ">>>>>>>>>> No libzmq 3, installing"
  # Install zeromq32 and its dependencies using homebrew.
  brew tap homebrew/versions
  brew install zeromq32
else
  echo ">>>>>>>>>> You already have libzmq (good job)"
fi  

mkdir ~/bin

if [[  `lein -v 2> /dev/null` =~ 2.0 ]]; then
  echo ">>>>>>>>> Already have lein 2"
else
  echo ""
  echo ">>>>>>>>>> Installing Lein                  <<<<<<<<<<<<<<"
  echo ">>>>>>>>>> Build an manage clojure projects <<<<<<<<<<<<<<"
  echo ""

  if [ -e /usr/bin/lein ]; then
      echo "*** sudo: Moving your current lein to lein-old"
      sudo mv /usr/bin/lein /usr/bin/lein-old
  fi

  curl "https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein" > ~/bin/lein
  chmod 755 ~/bin/lein
  echo "*** sudo: symlinking lein to /usr/bin/lein" 
  sudo ln -s ~/bin/lein /usr/bin/lein  
fi


echo ""
echo ">>>>>>>>>> Lein Repo Plugin (reinstall) <<<<<<<<<<<<<<"
echo ""

if [ -e ~/.lein/profiles.clj ]; then
  echo "*** Moving current lein profile to profiles-old.clj" 
  mv ~/.lein/profiles.clj ~/.lein/profiles-old.clj
fi

cd ~/grabbag/external/lein-repo
lein install
cd ~/grabbag/external/lein-coax
lein install


echo ""
echo ">>>>>>>>>> Lein Profile (~/.lein/profiles.clj) <<<<<<<<<<<<<<"
echo ""

echo "{:user {:plugins [[lein-repo \"0.2.1\"] [com.keminglabs/cljx \"0.6.0\" :exclusions [org.clojure/clojure]]]} :uberjar {:plugins [[lein-repo \"0.2.1\"]]}}" > ~/.lein/profiles.clj

echo ""
echo ">>>>>>>>>> Copying ssh and ec2 keys, installing crane  <<<<<<<<<<<<<<"
echo ""

rm ~/.crane.pem
cp ~/grabbag/config/crane/.crane.pem ~/.crane.pem
chmod 600 ~/.crane.pem
rm ~/.crane
cp ~/grabbag/config/crane/grabbag ~/.crane
chmod 600 ~/.crane
rm ~/bin/crane
echo "*** sudo: Symlinking crane to /usr/bin"
sudo ln -s ~/grabbag/lib/crane/resources/bin/crane-local /usr/bin/crane

echo ""
echo ">>>>>>>>>> Pull down all jars to maven repo ~/.m2 (approx 10mins)         <<<<<<<<<<<<<<"
echo ">>>>>>>>>> Run all unit tests using ptest (~/grabbag/config/bin/ptest) <<<<<<<<<<<<<<"
echo ""

export PATH=~/grabbag/config/bin:$PATH
ptest

echo ""
echo "You're all done!!"
echo "#### Make sure to add ~/bin and ~/grabbag/config/bin to PATH "
echo "#### Paste this into you ~/.bash_profile"
echo "copy-me>> export PATH=~/bin:~/grabbag/config/bin:\$PATH"
echo ""
