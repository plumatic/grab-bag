echo ""
echo ">>>>>>>>>> Making your .emacs file <<<<<<<<<<<<<<"
echo ""

if [ -e ~/.emacs ]; then
    echo "*** Moving current .emacs to .emacs-old"
    mv ~/.emacs ~/.emacs-old
fi

if [ ! -d ~/.emacs.d ]; then
    echo "*** Moving current .emacs.d to .emacs.d-old"
    mv ~/.emacs.d ~/.emacs.d-old
fi

echo "(load \"~/grabbag/config/emacs/grabbag\") (load-theme 'tango-dark)" > ~/.emacs

echo ""
echo ">>>>>>>>>> Downloading Emacs <<<<<<<<<<<<<<"
echo ""


cd ~/Downloads
wget http://emacsformacosx.com/emacs-builds/Emacs-24.3-universal-10.6.8.dmg 
open Emacs-24.3-universal-10.6.8.dmg

echo ">>>>>>>>>> use wemacs on command-line to open carbon emacs <<<<<<<<<<<<<<"

sudo echo 'open -a /Applications/Emacs.app $1' > /usr/bin/wemacs

echo ""
echo ">>>>>>>>>> Copy to /Applications and open Up Emacs <<<<<<<<<<<<<<"
echo ">>>>>>>>>> Will take few minutes to finish...      <<<<<<<<<<<<<<"
echo ""
