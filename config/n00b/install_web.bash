echo ""
echo ">>>>>>>>>> Installing node   <<<<<<<<<<<<<<"
echo ""

brew install node
brew install npm

echo ""
echo ">>>>>>>>>> Stylus                <<<<<<<<<<<<<<"
echo ""

sudo npm install -g stylus
sudo npm install -g nib

echo ""
echo ">>>>>>>>>> Add test domains to /etc/hosts               <<<<<<<<<<<<<<"
echo ""

cat /etc/hosts > hosts.tmp
echo "127.0.0.1       test.staging.example.com testprod.example.com test.example.com" >> hosts.tmp
awk '!seen[$0]++' hosts.tmp > hosts.tmp2
sudo mv hosts.tmp2 /etc/hosts
rm hosts.tmp*
