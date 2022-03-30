su root
# Don't you hate when you need to root to fix dumb issues?
sudo service network-manager stop
sudo rm /var/lib/NetworkManager/NetworkManager.state
sudo service network-manager start
sudo reboot -h now