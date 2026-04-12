# Stop the system Tor daemon that may conflict
echo "Stopping Tor services"
sudo systemctl stop tor
# Or if using service
sudo service tor stop

#Source python environment
echo "Setting up python env for Chutney"
source ./venv/bin/activate

# Remove ALL node directories and network artifacts
echo "Cleaning up chutney remnants"
cd ~/HSDir-Analysis/chutney

rm -rf net/nodes*

# Verify cleanup
ls -la net/

# This creates the correct topology automatically
echo "Setting up HSDir network via Chutney"
./chutney init --net hs-v3

# Bootstrap the network
./chutney bootstrap

# Should show something like:
# LISTEN  0  128  127.0.0.1:9009  0.0.0.0:*  users:(("tor",pid=XXXX,fd=XX))

./chutney verify

echo "Onion service address:"
cat net/nodes.*/network.json | grep hs_hostname | tail -1