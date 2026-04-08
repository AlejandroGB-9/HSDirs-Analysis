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

# Verify nodes are running
ps aux | grep "[t]or" | grep chutney

# Check if port 9009 is now listening
ss -tlnp | grep 9009

sleep 10

# Find the active network directory (latest timestamp)
ls -lt net/nodes.* | head -1

# Check logs from that directory
tail -100 net/nodes.*/*/tor.log