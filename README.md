# 1. HSDirs Set-up

Installation and initialization of the service Tor:

```
sudo apt-get install tor

sudo systemctl start tor
```

Add the device user to the Tor group (debian-based) to properly execute the program without the requirement of using ```sudo``` avoiding security risks:

```
sudo usermod -aG debian-tor $USER
```

Log-out or reboot to take effect.

The study requires the use of Python. Using Python version >= 3.12 as the current efficient version for this project.

Install the environment dependencies:

```
sudo apt-get install pip3 python3-venv -y
```

Set-up python environment and upgrade pip:

```
python3 -m venv ./venv

source ./venv/bin/activate

pip install --upgrade pip
```

Install the present dependencies from files `hsdir-fetcher.py` and `hsdir-analysis.py`:

```
# Dependencies: stem

pip install -r requirements.txt
```

Recommended: Make a backup file of the Tor configuration file before editing it for the use of Tor controller and queries:

```
sudo cp /etc/tor/torrc /etc/tor/torrc.bak
```

Edit the configuration file `torrc`:

```
#Uncomment line "ControlPort 9051" and "CookieAuthentication 1"

sudo vim /etc/tor/torrc
```

Restart the Tor daemon process to apply changes:

```
sudo systemctl restart tor
```

Verify that Tor is working as intended with demo-file `test-service.py`:

```
(venv) python3 test-service.py
```

Create a folder-directory in your drive, included the location into the variable DATA_DIR on the program.
Modifiy the permissions of the folder so only the user can interact with it:

```
sudo chmod 700 $folder
```
