# 1. HSDirs Set-up

Installation and initialization of the service Tor:

```
sudo apt-get install tor

sudo systemctl start tor
```

Add the user to the tor group to properly execute the program without the requirement of using ```sudo```, in this case the machine is debian-based:

```
sudo usermod -aG debian-tor $USER
```

Proceed to log-out or reboot to take effect.

For this study it is required the use of Python. To properly work please make use of a python version >= 3.12.

Install the following dependencies:

```
sudo apt-get install pip3 python3-venv -y
```

Set-up python environment and upgrade pip:

```
python3 -m venv ./venv

source ./venv/bin/activate

pip install --upgrade pip
```

Install dependencies for the project:

```
# Dependencies: stem

pip install -r requirements.txt
```

It is recommended to make a backup file of the Tor configuration file before proceding to edit the configuration file to make a Tor controller and make queries to a HSDir:

```
sudo cp /etc/tor/torrc /etc/tor/torrc.bak
```

Then edit the configuration file torrc and enable the port that will use the controller to query HSDirs:

```
#Uncomment line "ControlPort 9051" and "CookieAuthentication 1"

sudo vim /etc/tor/torrc
```

Restart the Tor daemon process to apply changes:

```
sudo systemctl restart tor
```

To verify that everything is working correcty, with python execute the file test-service.py:

```
(venv) python3 test-service.py
```

Create a folder-directory in your drive called hsdir_research_data, included the location into the variable DATA_DIR on the program, and modifiy the permissions so only the user can interact with it:

```
sudo chmod 700 hsdir_research_data
```
