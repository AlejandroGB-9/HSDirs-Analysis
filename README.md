# HSDir Research Toolkit

This repository contains Python programs for collecting and analyzing Tor Hidden Service Directory (HSDir) data:

- `hsdir-fetcher.py` — retrieves HSDir-related data through the Tor control port.
- `hsdir-analysis.py` — analyzes the collected data and generates statistical results and visualizations.
- `test-service.py` — optionally verifies that the Tor service and Python environment are working correctly.

The instructions below are intended for Debian-based Linux systems.

## 1. Requirements

The following are required:

- Debian-based Linux distribution
- Python 3.12 or newer
- Tor
- A working Tor control port
- Permission to access Tor's control authentication cookie
- Internet access for installing packages and collecting data

Check the installed Python version:

```bash
python3 --version
```
If the version is older than 3.12, install or configure Python 3.12 or newer before continuing.

## Install Tor and Python

Install Tor, Python, `pip`, and the Python virtual-environment package:

```bash
sudo apt-get update
sudo apt-get install -y tor python3 python3-pip python3-venv
```

Start Tor and configure it to start automatically when the system boots:

```bash
sudo systemctl enable --now tor
```

Verify that the service is running:

```bash
systemctl status tor
```

Press `q` to exit the status view.

## Allow the Current User to Access Tor

Add the current user to the Debian Tor group:

```bash
sudo usermod -aG debian-tor "\$USER"
```

Log out and log back in, or reboot the machine, so that the new group membership takes effect.

After logging in again, verify the group membership:


```bash
groups
```

The output should include:

```text
debian-tor
```

## Create and Activate a Python Virtual Environment

From the root directory of this repository, create a virtual environment:

```bash
python3 -m venv ./venv
```

Activate it:

```bash
source ./venv/bin/activate
```

After activation, the shell prompt should contain `(venv)`.

Upgrade `pip`:

```bash
python -m pip install --upgrade pip
```

The virtual environment must be activated whenever the repository scripts are executed. To activate it again in a new terminal session, run:

```bash
source ./venv/bin/activate
```

## Install the Python Dependencies

Install the external dependencies used by `hsdir-fetcher.py` and `hsdir-analysis.py` directly with `pip`:

```bash
python -m pip install stem pandas numpy matplotlib scipy
```

Confirm that the packages are available:

```bash
python -c "import stem, pandas, numpy, matplotlib, scipy; print('Dependencies installed successfully.')"
```

## Configure the Tor Control Port

Create a backup of the existing Tor configuration before editing it:

```bash
sudo cp /etc/tor/torrc /etc/tor/torrc.bak
```

Open the Tor configuration file:

```bash
sudo editor /etc/tor/torrc
```

Add the following lines, or uncomment them if they already exist:

```text
ControlPort 9051
CookieAuthentication 1
```

Save the file and restart Tor:

```bash
sudo systemctl restart tor
```

Verify that Tor restarted successfully:

```bash
systemctl status tor
```

You can also verify that the control port is listening:

```bash
ss -ltn | grep 9051
```

The expected output should show Tor listening on port `9051`.

If the control port is not available, inspect the Tor logs:

```bash
sudo journalctl -u tor --no-pager -n 50
```

## Create the Research Data Directory

Create the directory used to store downloaded and generated research data:

```bash
mkdir -p "\$HOME/hsdir\_research\_data"
```

Restrict access to the current user:

```bash
chmod 700 "\$HOME/hsdir\_research\_data"
```

The directory should be readable, writable, and accessible only by its owner.

The scripts must use this directory through their `DATA_DIR` variable. Set `DATA_DIR` to the absolute path of the directory, for example:

```python
DATA\_DIR = "/home/your-user/hsdir\_research\_data"
```

Replace `your-user` with the username used to run the scripts.

To determine the correct absolute path, run:


```bash
echo "\$HOME/hsdir\_research\_data"
```

Use the resulting path in the scripts.

## Verify the Installation

If `test-service.py` is included in the repository, run it while the virtual environment is active:

```bash
python3 test-service.py
```

## Run the HSDir Fetcher

From the repository root:


```bash
python3 hsdir-fetcher.py
```

The fetcher should save its output under the configured `DATA_DIR` directory.

Do not run the script with `sudo`. Running it as the configured user ensures that files are created with the expected ownership and permissions.

## Run the Analysis

After the fetcher has produced the required input data, execute the the analysis script under the root directory `DATA_DIR`:

```bash
python3 hsdir-analysis.py
```

Run the analysis only after the fetcher has completed or has produced a complete input dataset.

## Stopping Tor

Tor can be stopped when it is no longer needed:

```bash
sudo systemctl stop tor
```

To prevent Tor from starting automatically at boot:


```bash
sudo systemctl disable tor
```

To restore the original Tor configuration, stop Tor, restore the backup, and start Tor again:

```bash
sudo systemctl stop tor
sudo cp /etc/tor/torrc.bak /etc/tor/torrc
sudo systemctl start tor
```

## Removing the user from the Tor group

```bash
sudo gpasswd --delete "$USER" debian-tor
```

Log out and back in, or reboot, for the group removal to take effect. You can verify the current group membership with:

```bash
groups
```
