# 1. HSDirs Set-up

Installation and initialization of the service Tor:

```
sudo apt-get install tor

sudo systemctl start tor
```

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
(venv) sudo ./venv/bin/python test-service.py
```

# 2. Setting-up a private Tor network for testing

Before making the program a private Tor network for research with HSDirs is needed.

In this project 2 tools will be used for creating a private Tor network under different conditions to later pass to a the real environment of Tor:

- Chutney
    - 
    - Easy to set up and iterate
    - Real Tor behavior (no abstraction)
    - Great for:
        - HSDir flag behavior
        - descriptor upload/debugging
        - protocol-level experiments
        - Fast feedback loop (seconds/minutes)
    - Spin up a real mini Tor network (authorities, relays, clients) locally
    - Use actual Tor binaries and real networking (loopback)
    - WARNINGS:
        - Locally runned
        - Small scale (10-20 nodes)
        - No built-in latency, packet loss, or Internet topology

<!-- - Shadow
    -
    - Simulates a full network (latency, bandwidth, churn)
    - Runs Tor inside that simulated environment
    - Massive scale (100-1K nodes)
    - Realistic network conditions (delay, congestion, jitter)
    - Deterministic & reproducible experiments
    - Built for:
        - Performance evaluation
        - Anonymity research
        - Large-scale HSDir studies
    - WARNINGS:
        - Harder to set up
        - Heavy (RAM-intensive)
        - Some abstraction (not a perfect-real network behavior) -->

## 2.1 Chutnet set-up

Clone the stable gitlab repository of Chutney:

```
git clone https://gitlab.torproject.org/tpo/core/chutney.git

cd chutney
```

With the previous python environment install the following dependencies:

```
pip install cryptography paramiko typeguard tomli-w
```

The following networks for the used version of Chutney are:

1. <p><b>basic</b></p><p>Purpose: Minimal Tor network with a few relays and one authority.</p><p>Use: Quick functional tests, learning, and protocol debugging.</p><p>Notes: No hidden services. Small enough to bootstrap in seconds.</p>
2. <p><b>basic-min</b></p><p>Purpose: Ultra-minimal network.</p><p>Use: Fastest setup for testing core Tor behavior.</p><p>Notes: Often used for automated CI tests or scripting experiments.</p>
3. <p><b>basic-arti</b></p><p>Purpose: Uses the Arti Rust Tor client instead of the standard Tor binary.</p><p>Use: Test compatibility with Arti or integrate Arti clients in research.</p>
4. <p><b>basic-dual-stack</b></p><p>Purpose: Basic network with IPv4 + IPv6 enabled.</p><p>Use: Experiments requiring dual-stack behavior or testing IPv6 connectivity.</p>
5. <p><b>basic-families</b></p><p>Purpose: Includes relay family configurations (relays “trusting” each other).</p><p>Use: Study family policies or relay selection mechanisms.</p>
6. <p><b>bridges-min</b></p><p>Purpose: Minimal Tor network including bridges (obfuscated entry relays).</p><p>Use: Test censored-network behavior, pluggable transport, or bridge distribution.</p>
7. <p><b>bridges-min-arti</b></p><p>Purpose: Bridges + Arti clients.</p><p>Use: Experiments combining bridges and Rust-based clients.</p>
8. <p><b>bridges+hs-v3</b></p><p>Purpose: Bridges and v3 onion services in the same network.</p><p>Use: Realistic censored network + onion services testbed.</p>
9. <p><b>bridges+ipv6-min</b></p><p>Purpose: Bridges + IPv6-enabled minimal network.</p><p>Use: Test IPv6 behavior behind bridges.</p>
10. <p><b>bwscanner</b></p><p>Purpose: Network preconfigured for bandwidth scanning experiments.</p><p>Use: Measure Tor bandwidth-based relay selection, exit bandwidth, or traffic simulation.</p>
11. <p><b>hs-v3</b></p><p>Purpose: Modern v3 onion service network.</p><p>Use: Study HSDir assignment, descriptor uploads, onion-service connectivity.</p><p>Notes: This is the one you want for your HSDir experiments. Includes authorities, relays, and clients.</p>
12. <p><b>hs-v3-min</b></p><p>Purpose: Smaller / faster version of hs-v3.</p><p>Use: Quick tests or debugging HSDir behavior with fewer nodes.</p>
13. <p><b>hs-v3-arti</b></p><p>Purpose: v3 onion services tested with Arti clients.</p><p>Use: Research v3 HSDir assignment with Rust Tor clients.</p>
14. <p><b>hs-v3-ipv6</b></p><p>Purpose: v3 onion service network with IPv6-enabled relays.</p><p>Use: Study HSDir + IPv6 behavior, IPv6 descriptor assignment.</p>
15. <p><b>hs-v3-rd-arti</b></p><p>Purpose: v3 HS network with Arti + relay descriptor focus.</p><p>Use: Test descriptor upload/fetch behavior with Arti clients.</p>
16. <p><b>hs-ob-v3</b></p><p>Purpose: v3 onion services + obfs4 bridges.</p><p>Use: Simulate censored networks using onion services and bridges simultaneously.</p>
17. <p><b>mixed+hs-v3</b></p><p>Purpose: Mix of normal relays, authorities, clients, and HS nodes.</p><p>Use: More realistic HSDir experiments in medium-scale setups.</p>
18. <p><b>mixed+hs-v3-ipv6</b></p><p>Purpose: Like mixed+hs-v3 but IPv6-enabled.</p><p>Use: Large-scale, realistic HSDir testing including dual-stack relays.</p>
19. <p><b>single-onion-v3 / single-onion-v3-ipv6-md</b></p><p>Purpose: Minimal network with a single v3 onion service.</p><p>Use: Focused debugging of one onion service, descriptor uploads, and client connections.</p>

For this project the options which are interesting to study HSDir behavior are ```hs-v3```, ```hs-v3-min```, or ```miexd+hs-v3```.

Configure and initialize a HSDir network:

```
./chutney init --net hs-v3
./chutney bootstrap
./chutney status
./chutney verify
```

Alternatively, execute the BASH file 'chutney-init.sh' for automation:

```
./chutney-init.sh
```

# 2.1.1 Chutney-based programs to use:

The following files have been made to create and adapted program to collect HSDir descriptor information for the analysis. This programs were made in order to understand the information that could be retrieved from a simulated private network before working on a real one:

- chutney-descriptors.py @ Main program for Chutney for one request line-up
- descParse.py @ File created to parse descriptor information into a JSON file for analysis, required on main programs of chutney to parse information
- mulreq-chutney-descriptors.py @ Alternative file created to test multiple request to have a real execution line to gather information

<!-- # 2.2 Shadow set-up

Before installing of cloning the repository of Shadow it is necessary to install the required dependencies for it to properly work. Given this is made in a debian-based environment refer to its guide to install the dependencies with the owned packet manager. Nevertheless, the following steps are custom made for errors avoidance:

> ⚠️ **CAUTION-WARNING:** The following actions may be sensitive. Proceed with caution and make the intended backup files. These steps were need in this scenario.

As Shadow requires higher system limits and to manage thousands of open files and memory maps simultaneously (network-size dependant). 

1. Kernel Limits increase - Backup and edit ```/etc/sysctl.conf```:

    Backup:
    ```
    sudo cp /etc/sysctl.conf /etc/sysctl.conf.bak
    sudo nano /etc/sysctl.conf
    ```
    Edit:
    ```
    fs.nr_open = 10485760
    fs.file-max = 10485760
    vm.max_map_count = 1048576
    kernel.threads-max = 4194304
    kernel.pid_max = 4194304
    # Avoid SIGSEGV errors: Shadow allowed to interpose on syscalls 
    kernel.yama.ptrace_scope = 0
    ```
    Apply:
    ```
    sudo sysctl -p
    ```

2. User Limits edition - Backup and edit ```/etc/security/limits.conf```:
    Backup:
    ```
    sudo cp /etc/security/limits.conf /etc/security/limits.conf.bak
    sudo nano /etc/security/limits.conf
    ```
    Edit:
    ```
    * soft nofile 1048576
    * hard nofile 1048576
    * soft nproc unlimited
    * hard nproc unlimited
    * soft stack unlimited
    * hard stack unlimited
    ```
    > To apply these settings log out with and log in to take effect.

> **🛡️SAFE ZONE:** The following steps will be related to the instalation and start up of Shadow

1. Dependencies installation:

    ```
    # Required dependencies for Shadow, Standalone Tor and TGen

    sudo apt-get update
    sudo apt-get install -y \
    cmake \
    ninja-build \
    pkg-config \
    gcc \
    g++ \ 
    gdb \
    libglib2.0-dev \
    libclang-dev \
    python3-pip \
    python3-pygraphviz \
    python3-networkx \
    python3-yaml \
    xz-utils \
    automake \
    autoconf \
    libtool \
    openssl \
    libssl-dev \
    libevent-dev \
    zlib1g-dev

    # Installing Rust - rustup: https://rustup.rs . Press enter to install default configuration of RUST

    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    source $HOME/.cargo/env
    ```

2. Build Shadow (Memory-optimized):

    To avoid a high request and usage of RAM the following limits the parallel jobs to prevent the build from crashing or being killed by the OOM killer. (estimated build and install - **12 min**)

    ```
    git clone https://github.com/shadow/shadow.git
    cd shadow

    # Build with 1 job at a time - under user's RAM limit

    export CARGO_BUILD_JOBS=1 
    ./setup build --jobs 1
    ./setup install
    ```

3. Build TGen and (standalone binary) Tor - Shadow required

    These tools build from source ensures compatibility with Shadow's syscall interception.

    Before the installation of these, is highly recommended to take into consideration the following structure:

    ```
    ~/HSDir-Analysis/
    ├── shadow/
    ├── tor/
    └── tgen/
    ```

    TGen:

    ```
    sudo apt-get install libigraph-dev
    git clone https://github.com/shadow/tgen.git
    cd tgen
    mkdir build && cd build
    cmake ..
    make -j1
    sudo make install
    ```

    Tor:
    ```
    git clone https://gitlab.torproject.org/tpo/core/tor.git
    cd tor
    ./autogen.sh
    ./configure --disable-asciidoc --disable-unittests --disable-manpage
    make -j$(nproc)
    ```

4. > ⚠️ **CAUTION: ```PATH``` Update**

    The update of the ```PATH``` will allow Shadow to find the binaries of ```tor```and ```tgen``` every time it runs a simulation. To avoid continuous ```export``` the ```PATH``` is updated in the ```~/.bashrc``` file.

    ```
    sudo nano ~/.bashrc
    ```
    ```
    export PATH="$PATH:/home/<youruser>/HSDir-Analysis/tor/src/app"
    export PATH="$PATH:/home/<youruser>/HSDir-Analysis/tor/src/tools"
    export PATH="$PATH:/home/<youruser>/HSDir-Analysis/tgen/build"
    export PATH="$PATH:/home/<youruser>/.local/bin"
    ```
    ```
    source ~/.bashrc
    ```

5. Running Shadow Simulation

    To manage RAM effectively, avoid the "1% scale" public Tor models (which requires 30GB+ RAM). Use the built-in minimal template:

    ```
    cd shadow/examples/docs/tor

    # Remove old data and run
    rm -rf shadow.data/

    #Run
    shadow --template-directory shadow.data.template shadow.yaml > shadow.log
    ```

6. Stopping Shadow Simulation

    As Shadow is a discrete-event simulator. It will stop automatically when it reaches the end of the simulation time defined in the configuration file ```shadow.yaml```.

    Example:

    ```
    general:
        stop_time: 30 min
    ```

    Other ways to stop it are:

    - Stopping the background process:

        ```
        pkill -INT shadow
        ```
    - Emergency stop:
    
        ```
        pkill -9 shadow
        pkill -9 tor
        pkill -9 tgen
        ``` -->

# 3. Execution

```
(venv) sudo ./venv/bin/python hsdir_desc_fetcher.py
```