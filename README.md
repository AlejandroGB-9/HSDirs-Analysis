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
#Uncomment line "ControlPort 9051"

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

- Shadow
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
        - Some abstraction (not a perfect-real network behavior)

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
./chutney configure
./chutney start
./chutney wait_for_bootstrap #Be patient untill all nodes return SUCCESS
./chutney status
./chutney verify
```
