#!/usr/bin/env bash

# 1. Reset all existing firewall structures
iptables -F
iptables -X

# 2. Establish Global Default Filter Policies
# Drop all incoming external packets; permit all outbound traffic loops
iptables -P INPUT DROP
iptables -P FORWARD DROP
iptables -P OUTPUT ACCEPT

# 3. Permissive Loopback Interface Rule
# Ensure the local OS and internal processes can communicate unrestricted
iptables -A INPUT -i lo -j ACCEPT
iptables -A OUTPUT -o lo -j ACCEPT

# 4. Stateful Connection Tracking Rule
# Crucial: Permits incoming packets ONLY if they belong to an outbound connection
# initiated by your machine (e.g., Tor connecting out to HSDirs or Guard nodes)
iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT

# 5. Local Virtual Port Range Protection (Double-Layer Defense)
# Explicitly permit local loopback processes to access the virtual proxy array
iptables -A INPUT -p tcp -s 127.0.0.1 --dport 8080:8179 -j ACCEPT
iptables -A INPUT -p tcp --dport 8080:8179 -j DROP

# 6. Explicit Drop Rule for Invalid Packets
iptables -A INPUT -m conntrack --ctstate INVALID -j DROP

echo "[INFO] Iptable rules applied."