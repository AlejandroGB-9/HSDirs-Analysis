#!/usr/bin/env bash

# Reset all existing firewall structures
iptables -F
iptables -X

# Drop all incoming external packets; permit all outbound traffic loops
iptables -P INPUT DROP
iptables -P FORWARD DROP
iptables -P OUTPUT ACCEPT

# Ensure the local OS and internal processes can communicate unrestricted
iptables -A INPUT -i lo -j ACCEPT
iptables -A OUTPUT -o lo -j ACCEPT

# Permit incoming packets ONLY if they belong to an outbound connection initiated in the machine.
iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT

# Explicitly permit local loopback processes to access the virtual proxy array
iptables -A INPUT -p tcp -s 127.0.0.1 --dport 8080:8179 -j ACCEPT
iptables -A INPUT -p tcp --dport 8080:8179 -j DROP

# Explicit Drop Rule for Invalid Packets
iptables -A INPUT -m conntrack --ctstate INVALID -j DROP

echo "[INFO] Iptable rules applied."
