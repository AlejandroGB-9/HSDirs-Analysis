#!/usr/bin/env python3

#  __    __   ______   _______   __                                                     
# /  |  /  | /      \ /       \ /  |                                                   
# $$ |  $$ |/$$$$$$  |$$$$$$$  |$$/   ______             
# $$ |__$$ |$$ \__$$/ $$ |  $$ |/  | /      \           
# $$    $$ |$$      \ $$ |  $$ |$$ |/$$$$$$  |           
# $$$$$$$$ | $$$$$$  |$$ |  $$ |$$ |$$ |  $$/                 
# $$ |  $$ |/  \__$$ |$$ |__$$ |$$ |$$ |             
# $$ |  $$ |$$    $$/ $$    $$/ $$ |$$ |            
# $$/   $$/  $$$$$$/  $$$$$$$/  $$/ $$/             
#  ________           __                __                                                      
# /        |         /  |              /  |                                                     
# $$$$$$$$/______   _$$ |_     _______ $$ |____    ______    ______                             
# $$ |__  /      \ / $$   |   /       |$$      \  /      \  /      \                            
# $$    |/$$$$$$  |$$$$$$/   /$$$$$$$/ $$$$$$$  |/$$$$$$  |/$$$$$$  |                           
# $$$$$/ $$    $$ |  $$ | __ $$ |      $$ |  $$ |$$    $$ |$$ |  $$/                            
# $$ |   $$$$$$$$/   $$ |/  |$$ \_____ $$ |  $$ |$$$$$$$$/ $$ |                                 
# $$ |   $$       |  $$  $$/ $$       |$$ |  $$ |$$       |$$ |                                 
# $$/     $$$$$$$/    $$$$/   $$$$$$$/ $$/   $$/  $$$$$$$/ $$/

import os
import time
import json
import signal
import sys
import datetime
import threading
import traceback
import random
import re
import base64
import ast
from stem.control import Controller, EventType
from stem.descriptor.remote import DescriptorDownloader

# Configuration
CONTROL_PORT = 9051
NUM_EPHEMERALS = 50              # Expanded overview array scale
ROTATION_INTERVAL = 86400         # 24h before sweeping and creating fresh ephemerals
CONSENSUS_INTERVAL = 600        # 1 hour tracking snapshots
PROBE_INTERVAL = 1200               # 20 mins (Respectful frequency for checking third-party HSDirs)

with open('target_onions/owned_onions.json', 'r', encoding='utf-8') as f: OWNED_STATIC_ONIONS = json.load(f)
with open('target_onions/third_party_onions.json', 'r', encoding='utf-8') as f: THIRD_PARTY_ONIONS = json.load(f)

SHUTDOWN_FLAG = False
DATA_DIR = "/mnt/second-drive/hsdir_research_data"
os.makedirs(DATA_DIR, exist_ok=True)

# Thread-safe logging helpers
state_lock = threading.Lock()

# Global state tracking for active ephemerals
active_ephemerals = set()
tracked_target_hsdirs = {}  # Format: { fingerprint: {"onion_address": x, "first_seen": x} }
hsdir_contact_cache = {}    # Format: { fingerprint: {"contact": str, "last_fetched": datetime} }

# Remote directory authority downloader reference
downloader = DescriptorDownloader(timeout=15)

def save_json_log(filename, data):
    try:
        with state_lock:
            filepath = os.path.join(DATA_DIR, filename)
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data) + ",\n")

    except Exception as e:
        print(f"[LOG EXCEPTION] Failed writing entry to {filename}: {e}")
        traceback.print_exc()

def get_authenticated_controller():
    """Generates isolated socket connections to prevent control channel deadlocks."""
    controller = Controller.from_port(port=CONTROL_PORT)
    controller.authenticate()
    return controller

def run_event_listener():
    print("[INIT] Launching dedicated event registration pipe...")
    try:
        conn = get_authenticated_controller()
        print("[DEBUG-LISTENER] Launching event listener")
        conn.add_event_listener(hs_desc_event_listener, EventType.HS_DESC)
        while not SHUTDOWN_FLAG:
            time.sleep(1)
    except Exception as e:
        print(f"[CRITICAL] Event listener stream failed to register: {e}")
        traceback.print_exc()

def signal_handler(sig, frame):
    global SHUTDOWN_FLAG
    print("\n[EXIT] Shutdown signal received. Cleaning up and exiting...")
    SHUTDOWN_FLAG = True

def calculate_ring_distance(hsdir_index_hex, onion_target_index_hex):
    """
    Computes the clockwise distance between an HSDir's ring hash index and an
    onion descriptor target index on Tor's 256-bit circular integer space.
    Formula: Delta = (HSDir_Index - Onion_Index) mod 2^256
    """
    try:
        if not hsdir_index_hex or not onion_target_index_hex:
            return None
        RING_SIZE = 2**256
        hsdir_val = int(hsdir_index_hex, 16)
        onion_val = int(onion_target_index_hex, 16)
        distance = (hsdir_val - onion_val) % RING_SIZE
        return distance, round((distance / RING_SIZE) * 100, 2)
    except Exception:
        return None

def get_relay_contact_info(conn, fingerprint, node, utc_time):

    global hsdir_contact_cache

    # STRATEGY: REMOTE LOOKAHEAD ENGINE FOR CONTACT STRINGS (Refreshed every 24 Hours)
    contact_string = "None Specified"

    with state_lock:
        cache_entry = hsdir_contact_cache.get(fingerprint)

    needs_remote_fetch = False

    if not cache_entry:
        needs_remote_fetch = True
    else:
        # Verify if 24 hours have elapsed since this fingerprint's last remote check
        if (utc_time - cache_entry["last_fetched"]) >= datetime.timedelta(days=1):
            needs_remote_fetch = True
        else:
            contact_string = cache_entry["contact"]

    if needs_remote_fetch:
        try:
            # Query a remote directory authority mirror directly
            query = downloader.get_server_descriptors(fingerprints=[fingerprint])
            remote_contact = "None Specified"

            print(f"[DEBUG-DOWNLOADER] The query for contact retrieved:\n{query}")
            
            for desc in query:
                if getattr(desc, 'contact', None):
                    remote_contact = str(desc.contact)
                    if remote_contact:

                        if isinstance(remote_contact, bytes): 
                            s = remote_contact.decode("utf-8", errors="ignore") 
                        else: 
                            s = str(remote_contact).strip() 

                        while True: 
                            try: 
                                value = ast.literal_eval(s) 
                                
                                if isinstance(value, bytes): 
                                    s = value.decode("utf-8", errors="ignore") 
                                elif isinstance(value, str): 
                                    s = value 
                                else: 
                                    break

                            except (ValueError, SyntaxError): 
                                break

                        remote_contact = s.strip() 
                        
                break
            
            # Process tracking for modifications or initial profile detections
            old_contact = cache_entry["contact"] if cache_entry else None
            if old_contact != remote_contact:
                change_entry = {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "fingerprint": fingerprint,
                    "nickname": node.nickname if node.nickname else None,
                    "ip_address": node.address if node.address else None,
                    "old_contact": old_contact if old_contact else "INITIAL_DISCOVERY_RECORD",
                    "new_contact": remote_contact
                }
                # Output variations directly to a specialized historical timeline log
                save_json_log("hsdir_contact_history.json", change_entry)
            
            contact_string = remote_contact
            
            with state_lock:
                hsdir_contact_cache[fingerprint] = {
                    "contact": remote_contact,
                    "last_fetched": utc_time
                }
        except Exception as remote_err:
            # Fallback safely to current cache elements if network endpoints time out
            if cache_entry:
                contact_string = cache_entry["contact"]
                print(f"[WARN] Remote mirror unreachable for {fingerprint}. Using cache fallback. Error: {remote_err}")
                
            else:
                contact_string = "Fetch Failed (Authority Timeout)"
                print(f"[WARN] Remote download failed for new fingerprint {fingerprint}. Error: {remote_err}")
    
    return contact_string

# 1. Asynchronous Event Monitor: Catches Hash Ring positions and targets
def hs_desc_event_listener(event):
    global tracked_target_hsdirs

    try:
        
        onion_id = getattr(event, 'address', None)
        
        if not onion_id:
            return

        full_onion_address = f"{onion_id}.onion"

        is_tracked = (full_onion_address in OWNED_STATIC_ONIONS or 
                      full_onion_address in THIRD_PARTY_ONIONS or 
                      onion_id in active_ephemerals)

        if not is_tracked:
            return

        # print(f"[DEBUG-LISTENER] Tracked targets in memory: {len(tracked_target_hsdirs)}")
        # print(f"[DEBUG-LISTENER] Processing Onion ID: {full_onion_address}")
        
        # Classify the service identity profile
        if full_onion_address in OWNED_STATIC_ONIONS:
            service_type = "STATIC_CONTROL"
        # if full_onion_address in TESTING_ONIONS:
        #     service_type = "STATIC_CONTROL"
        elif onion_id in active_ephemerals:
            service_type = "EPHEMERAL_VARIABLE"
        elif full_onion_address in THIRD_PARTY_ONIONS:
            service_type = "THIRD_PARTY_PROBE"
        else:
            return

        # print(f"Event for onion @{onion_id}.onion of service type {service_type}:\n{event}")

        # Combine text elements to parse raw un-mapped v3 data fields
        raw_event_text = " ".join(list(event)) if isinstance(event, (list, tuple)) else str(event)

        # Fallback 1: Extract HSDir Fingerprint
        hsdir_fp = getattr(event, 'hsdir', None)
        if not hsdir_fp:
            fp_match = re.search(r'\$([0-9A-Fa-f]{40})', raw_event_text)
            if fp_match:
                hsdir_fp = fp_match.group(1).upper()

        if not hsdir_fp:
            return

        # Custom Field: Extract Descriptor ID (Blinded Public Key)
        # Matches the 43-character Base64 token directly succeeding the HSDir fingerprint/nickname
        descriptor_id = None
        desc_match = re.search(r'\$[0-9A-Fa-f]{40}(?:~[^\s]+)?\s+([A-Za-z0-9+/]{43})(?:\s+|$)', raw_event_text)
        if desc_match:
            descriptor_id = desc_match.group(1)

        # Fallback 2: Extract Replica Index
        replica = getattr(event, 'replica', None)
        if replica is None:
            rep_match = re.search(r'REPLICA=(\d+)', raw_event_text, re.IGNORECASE)
            if rep_match:
                replica = int(rep_match.group(1))

        # Fallback 3: Extract HSDIR_INDEX
        hsdir_index_hrt = getattr(event, 'hsdir_index', None)
        if not hsdir_index_hrt:
            idx_match = re.search(r'HSDIR_INDEX=([0-9A-Fa-f]{64})', raw_event_text, re.IGNORECASE)
            if idx_match:
                hsdir_index_hrt = idx_match.group(1).upper()

        if not hsdir_index_hrt:
            return

        # Fallback 4: Extract Reason String
        reason = getattr(event, 'reason', None)
        if not reason:
            reason_match = re.search(r'REASON=(\w+)', raw_event_text, re.IGNORECASE)
            if reason_match:
                reason = reason_match.group(1)

        # Real-time conversion of Descriptor ID to 256-bit hex layout (onion_target_index)
        onion_target_index_hex = None
        ring_distance = None
        ring_position = None
        if descriptor_id:
            try:
                padded_desc_id = descriptor_id + "=" * ((4 - len(descriptor_id) % 4) % 4)
                desc_bytes = base64.b64decode(padded_desc_id.encode('utf-8'))
                onion_target_index_hex = desc_bytes.hex().upper()
                
                if hsdir_index_hrt:
                    ring_distance, ring_position = calculate_ring_distance(hsdir_index_hrt, onion_target_index_hex)
            except Exception:
                pass

        actual_timestamp = datetime.datetime.now().isoformat()

        print(f"[DEBUG-LISTENER] Processing Onion ID: {full_onion_address}")
        print(f"Timestamp: {actual_timestamp}")
        print(f"Service Type: {service_type}")
        print(f"Onion Address: {full_onion_address}")
        print(f"Action: {event.action}")
        print(f"HSDir Fingerprint: {hsdir_fp}")
        print(f"Replica: {replica}")
        print(f"HSDir Index HRT: {hsdir_index_hrt}")
        print(f"Onion Target Index: {onion_target_index_hex}")
        print(f"Ring Distance: {ring_distance}")
        print(f"Ring Position: {ring_position}")
        print(f"Reason: {reason}")
        print(f"Event keys: {list(event)}")

        with state_lock:
            if hsdir_fp not in tracked_target_hsdirs:
                tracked_target_hsdirs[hsdir_fp] = {
                    "associated_onion": f"{full_onion_address}",
                    "service_type": service_type,
                    "first_discovery": actual_timestamp,
                    "consecutive_hourly_absences": 0,
                    "last_timestamp": actual_timestamp,
                    "last_known_action": event.action,
                    "descriptor_id_b64": descriptor_id,
                    "onion_target_index_hex": onion_target_index_hex,
                    "hsdir_index_hrt": hsdir_index_hrt, # Position on the Hash Ring Table
                    "ring_distance": ring_distance,
                    "ring_position": ring_position,
                }
            else:
                tracked_target_hsdirs[hsdir_fp]["last_known_action"] = event.action
                tracked_target_hsdirs[hsdir_fp]["last_timestamp"] = actual_timestamp
                tracked_target_hsdirs[hsdir_fp]["descriptor_id_b64"] = descriptor_id if descriptor_id else tracked_target_hsdirs[hsdir_fp]["descriptor_id_b64"]
                tracked_target_hsdirs[hsdir_fp]["onion_target_index_hex"] = onion_target_index_hex if onion_target_index_hex else tracked_target_hsdirs[hsdir_fp]["onion_target_index_hex"]
                tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt"] = hsdir_index_hrt if hsdir_index_hrt else tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt"]
                tracked_target_hsdirs[hsdir_fp]["ring_distance"] = ring_distance if ring_distance else tracked_target_hsdirs[hsdir_fp]["ring_distance"]
                tracked_target_hsdirs[hsdir_fp]["ring_position"] = ring_position if ring_position else tracked_target_hsdirs[hsdir_fp]["ring_position"]


        log_entry = {
            "timestamp": actual_timestamp,
            "service_type": service_type,
            "onion_address": full_onion_address,
            "action": event.action,
            "hsdir_fingerprint": hsdir_fp,
            "replica": replica,
            "descriptor_id_b64": descriptor_id if descriptor_id else tracked_target_hsdirs[hsdir_fp]["descriptor_id_b64"],
            "onion_target_index_hex": onion_target_index_hex if onion_target_index_hex else tracked_target_hsdirs[hsdir_fp]["onion_target_index_hex"],
            "hsdir_index_hrt": hsdir_index_hrt if hsdir_index_hrt else tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt"], # Position on the Hash Ring Table
            "ring_distance": ring_distance if ring_distance else tracked_target_hsdirs[hsdir_fp]["ring_distance"],
            "ring_position": ring_position if ring_position else tracked_target_hsdirs[hsdir_fp]["ring_position"],
            "reason": reason
        }

        print(f"[HRT EVENT] {event.action} -> Onion: {full_onion_address}... on HRT Position: {hsdir_index_hrt} with distance {ring_distance}")
        save_json_log("hsdir_ring_events.json", log_entry)

    except Exception as e:
        print(f"[EXC-EVENT-LISTENER] Error parsing incoming stream frame: {e}")
        traceback.print_exc()

# 2. Third-Party Active Prober Engine: Forces Un-cached Network Lookups
def active_prober():
    global SHUTDOWN_FLAG

    print("[INIT] Launching isolated uncached HSFETCH network channel...")

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Prober network pipeline connection dropped: {e}")
        return

    print("[PROBER] Active tracking loop initialized for endpoints.")

    #ONIONS_TO_QUERY = TESTING_ONIONS

    while not SHUTDOWN_FLAG:

        try:
            
            ONIONS_TO_QUERY = OWNED_STATIC_ONIONS + THIRD_PARTY_ONIONS + [f"{uid}.onion" for uid in active_ephemerals]

            for onion_id in ONIONS_TO_QUERY:

                if SHUTDOWN_FLAG: break

                raw_onion = onion_id.replace(".onion", "")
                print(f"[DEBUG-PROBER] To probe onion service: {raw_onion}")

                try:
                    # Use raw HSFETCH via control port to clear memory dependencies and force network checks
                    # This causes Tor to locate and interact with the remote service's current HSDirs
                    
                    # 1. Standard dynamic ring fetch
                    conn.msg(f"HSFETCH {raw_onion}")
                    
                    # 2. Targeted bypass strategy: Directly poll known directories to dodge the local cache
                    with state_lock:
                        known_directories = [fp for fp, meta in tracked_target_hsdirs.items() if meta["associated_onion"] == onion_id]

                    for fp in known_directories:
                        try:
                            conn.msg(f"HSFETCH {raw_onion} SERVER={fp}")
                        except Exception:
                            pass

                except Exception as e:
                    # Captures cases where the command cannot execute locally
                    save_json_log("prober_errors.json", {
                        "timestamp": datetime.datetime.now().isoformat(),
                        "onion": onion_id,
                        "error": str(e)
                    })

                time.sleep(int(random.uniform(5, 15))) # Slight spacer between unique domain fetches

        except Exception as e:
            print(f"[PROBER ERROR] Main discovery loop exception: {e}")

        # Wait for the next monitoring block interval safely
        for _ in range(PROBE_INTERVAL):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

# 3.Consensus Thread: Tracks network-wide HSDir churn, IP ranges, and stability
def consensus_monitor():
    global SHUTDOWN_FLAG, tracked_target_hsdirs
    
    print("[INIT] Launching network status consensus auditor...")

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Consensus channel connection dropped: {e}")
        return

    for _ in range(int(180)):
        if SHUTDOWN_FLAG: break
        time.sleep(1)

    last_hour = None

    print("[CONSENSUS] Consensus tracking engine started.")
    
    while not SHUTDOWN_FLAG:

        try:

            # Dynamic Time Calculation to find remaining window until XX:05 UTC
            now = datetime.datetime.now(datetime.UTC)

            if last_hour == None:

                last_hour = now.hour

            if now.minute >= 5:
                next_sync = (now + datetime.timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
            else:
                next_sync = now.replace(minute=5, second=0, microsecond=0)
            
            time_until_sync = (next_sync - now).total_seconds()
            
            # If 15 mins (900s) or less remain, sleep precisely until the publication pass
            if 0 < time_until_sync <= 900:
                print(f"[MONITOR] Within the 15-minute generation window ({time_until_sync:.1f}s remaining). Commencing deep alignment sleep...")
                for _ in range(int(time_until_sync)):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)

            with state_lock:
                target_fingerprints = list(tracked_target_hsdirs.keys())

            print(f"[DEBUG-CONSENSUS] Tracked targets: {len(target_fingerprints)}")

            if target_fingerprints:
                print(f"[AUDITOR] Executing localized stability audit on {len(target_fingerprints)} target HSDirs...")
                audited_nodes = []

                for fingerprint in target_fingerprints:
                    try:
                        node = conn.get_network_status(fingerprint)

                        # Fetch full server descriptors to query Sybil identity markers
                        server_desc = conn.get_microdescriptor(fingerprint, default=None)
                        print(f"[DEBUG-CONSENSUS] TEST-FAMILY&CONTACT with descriptor for fp @ {fingerprint}:\n {server_desc}")
                        declared_family = getattr(server_desc, 'family', []) if server_desc else []
                        # Process structural node families clean listing for offline JSON formatting
                        family_list = list(declared_family) if isinstance(declared_family, (set, list)) else []
                        family_list = [entry.lstrip('$') for entry in family_list]
                        contact_string = get_relay_contact_info(conn, fingerprint, node, now) 

                        if "HSDir" in node.flags:

                            with state_lock:
                                tracked_target_hsdirs[fingerprint]["consecutive_hourly_absences"] = 0

                            audited_nodes.append({
                                "fingerprint": fingerprint,
                                "nickname": node.nickname,
                                "ip_address": node.address,
                                "or_port": node.or_port,
                                "flags": node.flags,
                                "published": node.published.isoformat() if node.published else None,
                                "contact_info": contact_string,       # Structural Sybil Flag Vector
                                "declared_family": family_list,       # Structural Sybil Flag Vector
                                "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"],
                                "service_type": tracked_target_hsdirs[fingerprint]["service_type"],
                                "first_discovery": tracked_target_hsdirs[fingerprint]["first_discovery"],
                                "last_timestamp": tracked_target_hsdirs[fingerprint]["last_timestamp"],
                                "descriptor_id_b64": tracked_target_hsdirs[fingerprint]["descriptor_id_b64"],
                                "hsdir_index_hrt": tracked_target_hsdirs[fingerprint]["hsdir_index_hrt"],
                                "ring_distance": tracked_target_hsdirs[fingerprint].get("ring_distance"),
                                "ring_position": tracked_target_hsdirs[fingerprint].get("ring_position"),
                                "status": "ACTIVE_IN_RING"
                            })

                        else:
                            # Node exists in consensus payload but was stripped of active capabilities
                            
                            if last_hour < now.hour:
                                last_hour = now.hour
                                with state_lock:
                                    tracked_target_hsdirs[fingerprint]["consecutive_hourly_absences"] += 1
                                    current_absences = tracked_target_hsdirs[fingerprint]["consecutive_hourly_absences"]
                                    if tracked_target_hsdirs[fingerprint]["consecutive_hourly_absences"] >= 3:
                                        # Mark it as dead in the logs later popped.
                                        tracked_target_hsdirs[fingerprint]["last_known_action"] = "CLASSIFIED_DEAD_OFFLINE"
                                        audited_nodes.append({
                                            "fingerprint": fingerprint,
                                            "nickname": node.nickname if node.nickname else None,
                                            "ip_address": node.address if node.address else None,
                                            "or_port": node.or_port if node.or_port else None,
                                            "flags": node.flags if node.flags else None,
                                            "published": node.published.isoformat() if node.published else None,
                                            "contact_info": contact_string,       # Structural Sybil Flag Vector
                                            "declared_family": family_list,       # Structural Sybil Flag Vector
                                            "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"],
                                            "service_type": tracked_target_hsdirs[fingerprint]["service_type"],
                                            "first_discovery": tracked_target_hsdirs[fingerprint]["first_discovery"],
                                            "last_timestamp": tracked_target_hsdirs[fingerprint]["last_timestamp"],
                                            "descriptor_id_b64": tracked_target_hsdirs[fingerprint]["descriptor_id_b64"],
                                            "hsdir_index_hrt": tracked_target_hsdirs[fingerprint]["hsdir_index_hrt"],
                                            "ring_distance": tracked_target_hsdirs[fingerprint].get("ring_distance"),
                                            "ring_position": tracked_target_hsdirs[fingerprint].get("ring_position"),
                                            "timestamp_dropped": datetime.datetime.now().isoformat(),
                                            "status": "DEAD_OFFLINE_CHURN"
                                        })

                                    else:
                                        audited_nodes.append({
                                            "fingerprint": fingerprint,
                                            "nickname": node.nickname if node.nickname else None,
                                            "ip_address": node.address if node.address else None,
                                            "or_port": node.or_port if node.or_port else None,
                                            "flags": node.flags if node.flags else None,
                                            "published": node.published.isoformat() if node.published else None,
                                            "contact_info": contact_string,       # Structural Sybil Flag Vector
                                            "declared_family": family_list,       # Structural Sybil Flag Vector
                                            "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"],
                                            "service_type": tracked_target_hsdirs[fingerprint]["service_type"],
                                            "first_discovery": tracked_target_hsdirs[fingerprint]["first_discovery"],
                                            "last_timestamp": tracked_target_hsdirs[fingerprint]["last_timestamp"],
                                            "descriptor_id_b64": tracked_target_hsdirs[fingerprint]["descriptor_id_b64"],
                                            "hsdir_index_hrt": tracked_target_hsdirs[fingerprint]["hsdir_index_hrt"],
                                            "ring_distance": tracked_target_hsdirs[fingerprint].get("ring_distance"),
                                            "ring_position": tracked_target_hsdirs[fingerprint].get("ring_position"),
                                            "timestamp_dropped": None,
                                            "status": "STRIPPED_HSDIR_FLAG"
                                        })

                            else:
                                audited_nodes.append({
                                    "fingerprint": fingerprint,
                                    "nickname": node.nickname if node.nickname else None,
                                    "ip_address": node.address if node.address else None,
                                    "or_port": node.or_port if node.or_port else None,
                                    "flags": node.flags if node.flags else None,
                                    "published": node.published.isoformat() if node.published else None,
                                    "contact_info": contact_string,       # Structural Sybil Flag Vector
                                    "declared_family": family_list,       # Structural Sybil Flag Vector
                                    "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"],
                                    "service_type": tracked_target_hsdirs[fingerprint]["service_type"],
                                    "first_discovery": tracked_target_hsdirs[fingerprint]["first_discovery"],
                                    "last_timestamp": tracked_target_hsdirs[fingerprint]["last_timestamp"],
                                    "descriptor_id_b64": tracked_target_hsdirs[fingerprint]["descriptor_id_b64"],
                                    "hsdir_index_hrt": tracked_target_hsdirs[fingerprint]["hsdir_index_hrt"],
                                    "ring_distance": tracked_target_hsdirs[fingerprint].get("ring_distance"),
                                    "ring_position": tracked_target_hsdirs[fingerprint].get("ring_position"),
                                    "timestamp_dropped": None,
                                    "status": "STRIPPED_HSDIR_FLAG"
                                })
                    
                    except Exception:
                        # Node has fallen out of the active consensus entirely (Churn Event)
                        if last_hour < now.hour:
                            last_hour = now.hour
                            with state_lock:
                                tracked_target_hsdirs[fingerprint]["consecutive_hourly_absences"] += 1
                                current_absences = tracked_target_hsdirs[fingerprint]["consecutive_hourly_absences"]
                                if tracked_target_hsdirs[fingerprint]["consecutive_hourly_absences"] >= 3:
                                    # Mark it as dead in the logs later popped.
                                    tracked_target_hsdirs[fingerprint]["last_known_action"] = "CLASSIFIED_DEAD_OFFLINE"
                                    audited_nodes.append({
                                        "fingerprint": fingerprint,
                                        "nickname": node.nickname if node.nickname else None,
                                        "ip_address": node.address if node.address else None,
                                        "or_port": node.or_port if node.or_port else None,
                                        "flags": node.flags if node.flags else None,
                                        "published": node.published.isoformat() if node.published else None,
                                        "contact_info": contact_string,       # Structural Sybil Flag Vector
                                        "declared_family": family_list,       # Structural Sybil Flag Vector
                                        "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"],
                                        "service_type": tracked_target_hsdirs[fingerprint]["service_type"],
                                        "first_discovery": tracked_target_hsdirs[fingerprint]["first_discovery"],
                                        "last_timestamp": tracked_target_hsdirs[fingerprint]["last_timestamp"],
                                        "descriptor_id_b64": tracked_target_hsdirs[fingerprint]["descriptor_id_b64"],
                                        "hsdir_index_hrt": tracked_target_hsdirs[fingerprint]["hsdir_index_hrt"],
                                        "ring_distance": tracked_target_hsdirs[fingerprint].get("ring_distance"),
                                        "ring_position": tracked_target_hsdirs[fingerprint].get("ring_position"),
                                        "timestamp_dropped": datetime.datetime.now().isoformat(),
                                        "status": "DEAD_OFFLINE_CHURN"
                                    })

                                else:
                                    audited_nodes.append({
                                        "fingerprint": fingerprint,
                                        "nickname": node.nickname if node.nickname else None,
                                        "ip_address": node.address if node.address else None,
                                        "or_port": node.or_port if node.or_port else None,
                                        "flags": node.flags if node.flags else None,
                                        "published": node.published.isoformat() if node.published else None,
                                        "contact_info": contact_string,       # Structural Sybil Flag Vector
                                        "declared_family": family_list,       # Structural Sybil Flag Vector
                                        "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"],
                                        "service_type": tracked_target_hsdirs[fingerprint]["service_type"],
                                        "first_discovery": tracked_target_hsdirs[fingerprint]["first_discovery"],
                                        "last_timestamp": tracked_target_hsdirs[fingerprint]["last_timestamp"],
                                        "descriptor_id_b64": tracked_target_hsdirs[fingerprint]["descriptor_id_b64"],
                                        "hsdir_index_hrt": tracked_target_hsdirs[fingerprint]["hsdir_index_hrt"],
                                        "ring_distance": tracked_target_hsdirs[fingerprint].get("ring_distance"),
                                        "ring_position": tracked_target_hsdirs[fingerprint].get("ring_position"),
                                        "timestamp_dropped": None,
                                        "status": "OFFLINE_CHURN"
                                    })

                        else:
                            audited_nodes.append({
                                "fingerprint": fingerprint,
                                "nickname": node.nickname if node.nickname else None,
                                "ip_address": node.address if node.address else None,
                                "or_port": node.or_port if node.or_port else None,
                                "flags": node.flags if node.flags else None,
                                "published": node.published.isoformat() if node.published else None,
                                "contact_info": contact_string,       # Structural Sybil Flag Vector
                                "declared_family": family_list,       # Structural Sybil Flag Vector
                                "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"],
                                "service_type": tracked_target_hsdirs[fingerprint]["service_type"],
                                "first_discovery": tracked_target_hsdirs[fingerprint]["first_discovery"],
                                "last_timestamp": tracked_target_hsdirs[fingerprint]["last_timestamp"],
                                "descriptor_id_b64": tracked_target_hsdirs[fingerprint]["descriptor_id_b64"],
                                "hsdir_index_hrt": tracked_target_hsdirs[fingerprint]["hsdir_index_hrt"],
                                "ring_distance": tracked_target_hsdirs[fingerprint].get("ring_distance"),
                                "ring_position": tracked_target_hsdirs[fingerprint].get("ring_position"),
                                "timestamp_dropped": None,
                                "status": "OFFLINE_CHURN"
                            })

                save_json_log("hsdir_consensus_snapshots.json", {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "total_active_hsdirs": len(audited_nodes),
                    "relays": audited_nodes
                })

                print(f"[AUDITOR] Audit completed. Logs committed to targeted_hsdir_consensus.json.")            

                # Check for historical nodes crossing the 3-hour definitive offline threshold
                with state_lock:
                    if tracked_target_hsdirs[fingerprint]["consecutive_hourly_absences"] >= 3:
                        tracked_target_hsdirs.pop(fingerprint)

            else:
                # Prevent silent gaps: Log a baseline verification record if no nodes are discovered yet
                save_json_log("targeted_hsdir_consensus.json", {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "active_targets_count": 0,
                    "status": "AWAITING_FIRST_DESCRIPTOR_EVENT"
                })
                print("[AUDITOR] Baseline snapshot logged. Awaiting discovery vectors.")   

        except Exception as e:
            print(f"[AUDITOR ERROR] Main monitoring thread loop exception: {e}")
            traceback.print_exc()

        # Passive wait tracking interval cleanly
        for _ in range(CONSENSUS_INTERVAL):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

# 4. Scale-Optimized Loop: Drives 50 Ephemeral Onions Simultaneously = Scales out 800 HR reference points
def scaled_ephemeral_manager():
    global SHUTDOWN_FLAG, active_ephemerals

    print("[INIT] Launching ephemeral cryptographic generator...")
    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Ephemeral controller link dropped: {e}")
        return

    while not SHUTDOWN_FLAG:
        try:
            # Step A: Clean up any old ephemerals safely
            if active_ephemerals:
                print(f"[SCALE] Clearing {len(active_ephemerals)} expired ephemeral profiles...")
                for onion_id in list(active_ephemerals):
                    try:
                        conn.remove_ephemeral_hidden_service(onion_id)
                    except Exception:
                        pass
                active_ephemerals.clear()
            
            # Step B: Bulk instantiate 50 fresh services across the ring spectrum
            print(f"[SCALE] Spawning {NUM_EPHEMERALS} concurrent v3 ephemeral targets...")
            for i in range(NUM_EPHEMERALS):
                if SHUTDOWN_FLAG: break
                creation_delay = int(random.uniform(5,10))
                for _ in range(int(creation_delay)):
                    if SHUTDOWN_FLAG: 
                        break
                    time.sleep(1)
                try:
                    # await_publication=False avoids blocking execution threads
                    response = conn.create_ephemeral_hidden_service({80: 8130 + i}, key_content = 'ED25519-V3', await_publication=False)
                    active_ephemerals.add(response.service_id)
                except Exception as e:
                    print(f"[ERROR - BULK SPAWN ALLOCATION SLOT {i}]: {e}")
            
            print(f"[SCALE] Bulk deployment complete. {len(active_ephemerals)} active monitoring targets.")
            
        except Exception as e:
            print(f"[ERROR - ENGINE LOOP]: {e}")
            traceback.print_exc()
            
        # Hold positions open to collect behavioral metrics before changing targets
        for _ in range(ROTATION_INTERVAL):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

    # Emergency Final Exit Cleanup
    print("[CLEANUP] Finalizing framework teardown...")
    for onion_id in list(active_ephemerals):
        try: conn.remove_ephemeral_hidden_service(onion_id)
        except: pass

def main():
    print("=" * 60)
    print("""
         _   _ ____  ____  _       
        | | | / ___||  _ \(_)_ __     
        | |_| \___ \| | | | | '__|   
        |  _  |___) | |_| | | |     
        |_| |_|____/|____/|_|_|
         ________ _       _
        |  ______| |_ ___| |__   ___ _ __                 
        | |_ / _ | __/ __| '_ \ / _ | '__|                
        |  _|  __| || (__| | | |  __| |                   
        |_|  \___|\__\___|_| |_|\___|_|                   
    """)
    print("=" * 60)
    
    try:
        test_conn = get_authenticated_controller()
        print(f"[INIT] Successfully validated control port {CONTROL_PORT} access and credentials.")
        test_conn.close()
    except Exception as e:
        print(f"[FATAL SETUP ERROR] Cannot establish connection to Tor daemon over port 9051: {e}")
        print("Please check that your torrc contains an active 'ControlPort 9051' directive.")
        sys.exit(1)

    threads = [
        threading.Thread(target=run_event_listener, daemon=True),
        threading.Thread(target=consensus_monitor, daemon=True),
        threading.Thread(target=scaled_ephemeral_manager, daemon=True),
        threading.Thread(target=active_prober, daemon=True)
    ]
    
    for t in threads:
        t.start()
    
    while not SHUTDOWN_FLAG:
        try: time.sleep(1)
        except KeyboardInterrupt: break
        
    print("[CLEANUP] Halting execution, writing state files and detaching listeners.")
    print("[INFO] Waiting for threads to finish current tasks...")
    for t in threads:
        t.join()
        
    print("[INFO] All threads stopped. Exiting cleanly.")
    sys.exit(0) 

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main()
