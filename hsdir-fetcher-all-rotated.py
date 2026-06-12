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

# Stem library required dependencies
from stem.control import Controller, EventType
from stem.descriptor.remote import DescriptorDownloader

# Configuration
CONTROL_PORT = 9051              # Tor's Control Port
NUM_EPHEMERALS = 50              # Scale of ephemeral onion to open
ROTATION_INTERVAL = 86400        # 24-hour interval before sweeping and creating fresh ephemerals
#CONSENSUS_INTERVAL = 600        # 30-min interval for tracking consensus snapshots
PROBE_INTERVAL = 1200            # 20-min interval between active probes

# Collection of onion services to query
with open('target_onions/owned_onions.json', 'r', encoding='utf-8') as f: OWNED_STATIC_ONIONS = json.load(f)
with open('target_onions/third_party_onions.json', 'r', encoding='utf-8') as f: THIRD_PARTY_ONIONS = json.load(f)
with open('target_onions/testing_onions.json', 'r', encoding='utf-8') as f: TEST_ONIONS = json.load(f)
# Collection of file to create thread-safety locks (bottleneck issues)
ACCEPTED_LOG_FILES = ["hsdir_contact_history.json", "hsdir_ring_events.json", "prober_errors.json", "hsdir_consensus_snapshots.json", "rotated_hsdir_consensus_snapshots.json"]

# Global condition and default log directory
SHUTDOWN_FLAG = False
DATA_DIR = "/mnt/second-drive/hsdir_research_data"
os.makedirs(DATA_DIR, exist_ok=True)

# Dictionary of thread-safety locks (bottleneck issues)
file_write_locks = {}

for _ in ACCEPTED_LOG_FILES:
    file_write_locks[_] = threading.Lock()

# Collection of thread-data specific locks (deadlock-starvation issues) 
hsdir_state_lock = threading.Lock()
ephemeral_lock = threading.Lock()
contact_cache_lock = threading.Lock()

# Global state data collector for active tracking
active_ephemerals = set()
tracked_target_hsdirs = {}  # Format: { fingerprint: {"onion_address": x, "first_seen": x} }
hsdir_contact_cache = {}    # Format: { fingerprint: {"contact": str, "last_fetched": datetime} }
rotated_hsdirs = {}
state_updates = {} # Consensus monitor only

# Remote directory authority downloader reference (contact info)
downloader = DescriptorDownloader(timeout=15)

def save_json_log(filename, data):
    try:
        target_file_lock = file_write_locks[filename] 
        with target_file_lock:
            filepath = os.path.join(DATA_DIR, filename)
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data) + ",\n")
    except Exception as e:
        print(f"[LOG EXCEPTION] Failed writing entry to {filename}: {e}")
        traceback.print_exc()

def get_authenticated_controller():
    # Isolated socket connection (prevent control channel deadlocks)
    controller = Controller.from_port(port=CONTROL_PORT)
    controller.authenticate()
    return controller

def run_event_listener():
    print("[INIT] Launching dedicated event registration pipe...")
    for _ in range(60):
        if SHUTDOWN_FLAG: break
        time.sleep(1)
    try:
        conn = get_authenticated_controller()
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
    # Compute clockwise distance between an HSDir's ring hash index and an
    # onion descriptor target index on Tor's 256-bit circular integer space.
    # Formula: Delta = (HSDir_Index - Onion_Index) mod 2^256

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

def rotated_hsdir_monitor():

    global rotated_hsdirs

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Consensus channel for rotated HSDirs connection dropped: {e}")
        return

    last_hour = datetime.datetime.now(datetime.UTC).hour
    rotated_statuses = {}

    while not SHUTDOWN_FLAG:

        try:

            now = datetime.datetime.now(datetime.UTC)
            next_sync = (now + datetime.timedelta(hours=1)).replace(minute=58, second=59, microsecond=0)      
            time_until_sync = (next_sync - now).total_seconds()
            
            if 0 < time_until_sync:
                for _ in range(int(time_until_sync)):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)

            timestamp = (now + datetime.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

            if now.hour == 23:
                if tracked_target_hsdirs:
                    with hsdir_state_lock:
                        new_rotated_hsdirs = dict(tracked_target_hsdirs)
                        rotated_statuses.update(dict(state_updates))
                    for fp in new_rotated_hsdirs.keys():
                        if fp not in rotated_hsdirs.keys():
                            rotated_hsdirs[fp] = new_rotated_hsdirs[fp]
                            rotated_hsdirs[fp]["rotated_at"] = timestamp

                # Timer of 3-min to properly populate with fingerprints after rotation
                for _ in range(360):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)

            else:
                for _ in range(360):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)

            rotated = dict(rotated_hsdirs)

            if rotated:
                print(f"[ROTATOR] Executing localized stability audit on {len(rotated)} target HSDirs...")

                audited_nodes = []

                hour_changed = (last_hour != timestamp.hour)

                for fingerprint, meta in rotated_hsdirs.items():
                    if SHUTDOWN_FLAG: break
                    if rotated_statuses.get(fingerprint) is None:
                        rotated_statuses[fingerprint] = {"consecutive_hourly_absences": 0}
                    try:
                        node = conn.get_network_status(fingerprint)
                        # Fetch server full and micro descriptors to query Sybil identity markers (effective family and contact info)
                        server_desc = conn.get_microdescriptor(fingerprint, default=None)
                        declared_family = getattr(server_desc, 'family', None) if server_desc else None
                        family_list = [entry.lstrip('$') for entry in (list(declared_family) if isinstance(declared_family, (set, list)) else None)]
                        declared_family_id = getattr(server_desc, 'family_ids', None)

                        if not declared_family_id and server_desc:
                            for line in server_desc.get_unrecognized_lines():
                                if line.startswith("family-ids "):
                                    # Split off the token prefix and capture all space-separated IDs
                                    declared_family_id = line.strip().split()[1:]
                                    break

                        if not declared_family_id and server_desc:
                            try:
                                for line in server_desc.get_text().splitlines():
                                    if line.startswith("family-ids "):
                                        declared_family_id = line.strip().split()[1:]
                                        break
                            except Exception:
                                pass

                        if declared_family_id:
                            declared_family_id = declared_family_id[0].replace("ed25519:", "")

                        if not declared_family_id:
                            declared_family_id = None

                        contact_string = get_relay_contact_info(conn, fingerprint, node, timestamp) 

                        if "HSDir" in node.flags:

                            rotated_statuses[fingerprint] = {"consecutive_hourly_absences": 0}
                            fp_status = "ACTIVE_IN_RING"
                            consider_dropped = None

                        else:
                            # Node exists but was stripped of active capabilities
                            if hour_changed:
                                current_absences = rotated_statuses[fingerprint]["consecutive_hourly_absences"] + 1
                                rotated_statuses[fingerprint] = {"consecutive_hourly_absences": current_absences}
                                
                            if rotated_statuses[fingerprint]["consecutive_hourly_absences"] >= 2:
                                fp_status = "DEAD_OFFLINE_CHURN"
                                consider_dropped = timestamp
                            
                            else:
                                fp_status = "STRIPPED_HSDIR_FLAG"
                                consider_dropped = None

                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname if node.nickname else None,
                            "ip_address": node.address if node.address else None,
                            "or_port": node.or_port if node.or_port else None,
                            "flags": node.flags if node.flags else None,
                            "published": node.published.isoformat() if node.published else None,
                            "contact_info": contact_string,                                             # Structural Sybil Flag Vector
                            "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                            "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                            "last_known_onion": meta["associated_onion"],
                            "rotated_at": meta["rotated_at"],
                            "service_type": meta["service_type"],
                            "first_discovery": meta["first_discovery"],
                            "last_timestamp": meta["last_timestamp"],
                            "descriptor_id_b64": meta["descriptor_id_b64"],
                            "hsdir_index_hrt": meta["hsdir_index_hrt"],
                            "ring_distance": meta["ring_distance"],
                            "ring_position": meta["ring_position"],
                            "consecutive_hourly_absences": rotated_statuses[fingerprint]["consecutive_hourly_absences"],
                            "timestamp_dropped": consider_dropped,
                            "status": fp_status
                        })
                    
                    except Exception:
                        # Node has fallen out of the active consensus entirely (churn event)
                        if hour_changed:
                            current_absences = rotated_statuses[fingerprint]["consecutive_hourly_absences"] + 1
                            rotated_statuses[fingerprint] = {"consecutive_hourly_absences": current_absences}
                            
                        if rotated_statuses[fingerprint]["consecutive_hourly_absences"] >= 2:
                            fp_status = "DEAD_OFFLINE_CHURN"
                            consider_dropped = timestamp
                    
                        else:
                            fp_status = "OFFLINE_CHURN"
                            consider_dropped = None

                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname if node.nickname else None,
                            "ip_address": node.address if node.address else None,
                            "or_port": node.or_port if node.or_port else None,
                            "flags": node.flags if node.flags else None,
                            "published": node.published.isoformat() if node.published else None,
                            "contact_info": contact_string,                                             # Structural Sybil Flag Vector
                            "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                            "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                            "last_known_onion": meta["associated_onion"],
                            "rotated_at": meta["rotated_at"],
                            "service_type": meta["service_type"],
                            "first_discovery": meta["first_discovery"],
                            "last_timestamp": meta["last_timestamp"],
                            "descriptor_id_b64": meta["descriptor_id_b64"],
                            "hsdir_index_hrt": meta["hsdir_index_hrt"],
                            "ring_distance": meta["ring_distance"],
                            "ring_position": meta["ring_position"],
                            "consecutive_hourly_absences": rotated_statuses[fingerprint]["consecutive_hourly_absences"],
                            "timestamp_dropped": consider_dropped,
                            "status": fp_status
                        })

                    time.sleep(random.uniform(0.2,1.2))

                save_json_log("rotated_hsdir_consensus_snapshots.json", {
                    "timestamp": timestamp,
                    "total_rotated_hsdirs": len(audited_nodes),
                    "relays": audited_nodes
                })

                print(f"[ROTATOR] Audit completed. Logs committed to rotated_hsdir_consensus_snapshots.json.")

                if hour_changed:
                    last_hour = timestamp.hour
        
        except Exception as e:
            print(f"[ROTATOR ERROR] Main monitoring thread loop exception: {e}")
            traceback.print_exc()

    conn.close()

def get_relay_contact_info(conn, fingerprint, node, utc_time):
    # Remote lookahead for contact information of a fingerprint
    global hsdir_contact_cache

    with contact_cache_lock:
        cache_entry = hsdir_contact_cache.get(fingerprint)

    needs_remote_fetch = False
    contact_string = "None Specified"

    if not cache_entry:
        needs_remote_fetch = True
    else:
        contact_string = cache_entry["contact"]

    if needs_remote_fetch:
        try:
            # Query the remote directory authority mirror directly
            query = downloader.get_server_descriptors(fingerprints=[fingerprint])
            remote_contact = "None Specified"
            
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
            
            change_entry = {
                "timestamp": datetime.datetime.now().isoformat(),
                "fingerprint": fingerprint,
                "nickname": node.nickname if node.nickname else None,
                "ip_address": node.address if node.address else None,
                "contact_info": remote_contact
            }

            save_json_log("hsdir_contact_history.json", change_entry)
            
            contact_string = remote_contact
            
            with contact_cache_lock:
                hsdir_contact_cache[fingerprint] = {
                    "contact": remote_contact,
                    "last_fetched": utc_time
                }

        except Exception as remote_err:
            # Fallback
            if cache_entry:
                contact_string = cache_entry["contact"]
                
            else:
                contact_string = "Fetch Failed (Authority Timeout)"
    
    return contact_string

def hs_desc_event_listener(event):
    # Asynchronous Event Monitor: Get hash ring positions and targets
    global tracked_target_hsdirs, active_ephemerals

    if SHUTDOWN_FLAG: return

    try:
        
        onion_id = getattr(event, 'address', None)
        
        if not onion_id:
            return

        full_onion_address = f"{onion_id}.onion"

        with ephemeral_lock:
            is_ephemeral = onion_id in active_ephemerals

        is_tracked = (full_onion_address in OWNED_STATIC_ONIONS or 
                      full_onion_address in THIRD_PARTY_ONIONS or 
                      is_ephemeral)

        if not is_tracked:
            return
        
        # Onion type classification
        if full_onion_address in OWNED_STATIC_ONIONS:
            service_type = "STATIC_CONTROL"
        elif is_ephemeral:
            service_type = "EPHEMERAL_VARIABLE"
        elif full_onion_address in THIRD_PARTY_ONIONS:
            service_type = "THIRD_PARTY_PROBE"
        else:
            return

        raw_event_text = " ".join(list(event)) if isinstance(event, (list, tuple)) else str(event)

        # Extract HSDir fingerprint
        hsdir_fp = getattr(event, 'hsdir', None)
        if not hsdir_fp:
            fp_match = re.search(r'\$([0-9A-Fa-f]{40})', raw_event_text)
            if fp_match:
                hsdir_fp = fp_match.group(1).upper()

        if not hsdir_fp:
            return

        # Extract Descriptor ID (Blinded Public Key)
        descriptor_id = None
        desc_match = re.search(r'\$[0-9A-Fa-f]{40}(?:~[^\s]+)?\s+([A-Za-z0-9+/]{43})(?:\s+|$)', raw_event_text)
        if desc_match:
            descriptor_id = desc_match.group(1)

        # Extract replica index (if any)
        replica = getattr(event, 'replica', None)
        if replica is None:
            rep_match = re.search(r'REPLICA=(\d+)', raw_event_text, re.IGNORECASE)
            if rep_match:
                replica = int(rep_match.group(1))

        # Extract HSDIR_INDEX field
        hsdir_index_hrt = getattr(event, 'hsdir_index', None)
        if not hsdir_index_hrt:
            idx_match = re.search(r'HSDIR_INDEX=([0-9A-Fa-f]{64})', raw_event_text, re.IGNORECASE)
            if idx_match:
                hsdir_index_hrt = idx_match.group(1).upper()

        # Extract reason string (in FAILED actions)
        reason = getattr(event, 'reason', None)
        if not reason:
            reason_match = re.search(r'REASON=(\w+)', raw_event_text, re.IGNORECASE)
            if reason_match:
                reason = reason_match.group(1)

        # Descriptor ID conversion to 256-bit hex layout (onion_target_index)
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

        with hsdir_state_lock:
            if hsdir_fp not in tracked_target_hsdirs:
                tracked_target_hsdirs[hsdir_fp] = {
                    "associated_onion": f"{full_onion_address}",
                    "service_type": service_type,
                    "first_discovery": actual_timestamp,
                    "last_timestamp": actual_timestamp,
                    "last_known_action": event.action,
                    "descriptor_id_b64": descriptor_id,
                    "onion_target_index_hex": onion_target_index_hex,
                    "hsdir_index_hrt": hsdir_index_hrt, # Position on the Hash Ring Table
                    "ring_distance": ring_distance,
                    "ring_position": ring_position,
                }
            else:
                tracked_target_hsdirs[hsdir_fp]["associated_onion"] = full_onion_address
                tracked_target_hsdirs[hsdir_fp]["service_type"] = service_type
                tracked_target_hsdirs[hsdir_fp]["last_known_action"] = event.action
                tracked_target_hsdirs[hsdir_fp]["last_timestamp"] = actual_timestamp
                tracked_target_hsdirs[hsdir_fp]["descriptor_id_b64"] = descriptor_id if descriptor_id else tracked_target_hsdirs[hsdir_fp]["descriptor_id_b64"]
                tracked_target_hsdirs[hsdir_fp]["onion_target_index_hex"] = onion_target_index_hex if onion_target_index_hex else tracked_target_hsdirs[hsdir_fp]["onion_target_index_hex"]
                tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt"] = hsdir_index_hrt if hsdir_index_hrt else tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt"]
                tracked_target_hsdirs[hsdir_fp]["ring_distance"] = ring_distance if ring_distance else tracked_target_hsdirs[hsdir_fp]["ring_distance"]
                tracked_target_hsdirs[hsdir_fp]["ring_position"] = ring_position if ring_position else tracked_target_hsdirs[hsdir_fp]["ring_position"]

        if event.action not in {"FAILED", "RECEIVED", "UPLOADED"}:
            return

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

        save_json_log("hsdir_ring_events.json", log_entry)

    except Exception as e:
        print(f"[EXC-EVENT-LISTENER] Error parsing incoming stream frame: {e}")
        traceback.print_exc()

def active_prober():
    # Active onion prober
    global SHUTDOWN_FLAG

    print("[INIT] Launching isolated uncached HSFETCH network channel...")

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Prober network pipeline connection dropped: {e}")
        return

    print("[PROBER] Active tracking loop initialized for endpoints.")

    for _ in range(60):
        if SHUTDOWN_FLAG: break
        time.sleep(1)

    while not SHUTDOWN_FLAG:

        try:

            with ephemeral_lock:
                current_ephemerals = [f"{uid}.onion" for uid in active_ephemerals]
            
            #ONIONS_TO_QUERY = OWNED_STATIC_ONIONS + THIRD_PARTY_ONIONS + [f"{uid}.onion" for uid in active_ephemerals]
            ONIONS_TO_QUERY = TEST_ONIONS

            for onion_id in ONIONS_TO_QUERY:

                if SHUTDOWN_FLAG: break

                raw_onion = onion_id.replace(".onion", "")

                try:
                    # Use raw HSFETCH to clear memory dependencies and force un-cached network checks
                    
                    # Standard dynamic ring fetch
                    conn.msg(f"HSFETCH {raw_onion}")
                    
                    # Direct poll from known directories
                    with hsdir_state_lock:
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

                time.sleep(random.uniform(5.0, 15.0)) # Spacer between unique domain fetches

        except Exception as e:
            print(f"[PROBER ERROR] Main discovery loop exception: {e}")

        # Wait for the next monitoring block interval
        for _ in range(PROBE_INTERVAL):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

def consensus_monitor():
    # Consensus Monitor: Tracks known network HSDir churn, IP ranges, and stability
    global SHUTDOWN_FLAG, tracked_target_hsdirs, hsdir_contact_cache, state_updates
    
    print("[INIT] Launching network status consensus auditor...")

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Consensus channel connection dropped: {e}")
        return

    last_hour = datetime.datetime.now(datetime.UTC).hour

    print("[CONSENSUS] Consensus tracking engine started.")
    
    while not SHUTDOWN_FLAG:

        try:

            # Time calculation to find the remaining window until XX:05 UTC
            now = datetime.datetime.now(datetime.UTC)
            next_sync = (now + datetime.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            time_until_sync = (next_sync - now).total_seconds()
            
            if 0 < time_until_sync:
                for _ in range(int(time_until_sync)):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)

            now = datetime.datetime.now(datetime.UTC)
            timestamp = datetime.datetime.now().isoformat()

            # Refresh + rotated status HSDirs
            if now.hour == 0:
                if tracked_target_hsdirs:
                    with hsdir_state_lock:
                        tracked_target_hsdirs = {}
                        state_updates = {}

                # Timer of 3-min to properly populate with fingerprints after rotation
                for _ in range(300):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)

            else:
                for _ in range(300):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)

            with hsdir_state_lock:
                target_fingerprints = dict(tracked_target_hsdirs)

            if target_fingerprints:
                print(f"[AUDITOR] Executing localized stability audit on {len(target_fingerprints)} target HSDirs...")
                audited_nodes = []

                hour_changed = (last_hour != now.hour)

                for fingerprint, meta in target_fingerprints.items():
                    if SHUTDOWN_FLAG: break
                    if state_updates.get(fingerprint) is None:
                        state_updates[fingerprint] = {"consecutive_hourly_absences": 0}
                    try:
                        node = conn.get_network_status(fingerprint)
                        # Fetch server full and micro descriptors to query Sybil identity markers (effective family and contact info)
                        server_desc = conn.get_microdescriptor(fingerprint, default=None)
                        declared_family = getattr(server_desc, 'family', None) if server_desc else None
                        family_list = [entry.lstrip('$') for entry in (list(declared_family) if isinstance(declared_family, (set, list)) else None)]
                        declared_family_id = getattr(server_desc, 'family_ids', None)

                        if not declared_family_id and server_desc:
                            for line in server_desc.get_unrecognized_lines():
                                if line.startswith("family-ids "):
                                    # Split off the token prefix and capture all space-separated IDs
                                    declared_family_id = line.strip().split()[1:]
                                    break

                        if not declared_family_id and server_desc:
                            try:
                                for line in server_desc.get_text().splitlines():
                                    if line.startswith("family-ids "):
                                        declared_family_id = line.strip().split()[1:]
                                        break
                            except Exception:
                                pass

                        if declared_family_id:
                            declared_family_id = declared_family_id[0].replace("ed25519:", "")

                        if not declared_family_id:
                            declared_family_id = None

                        contact_string = get_relay_contact_info(conn, fingerprint, node, now) 

                        if "HSDir" in node.flags:

                            state_updates[fingerprint] = {"consecutive_hourly_absences": 0}
                            fp_status = "ACTIVE_IN_RING"
                            consider_dropped = None

                        else:
                            # Node exists but was stripped of active capabilities
                            if hour_changed:
                                current_absences = state_updates[fingerprint]["consecutive_hourly_absences"] + 1
                                state_updates[fingerprint] = {"consecutive_hourly_absences": current_absences}
                                
                            if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                                fp_status = "DEAD_OFFLINE_CHURN"
                                consider_dropped = timestamp
                                with hsdir_state_lock:
                                    tracked_target_hsdirs.pop(fingerprint)
                                    state_updates.pop(fingerprint)
                                    with contact_cache_lock:
                                        hsdir_contact_cache.pop(fingerprint)
                            else:
                                fp_status = "STRIPPED_HSDIR_FLAG"
                                consider_dropped = None

                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname if node.nickname else None,
                            "ip_address": node.address if node.address else None,
                            "or_port": node.or_port if node.or_port else None,
                            "flags": node.flags if node.flags else None,
                            "published": node.published.isoformat() if node.published else None,
                            "contact_info": contact_string,                                             # Structural Sybil Flag Vector
                            "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                            "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                            "mapped_onion": meta["associated_onion"],
                            "service_type": meta["service_type"],
                            "first_discovery": meta["first_discovery"],
                            "last_timestamp": meta["last_timestamp"],
                            "descriptor_id_b64": meta["descriptor_id_b64"],
                            "hsdir_index_hrt": meta["hsdir_index_hrt"],
                            "ring_distance": meta["ring_distance"],
                            "ring_position": meta["ring_position"],
                            "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                            "timestamp_dropped": consider_dropped,
                            "status": fp_status
                        })
                    
                    except Exception:
                        # Node has fallen out of the active consensus entirely (churn event)
                        if hour_changed:
                            current_absences = state_updates[fingerprint]["consecutive_hourly_absences"] + 1
                            state_updates[fingerprint] = {"consecutive_hourly_absences": current_absences}
                            
                        if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                            fp_status = "DEAD_OFFLINE_CHURN"
                            consider_dropped = timestamp
                            with hsdir_state_lock:
                                tracked_target_hsdirs.pop(fingerprint)
                                state_updates.pop(fingerprint)
                                with contact_cache_lock:
                                    hsdir_contact_cache.pop(fingerprint)
                        else:
                            fp_status = "OFFLINE_CHURN"
                            consider_dropped = None

                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname if node.nickname else None,
                            "ip_address": node.address if node.address else None,
                            "or_port": node.or_port if node.or_port else None,
                            "flags": node.flags if node.flags else None,
                            "published": node.published.isoformat() if node.published else None,
                            "contact_info": contact_string,                                             # Structural Sybil Flag Vector
                            "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                            "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                            "mapped_onion": meta["associated_onion"],
                            "service_type": meta["service_type"],
                            "first_discovery": meta["first_discovery"],
                            "last_timestamp": meta["last_timestamp"],
                            "descriptor_id_b64": meta["descriptor_id_b64"],
                            "hsdir_index_hrt": meta["hsdir_index_hrt"],
                            "ring_distance": meta["ring_distance"],
                            "ring_position": meta["ring_position"],
                            "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                            "timestamp_dropped": consider_dropped,
                            "status": fp_status
                        })

                    time.sleep(random.uniform(0.2,1.2))

                save_json_log("hsdir_consensus_snapshots.json", {
                    "timestamp": timestamp,
                    "total_active_hsdirs": len(audited_nodes),
                    "relays": audited_nodes
                })

                print(f"[AUDITOR] Audit completed. Logs committed to hsdir_consensus_snapshots.json.")

                if hour_changed:
                    last_hour = now.hour            

            else:
                save_json_log("hsdir_consensus_snapshots.json", {
                    "timestamp": timestamp,
                    "active_targets_count": 0,
                    "status": "AWAITING_FIRST_DESCRIPTOR_EVENT"
                })
                print("[AUDITOR] Baseline snapshot logged. Awaiting discovery vectors.")   

        except Exception as e:
            print(f"[AUDITOR ERROR] Main monitoring thread loop exception: {e}")
            traceback.print_exc()

    conn.close()

def scaled_ephemeral_manager():
    # Scaled ephemeral onions manager: Drives 50 Ephemeral Onions
    global SHUTDOWN_FLAG, active_ephemerals

    print("[INIT] Launching ephemeral cryptographic generator...")
    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Ephemeral controller link dropped: {e}")
        return

    for _ in range(60):
        if SHUTDOWN_FLAG: break
        time.sleep(1)

    while not SHUTDOWN_FLAG:
        try:
            # Remove any old ephemerals
            with ephemeral_lock:
                expired_onions = list(active_ephemerals)
            
            if expired_onions:
                print(f"[SCALE] Clearing {len(expired_onions)} expired ephemeral profiles...")
                for onion_id in expired_onions:
                    try: 
                        conn.remove_ephemeral_hidden_service(onion_id)
                    except Exception: 
                        pass
                with ephemeral_lock:
                    active_ephemerals.clear()
            
            # Instantiate 50 fresh services
            print(f"[SCALE] Spawning {NUM_EPHEMERALS} concurrent v3 ephemeral targets...")
            for i in range(NUM_EPHEMERALS):
                if SHUTDOWN_FLAG: break
                try:
                    response = conn.create_ephemeral_hidden_service({80: 8130 + i}, key_content = 'ED25519-V3', await_publication=False)
                    with ephemeral_lock:
                        active_ephemerals.add(response.service_id)
                except Exception as e:
                    print(f"[ERROR - BULK SPAWN ALLOCATION SLOT {i}]: {e}")

                time.sleep(random.uniform(4.5,9.5))
            
            print(f"[SCALE] Deployment complete. {len(active_ephemerals)} active monitoring targets.")
            
        except Exception as e:
            print(f"[ERROR - ENGINE LOOP]: {e}")
            traceback.print_exc()
            
        base_rotation_window = ROTATION_INTERVAL + int(random.uniform(-280, 280))
        for _ in range(base_rotation_window):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

    print("[CLEANUP] Finalizing framework teardown...")
    for onion_id in list(active_ephemerals):
        try: conn.remove_ephemeral_hidden_service(onion_id)
        except: pass
    
    conn.close()

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
        print(f"[INIT] Validated control port {CONTROL_PORT} access.")
        test_conn.close()
    except Exception as e:
        print(f"[FATAL ERROR] Cannot establish connection to Tor daemon over port 9051: {e}")
        traceback.print_exc()
        sys.exit(1)

    started = datetime.datetime.now().isoformat()

    threads = [
        threading.Thread(target=run_event_listener, daemon=True),
        threading.Thread(target=consensus_monitor, daemon=True),
        threading.Thread(target=scaled_ephemeral_manager, daemon=True),
        threading.Thread(target=rotated_hsdir_monitor, daemon=True),
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

    ended = datetime.datetime.now().isoformat()

    uptime = {
        "started": started,
        "ended": ended
    }

    filepath = os.path.join(DATA_DIR, "program_execution_period.json")
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(json.dumps(uptime) + ",\n")
        
    print("[INFO] All threads stopped. Exiting cleanly.")
    sys.exit(0) 

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main()
