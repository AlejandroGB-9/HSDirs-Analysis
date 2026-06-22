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
import math
import hashlib
import struct

# Stem library required dependencies
from stem.control import Controller, EventType

# Configuration
CONTROL_PORT = 9051              # Tor's Control Port
NUM_EPHEMERALS = 50              # Scale of ephemeral onion to open
PROBE_INTERVAL = 1200            # 20-min interval between active probes

# Synchronization windows
WINDOW_START = datetime.time(23, 55, 0)     # 23:55 UTC
WINDOW_END = datetime.time(0, 7, 0)         # 00:07 UTC
EPHEMERAL_START_MIN = 1                     # 00:01 UTC
PROBER_START_MIN = 8                        # 00:08 UTC
CONSENSUS_START_MIN = 10                    # 00:10 UTC
RING_SIZE = 2**256

# Collection of onion services to query
with open('target_onions/owned_onions.json', 'r', encoding='utf-8') as f: OWNED_STATIC_ONIONS = json.load(f)
with open('target_onions/third_party_onions.json', 'r', encoding='utf-8') as f: THIRD_PARTY_ONIONS = json.load(f)
# Collection of files to create thread-safety locks (bottleneck issues)
ACCEPTED_LOG_FILES = ["hsdir_ring_events.jsonl", "prober_errors.jsonl", "network_hsdirs_consensus_snapshots.jsonl", "tracked_hsdir_consensus_snapshots.jsonl", "rotated_tracked_hsdir_consensus_snapshots.jsonl", "rotated_network_hsdir_consensus_snapshots.jsonl"]

# Global condition and default log directory
SHUTDOWN_FLAG = False
DATA_DIR = "/mnt/second-drive/hsdir_research_data"
os.makedirs(DATA_DIR, exist_ok=True)

# Dictionary of thread-safety locks (bottleneck issues)
file_write_locks = {}

for _ in ACCEPTED_LOG_FILES:
    file_write_locks[_] = threading.Lock()

# Collection of thread-data specific locks (deadlock-starvation issues)
network_wide_lock = threading.Lock()
hsdir_state_lock = threading.Lock()
ephemeral_lock = threading.Lock()

# Global state data collector for active tracking
active_ephemerals = set()
tracked_target_hsdirs = {}  # Format: { fingerprint: {"onion_address": x, "first_seen": x} }
network_wide_hsdirs = {}

def save_json_log(filename, data):
    try:
        target_file_lock = file_write_locks[filename] 
        with target_file_lock:
            filepath = os.path.join(DATA_DIR, filename)
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data) + "\n")
    except Exception as e:
        print(f"[LOG EXCEPTION] Failed writing entry to {filename}: {e}")
        traceback.print_exc()

def get_authenticated_controller():
    # Isolated socket connection (prevent control channel deadlocks)
    controller = Controller.from_port(port=CONTROL_PORT)
    controller.authenticate()
    return controller

def shuffle_container(data):
    if isinstance(data, list):
        return random.sample(data, len(data))
    
    elif isinstance(data, dict):
        dict_items = list(data.items())
        random.shuffle(dict_items)
        return dict(dict_items)
    
    else:
        raise TypeError("This function only accepts lists or dictionaries.")

def is_inside_window():
    now_utc = datetime.datetime.now(datetime.UTC).time()
    if WINDOW_START <= WINDOW_END:
        return WINDOW_START <= now_utc <= WINDOW_END
    else:  # Crosses midnight boundary
        return now_utc >= WINDOW_START or now_utc <= WINDOW_END

def until_next_cycle(hour, minute):
    global SHUTDOWN_FLAG
    now = datetime.datetime.now(datetime.UTC)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += datetime.timedelta(days=1)
    
    time_to_wait = (target - now).total_seconds()
    if time_to_wait > 0:
        for _ in range(math.ceil(time_to_wait)):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

def run_event_listener():
    until_next_cycle(0, PROBER_START_MIN)
    print("[INIT] Launching dedicated event registration pipe...")
    if SHUTDOWN_FLAG: return
    while not SHUTDOWN_FLAG:
        try:
            # The context manager automatically calls conn.close() when exiting this block
            with get_authenticated_controller() as conn:
                conn.add_event_listener(hs_desc_event_listener, EventType.HS_DESC)
                print("[INFO] Event listener successfully registered.")
                
                while not SHUTDOWN_FLAG and conn.is_alive():
                    time.sleep(1)
                    
                if not SHUTDOWN_FLAG:
                    print("[WARNING] Control port connection lost. Reconnecting...")
                    
        except Exception as e:
            print(f"[CRITICAL] Event listener connection channel failed: {e}")
        
        # Interruptible backoff cooldown
        for _ in range(10):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

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
            return None, None
        hsdir_val = int(hsdir_index_hex, 16)
        onion_val = int(onion_target_index_hex, 16)
        distance = (hsdir_val - onion_val) % RING_SIZE
        return distance, round((distance / RING_SIZE) * 100, 2)
    except Exception:
        return None, None

def rotated_hsdir_monitor(rotated_hsdirs, state_updates, hour_changed, timestamp, target_file):

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Consensus channel connection dropped: {e}")
        return

    try:
        consensus_text = conn.get_info("dir/status-vote/current/consensus-microdesc")
        
        # Extract current SRV line
        srv_line = [l for l in consensus_text.splitlines() if l.startswith("shared-rand-current-value")]
        current_srv_b64 = srv_line[0].split()[2] if srv_line else None
        
        if current_srv_b64:
            padded_srv = current_srv_b64 + "=" * ((4 - len(current_srv_b64) % 4) % 4)
            current_srv_bytes = base64.b64decode(padded_srv)
        else:
            current_srv_bytes = None
            
        # Calculate current Tor v3 Time Period
        current_time_period = int(time.time()) // 86400
        
    except Exception as net_err:
        print(f"[ERROR] Could not gather network consensus headers: {net_err}")
        current_srv_bytes = None
        current_time_period = None

    if rotated_hsdirs:
        rotated_hsdirs = shuffle_container(rotated_hsdirs)
        audited_nodes = []
        for fingerprint, meta in rotated_hsdirs.items():
            if SHUTDOWN_FLAG: break
            if fingerprint not in state_updates:
                state_updates[fingerprint] = {"consecutive_hourly_absences": 0}
            node = None
            declared_family_id = None
            family_list = None
            hsdir_index_hrt = None
            try:
                node = conn.get_network_status(fingerprint)
                # Fetch server full and micro descriptors to query Sybil identity markers (effective family info)
                server_desc = conn.get_microdescriptor(fingerprint, default=None)
                declared_family = getattr(server_desc, 'family', None) if server_desc else []
                family_list = [entry.lstrip('$') for entry in (list(declared_family) if isinstance(declared_family, (set, list)) else [])]
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

                if server_desc:
                    # Ed25519 master key                       
                    ed25519_b64 = None
                    if hasattr(server_desc, 'ed25519_identity') and server_desc.ed25519_identity:
                        ed25519_b64 = server_desc.ed25519_identity
                    else:
                        for line in str(server_desc).splitlines():
                            if line.startswith("id ed25519 "):
                                ed25519_b64 = line.split()[2]
                                break
                    
                    # HRT position calculation
                    if ed25519_b64 and current_srv_bytes and current_time_period:
                        try:
                            padded_id = ed25519_b64 + "=" * ((4 - len(ed25519_b64) % 4) % 4)
                            ed25519_bytes = base64.b64decode(padded_id)
                            tp_packed = struct.pack(">Q", current_time_period)
                            node_idx_msg = b"node-idx" + ed25519_bytes + tp_packed + current_srv_bytes
                            hsdir_index_bytes = hashlib.sha3_256(node_idx_msg).digest()
                            hsdir_index_hrt = int.from_bytes(hsdir_index_bytes, byteorder='big')
                        except Exception:
                            hsdir_index_hrt = None

                if "HSDir" in node.flags:

                    state_updates[fingerprint] = {"consecutive_hourly_absences": 0}
                    fp_status = "ACTIVE_IN_RING"
                    consider_dropped = None

                else:
                    # Node exists but was stripped of active capabilities
                    if hour_changed:
                        state_updates[fingerprint]["consecutive_hourly_absences"] += 1
                        
                    if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                        fp_status = "DEAD_OFFLINE_CHURN"
                        consider_dropped = timestamp
                    
                    else:
                        fp_status = "STRIPPED_HSDIR_FLAG"
                        consider_dropped = None

                audited_nodes.append({
                    "fingerprint": fingerprint,
                    "nickname": node.nickname if node else None,
                    "ip_address": node.address if node else None,
                    "or_port": node.or_port if node else None,
                    "flags": node.flags if node else None,
                    "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                    "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                    "last_known_onion": meta["associated_onion"] if meta["associated_onion"] else None,
                    "last_known_onion_index_hex": meta["onion_target_index_hex"] if meta["onion_target_index_hex"] else None,
                    "last_known_onion_index_100": meta["onion_target_index_100"] if meta["onion_target_index_100"] else None,
                    "service_type": meta["service_type"] if meta["service_type"] else None,
                    "first_discovery": meta["first_discovery"],
                    "last_timestamp": meta["last_timestamp"],
                    "descriptor_id_b64": meta["descriptor_id_b64"] if meta["descriptor_id_b64"] else None,
                    "hsdir_index_hrt_hex": format(hsdir_index_hrt,'X') if hsdir_index_hrt else None,
                    "hsdir_index_hrt_100": round((hsdir_index_hrt/RING_SIZE)*100,2) if hsdir_index_hrt else None,
                    "hsdir_to_onion_ring_distance": meta["hsdir_to_onion_ring_distance"] if meta["hsdir_to_onion_ring_distance"] else None,
                    "hsdir_to_onion_ring_position": meta["hsdir_to_onion_ring_position"] if meta["hsdir_to_onion_ring_position"] else None,
                    "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                    "timestamp_dropped": consider_dropped,
                    "status": fp_status
                })
            
            except Exception:
                # Node has fallen out of the active consensus entirely (churn event)
                if hour_changed:
                    state_updates[fingerprint]["consecutive_hourly_absences"] += 1
                    
                if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                    fp_status = "DEAD_OFFLINE_CHURN"
                    consider_dropped = timestamp
            
                else:
                    fp_status = "OFFLINE_CHURN"
                    consider_dropped = None

                audited_nodes.append({
                    "fingerprint": fingerprint,
                    "nickname": node.nickname if node else None,
                    "ip_address": node.address if node else None,
                    "or_port": node.or_port if node else None,
                    "flags": node.flags if node else None,
                    "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                    "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                    "last_known_onion": meta["associated_onion"] if meta["associated_onion"] else None,
                    "last_known_onion_index_hex": meta["onion_target_index_hex"] if meta["onion_target_index_hex"] else None,
                    "last_known_onion_index_100": meta["onion_target_index_100"] if meta["onion_target_index_100"] else None,
                    "service_type": meta["service_type"] if meta["service_type"] else None,
                    "first_discovery": meta["first_discovery"],
                    "last_timestamp": meta["last_timestamp"],
                    "descriptor_id_b64": meta["descriptor_id_b64"] if meta["descriptor_id_b64"] else None,
                    "hsdir_index_hrt_hex": meta["hsdir_index_hrt_hex"],
                    "hsdir_index_hrt_100": meta["hsdir_index_hrt_100"],
                    "hsdir_to_onion_ring_distance": meta["hsdir_to_onion_ring_distance"] if meta["hsdir_to_onion_ring_distance"] else None,
                    "hsdir_to_onion_ring_position": meta["hsdir_to_onion_ring_position"] if meta["hsdir_to_onion_ring_position"] else None,
                    "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                    "timestamp_dropped": consider_dropped,
                    "status": fp_status
                })

            time.sleep(random.uniform(0.2,0,5))

        save_json_log(target_file, {
            "timestamp": timestamp,
            "total_rotated_hsdirs": len(audited_nodes),
            "relays": audited_nodes
        })

    conn.close()

def hs_desc_event_listener(event):
    # Asynchronous Event Monitor: Get hash ring positions and targets
    global tracked_target_hsdirs, active_ephemerals

    if SHUTDOWN_FLAG or is_inside_window():
        return

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

        actual_timestamp = datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0, tzinfo=None).isoformat(timespec="minutes")

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
                    "onion_target_index_100": round((int(onion_target_index_hex, 16)/RING_SIZE)*100,2) if onion_target_index_hex else None,
                    "hsdir_index_hrt_hex": hsdir_index_hrt, # Position on the Hash Ring Table
                    "hsdir_index_hrt_100": round((int(hsdir_index_hrt, 16)/RING_SIZE)*100,2) if hsdir_index_hrt else None,
                    "hsdir_to_onion_ring_distance": ring_distance,
                    "hsdir_to_onion_ring_position": ring_position,
                }
            else:
                tracked_target_hsdirs[hsdir_fp]["associated_onion"] = full_onion_address
                tracked_target_hsdirs[hsdir_fp]["service_type"] = service_type
                tracked_target_hsdirs[hsdir_fp]["last_known_action"] = event.action
                tracked_target_hsdirs[hsdir_fp]["last_timestamp"] = actual_timestamp
                tracked_target_hsdirs[hsdir_fp]["descriptor_id_b64"] = descriptor_id if descriptor_id else tracked_target_hsdirs[hsdir_fp]["descriptor_id_b64"]
                tracked_target_hsdirs[hsdir_fp]["onion_target_index_hex"] = onion_target_index_hex if onion_target_index_hex else tracked_target_hsdirs[hsdir_fp]["onion_target_index_hex"]
                tracked_target_hsdirs[hsdir_fp]["onion_target_index_100"] = round((int(onion_target_index_hex, 16)/RING_SIZE)*100,2) if onion_target_index_hex else tracked_target_hsdirs[hsdir_fp]["onion_target_index_100"]
                tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt_hex"] = hsdir_index_hrt if hsdir_index_hrt else tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt_hex"]
                tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt_100"] = round((int(hsdir_index_hrt, 16)/RING_SIZE)*100,2) if hsdir_index_hrt else tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt_100"]
                tracked_target_hsdirs[hsdir_fp]["hsdir_to_onion_ring_distance"] = ring_distance if ring_distance else tracked_target_hsdirs[hsdir_fp]["hsdir_to_onion_ring_distance"]
                tracked_target_hsdirs[hsdir_fp]["hsdir_to_onion_ring_position"] = ring_position if ring_position else tracked_target_hsdirs[hsdir_fp]["hsdir_to_onion_ring_position"]

            desc_id = tracked_target_hsdirs[hsdir_fp]["descriptor_id_b64"]
            target_idx_hex = tracked_target_hsdirs[hsdir_fp]["onion_target_index_hex"]
            target_idx_100 = tracked_target_hsdirs[hsdir_fp]["onion_target_index_100"]
            hrt_hex = tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt_hex"]
            hrt_100 = tracked_target_hsdirs[hsdir_fp]["hsdir_index_hrt_100"]
            dist = tracked_target_hsdirs[hsdir_fp]["hsdir_to_onion_ring_distance"]
            pos = tracked_target_hsdirs[hsdir_fp]["hsdir_to_onion_ring_position"]

        if event.action not in {"FAILED", "RECEIVED", "UPLOADED"}:
            return

        log_entry = {
            "timestamp": actual_timestamp,
            "service_type": service_type,
            "onion_address": full_onion_address,
            "action": event.action,
            "hsdir_fingerprint": hsdir_fp,
            "descriptor_id_b64": desc_id,
            "onion_target_index_hex": target_idx_hex,
            "onion_target_index_100": target_idx_100,
            "hsdir_index_hrt_hex": hrt_hex, # Position on the Hash Ring Table
            "hsdir_index_hrt_100": hrt_100,
            "hsdir_to_onion_ring_distance": dist,
            "hsdir_to_onion_ring_position": pos,
            "reason": reason
        }

        save_json_log("hsdir_ring_events.jsonl", log_entry)

    except Exception as e:
        print(f"[EXC-EVENT-LISTENER] Error parsing incoming stream frame: {e}")
        traceback.print_exc()

def active_prober():
    # Active onion prober
    global SHUTDOWN_FLAG, active_ephemerals, tracked_target_hsdirs

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Prober network pipeline connection dropped: {e}")
        return

    until_next_cycle(0, PROBER_START_MIN)

    print("[INIT] Launching isolated uncached HSFETCH network channel...")
    print("[PROBER] Active tracking loop initialized for endpoints.")

    targets_to_query = {}

    while not SHUTDOWN_FLAG:

        if is_inside_window():
            until_next_cycle(0, PROBER_START_MIN)
            if SHUTDOWN_FLAG: break

        try:

            with ephemeral_lock:
                current_ephemerals = [f"{uid}.onion" for uid in active_ephemerals]
            
            ONIONS_TO_QUERY = OWNED_STATIC_ONIONS + THIRD_PARTY_ONIONS + current_ephemerals
            ONIONS_TO_QUERY = shuffle_container(ONIONS_TO_QUERY)

            if tracked_target_hsdirs:
                with hsdir_state_lock:
                    targets_to_query = dict(tracked_target_hsdirs)

            for onion_id in ONIONS_TO_QUERY:

                if SHUTDOWN_FLAG or is_inside_window():
                    break

                raw_onion = onion_id.replace(".onion", "")

                try:
                    # Use raw HSFETCH to clear memory dependencies and force un-cached network checks
                    
                    # Standard dynamic ring fetch
                    conn.msg(f"HSFETCH {raw_onion}")
                    
                    # Direct poll from known directories

                    if targets_to_query:
                    
                        known_directories = [fp for fp, meta in targets_to_query.items() if meta["associated_onion"] == onion_id]

                        for fp in known_directories:
                            if SHUTDOWN_FLAG or is_inside_window(): break
                            try:
                                conn.msg(f"HSFETCH {raw_onion} SERVER={fp}")
                            except Exception:
                                pass
                            time.sleep(random.uniform(2.0, 4.0))

                except Exception as e:
                    # Captures cases where the command cannot execute locally
                    save_json_log("prober_errors.jsonl", {
                        "timestamp": datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0, tzinfo=None).isoformat(timespec="minutes"),
                        "onion": onion_id,
                        "error": str(e)
                    })

                time.sleep(random.uniform(5.0, 13.0)) # Spacer between unique domain fetches

        except Exception as e:
            print(f"[PROBER ERROR] Main discovery loop exception: {e}")

        # Wait for the next monitoring block interval
        for _ in range(PROBE_INTERVAL):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

        print("[PROBER] Reiterating and probing target onions.")
    
    conn.close()

def network_consensus():
    global SHUTDOWN_FLAG, network_wide_hsdirs

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Consensus channel connection dropped: {e}")
        return

    until_next_cycle(23, 57)

    print("[INIT] Launching network status consensus auditor...")

    last_hour = datetime.datetime.now(datetime.UTC).hour
    state_updates = {}

    print("[CONSENSUS] Consensus tracking engine started.")

    while not SHUTDOWN_FLAG:

        try:
            # Time calculation to find the remaining window until XX:05 UTC
            now = datetime.datetime.now(datetime.UTC)
            next_sync = (now + datetime.timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
            time_until_sync = (next_sync - now).total_seconds()
            
            if 0 < time_until_sync:
                for _ in range(math.ceil(time_until_sync)):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)
                
            if SHUTDOWN_FLAG: break

            now = datetime.datetime.now(datetime.UTC)
            timestamp = datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0, tzinfo=None).isoformat(timespec="minutes")

            # Refresh + rotated status HSDirs
            if now.hour == 0:
                print("[AUDITOR] 00:05 UTC - Processing historical HSDirs states...")
                if network_wide_hsdirs:
                    with network_wide_lock:
                        rotated_hsdirs = dict(network_wide_hsdirs)
                        rotated_updates = dict(state_updates)
                        network_wide_hsdirs.clear()
                        state_updates.clear()

                    hour_changed = (last_hour != now.hour)
                    rotated_hsdir_monitor(rotated_hsdirs, rotated_updates, hour_changed, timestamp, "rotated_network_hsdir_consensus_snapshots.jsonl")
                        
                minute_check = datetime.datetime.now(datetime.UTC).minute
                if minute_check >= CONSENSUS_START_MIN:
                    for _ in range(180):
                        if SHUTDOWN_FLAG: break
                        time.sleep(1)
                # Time window to populate with fingerprints after rotation
                else:
                    until_next_cycle(0, CONSENSUS_START_MIN)
                
                last_hour = 0
                continue

            print(f"[AUDITOR-WIDE] Executing localized stability audit on network-wide HSDirs...")

            try:
                consensus_text = conn.get_info("dir/status-vote/current/consensus-microdesc")
                
                # Extract current SRV line
                srv_line = [l for l in consensus_text.splitlines() if l.startswith("shared-rand-current-value")]
                current_srv_b64 = srv_line[0].split()[2] if srv_line else None
                
                if current_srv_b64:
                    padded_srv = current_srv_b64 + "=" * ((4 - len(current_srv_b64) % 4) % 4)
                    current_srv_bytes = base64.b64decode(padded_srv)
                else:
                    current_srv_bytes = None
                    
                # Calculate current Tor v3 Time Period
                current_time_period = int(time.time()) // 86400
                
            except Exception as net_err:
                print(f"[ERROR] Could not gather network consensus headers: {net_err}")
                current_srv_bytes = None
                current_time_period = None
            
            audited_nodes = []
            hour_changed = (last_hour != now.hour)
            audited_list = []

            try:
                for node in conn.get_network_statuses():
                    if SHUTDOWN_FLAG: break
                    fingerprint = None
                    declared_family_id = None
                    family_list = None
                    hsdir_index_hrt = None
                    if "HSDir" in node.flags:
                        fingerprint = node.fingerprint
                        if fingerprint not in state_updates:
                            state_updates[fingerprint] = {"consecutive_hourly_absences": 0}
                        server_desc = conn.get_microdescriptor(fingerprint, default=None)
                        declared_family = getattr(server_desc, 'family', None) if server_desc else []
                        family_list = [entry.lstrip('$') for entry in (list(declared_family) if isinstance(declared_family, (set, list)) else [])]
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

                        if server_desc:
                            # Ed25519 master key                       
                            ed25519_b64 = None
                            if hasattr(server_desc, 'ed25519_identity') and server_desc.ed25519_identity:
                                ed25519_b64 = server_desc.ed25519_identity
                            else:
                                for line in str(server_desc).splitlines():
                                    if line.startswith("id ed25519 "):
                                        ed25519_b64 = line.split()[2]
                                        break
                            
                            # HRT position calculation
                            if ed25519_b64 and current_srv_bytes and current_time_period:
                                try:
                                    padded_id = ed25519_b64 + "=" * ((4 - len(ed25519_b64) % 4) % 4)
                                    ed25519_bytes = base64.b64decode(padded_id)
                                    tp_packed = struct.pack(">Q", current_time_period)
                                    node_idx_msg = b"node-idx" + ed25519_bytes + tp_packed + current_srv_bytes
                                    hsdir_index_bytes = hashlib.sha3_256(node_idx_msg).digest()
                                    hsdir_index_hrt = int.from_bytes(hsdir_index_bytes, byteorder='big')
                                except Exception:
                                    hsdir_index_hrt = None

                        fp_status = "ACTIVE_IN_RING"
                        consider_dropped = None
                        state_updates[fingerprint] = {"consecutive_hourly_absences": 0}

                        if fingerprint:
                            with network_wide_lock:
                                if fingerprint not in network_wide_hsdirs:
                                    network_wide_hsdirs[fingerprint] = {
                                        "first_discovery": timestamp,
                                        "last_timestamp": timestamp,
                                        "hsdir_index_hrt_hex": format(hsdir_index_hrt,'X') if hsdir_index_hrt else None,
                                        "hsdir_index_hrt_100": round((hsdir_index_hrt/RING_SIZE)*100,2) if hsdir_index_hrt else None
                                    }
                                    f_discovery = network_wide_hsdirs[fingerprint]["first_discovery"]
                                    l_timestamp = network_wide_hsdirs[fingerprint]["last_timestamp"]
                                    hsdir_hrt_hex = network_wide_hsdirs[fingerprint]["hsdir_index_hrt_hex"]
                                    hsdir_hrt_100 = network_wide_hsdirs[fingerprint]["hsdir_index_hrt_100"]
                                else:
                                    network_wide_hsdirs[fingerprint]["last_timestamp"] = timestamp
                                    network_wide_hsdirs[fingerprint]["hsdir_index_hrt_hex"] = format(hsdir_index_hrt,'X') if hsdir_index_hrt else None
                                    network_wide_hsdirs[fingerprint]["hsdir_index_hrt_100"] = round((hsdir_index_hrt/RING_SIZE)*100,2) if hsdir_index_hrt else None
                                    f_discovery = network_wide_hsdirs[fingerprint]["first_discovery"]
                                    l_timestamp = network_wide_hsdirs[fingerprint]["last_timestamp"]
                                    hsdir_hrt_hex = network_wide_hsdirs[fingerprint]["hsdir_index_hrt_hex"]
                                    hsdir_hrt_100 = network_wide_hsdirs[fingerprint]["hsdir_index_hrt_100"]

                            audited_list.append(fingerprint)

                            audited_nodes.append({
                                "fingerprint": fingerprint,
                                "nickname": node.nickname if node else None,
                                "ip_address": node.address if node else None,
                                "or_port": node.or_port if node else None,
                                "flags": node.flags if node else None,
                                "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                                "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                                "first_discovery": f_discovery,
                                "last_timestamp": l_timestamp,
                                "hsdir_index_hrt_hex": hsdir_hrt_hex,
                                "hsdir_index_hrt_100": hsdir_hrt_100,
                                "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                                "timestamp_dropped": consider_dropped,
                                "status": fp_status
                            })

                    time.sleep(random.uniform(0.2,0.7))

            except Exception as e:
                print(f"[AUDITOR ERROR] Main monitoring thread loop exception: {e}")
                traceback.print_exc()
            with network_wide_lock:
                wide_fingerprints = dict(network_wide_hsdirs)
                
            wide_fingerprints = shuffle_container(wide_fingerprints)

            for fingerprint, meta in wide_fingerprints.items():
                if SHUTDOWN_FLAG: break
                if fingerprint not in audited_list:
                    node = None
                    declared_family_id = None
                    family_list = None
                    try:
                        node = conn.get_network_status(fingerprint)
                        # Fetch server full and micro descriptors to query Sybil identity markers (effective family info)
                        server_desc = conn.get_microdescriptor(fingerprint, default=None)
                        declared_family = getattr(server_desc, 'family', None) if server_desc else []
                        family_list = [entry.lstrip('$') for entry in (list(declared_family) if isinstance(declared_family, (set, list)) else [])]
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

                        if server_desc:
                            # Ed25519 master key                       
                            ed25519_b64 = None
                            if hasattr(server_desc, 'ed25519_identity') and server_desc.ed25519_identity:
                                ed25519_b64 = server_desc.ed25519_identity
                            else:
                                for line in str(server_desc).splitlines():
                                    if line.startswith("id ed25519 "):
                                        ed25519_b64 = line.split()[2]
                                        break
                            
                            # HRT position calculation
                            if ed25519_b64 and current_srv_bytes and current_time_period:
                                try:
                                    padded_id = ed25519_b64 + "=" * ((4 - len(ed25519_b64) % 4) % 4)
                                    ed25519_bytes = base64.b64decode(padded_id)
                                    tp_packed = struct.pack(">Q", current_time_period)
                                    node_idx_msg = b"node-idx" + ed25519_bytes + tp_packed + current_srv_bytes
                                    hsdir_index_bytes = hashlib.sha3_256(node_idx_msg).digest()
                                    hsdir_index_hrt = int.from_bytes(hsdir_index_bytes, byteorder='big')
                                except Exception:
                                    hsdir_index_hrt = None

                        with network_wide_lock:
                            network_wide_hsdirs[fingerprint]["hsdir_index_hrt_hex"] = format(hsdir_index_hrt,'X') if hsdir_index_hrt else None
                            network_wide_hsdirs[fingerprint]["hsdir_index_hrt_100"] = round((hsdir_index_hrt/RING_SIZE)*100,2) if hsdir_index_hrt else None
                            hsdir_hrt_hex = network_wide_hsdirs[fingerprint]["hsdir_index_hrt_hex"]
                            hsdir_hrt_100 = network_wide_hsdirs[fingerprint]["hsdir_index_hrt_100"]

                        if "HSDir" in node.flags:
                            with network_wide_lock:
                                network_wide_hsdirs[fingerprint]["last_timestamp"] = timestamp
                            state_updates[fingerprint] = {"consecutive_hourly_absences": 0}
                            fp_status = "ACTIVE_IN_RING"
                            consider_dropped = None

                        else:
                            # Node exists but was stripped of active capabilities
                            if hour_changed:
                                state_updates[fingerprint]["consecutive_hourly_absences"] += 1
                                
                            if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                                fp_status = "DEAD_OFFLINE_CHURN"
                                consider_dropped = timestamp
                            else:
                                fp_status = "STRIPPED_HSDIR_FLAG"
                                consider_dropped = None

                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname if node else None,
                            "ip_address": node.address if node else None,
                            "or_port": node.or_port if node else None,
                            "flags": node.flags if node else None,
                            "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                            "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                            "first_discovery": meta["first_discovery"],
                            "last_timestamp": timestamp if node and "HSDir" in node.flags else meta["last_timestamp"],
                            "hsdir_index_hrt_hex": hsdir_hrt_hex,
                            "hsdir_index_hrt_100": hsdir_hrt_100,
                            "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                            "timestamp_dropped": consider_dropped,
                            "status": fp_status
                        })
                    
                    except Exception:
                        # Node has fallen out of the active consensus entirely (churn event)
                        if hour_changed:
                            state_updates[fingerprint]["consecutive_hourly_absences"] += 1
                            
                        if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                            fp_status = "DEAD_OFFLINE_CHURN"
                            consider_dropped = timestamp
                        else:
                            fp_status = "OFFLINE_CHURN"
                            consider_dropped = None

                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname if node else None,
                            "ip_address": node.address if node else None,
                            "or_port": node.or_port if node else None,
                            "flags": node.flags if node else None,
                            "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                            "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                            "first_discovery": meta["first_discovery"],
                            "last_timestamp": meta["last_timestamp"],
                            "hsdir_index_hrt_hex": meta["hsdir_index_hrt_hex"],
                            "hsdir_index_hrt_100": meta["hsdir_index_hrt_100"],
                            "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                            "timestamp_dropped": consider_dropped,
                            "status": fp_status
                        })

                    if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                        with network_wide_lock:
                            network_wide_hsdirs.pop(fingerprint, None)
                            state_updates.pop(fingerprint, None)

                    time.sleep(random.uniform(0.2,0.7))

            save_json_log("network_hsdirs_consensus_snapshots.jsonl", {
                "timestamp": timestamp,
                "total_active_hsdirs": len(audited_nodes),
                "relays": audited_nodes
            })

            print(f"[AUDITOR-WIDE] Audit completed. Logs committed to network_hsdirs_consensus_snapshots.jsonl.")

            if hour_changed:
                last_hour = now.hour  
                            
        except Exception as e:
            print(f"[AUDITOR ERROR] Main monitoring thread loop exception: {e}")
            traceback.print_exc()

    conn.close()

def consensus_monitor():
    # Consensus Monitor: Tracks known network HSDir churn, IP ranges, and stability
    global SHUTDOWN_FLAG, tracked_target_hsdirs

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Consensus channel connection dropped: {e}")
        return

    until_next_cycle(23, 57)

    print("[INIT] Launching network status consensus auditor...")

    last_hour = datetime.datetime.now(datetime.UTC).hour
    state_updates = {}

    print("[CONSENSUS] Consensus tracking engine started.")
    
    while not SHUTDOWN_FLAG:

        try:
            # Time calculation to find the remaining window until XX:05 UTC
            now = datetime.datetime.now(datetime.UTC)
            next_sync = (now + datetime.timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
            time_until_sync = (next_sync - now).total_seconds()
            
            if 0 < time_until_sync:
                for _ in range(math.ceil(time_until_sync)):
                    if SHUTDOWN_FLAG: break
                    time.sleep(1)
                
            if SHUTDOWN_FLAG: break

            now = datetime.datetime.now(datetime.UTC)
            timestamp = datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0, tzinfo=None).isoformat(timespec="minutes")

            # Refresh + rotated status HSDirs
            if now.hour == 0:
                print("[AUDITOR] 00:05 UTC - Processing historical HSDirs states...")
                if tracked_target_hsdirs:
                    with hsdir_state_lock:
                        rotated_hsdirs = dict(tracked_target_hsdirs)
                        rotated_updates = dict(state_updates)
                        tracked_target_hsdirs.clear()
                        state_updates.clear()

                    hour_changed = (last_hour != now.hour)
                    rotated_hsdir_monitor(rotated_hsdirs, rotated_updates, hour_changed, timestamp, "rotated_tracked_hsdir_consensus_snapshots.jsonl")
                        
                minute_check = datetime.datetime.now(datetime.UTC).minute
                if minute_check >= CONSENSUS_START_MIN:
                    for _ in range(180):
                        if SHUTDOWN_FLAG: break
                        time.sleep(1)
                # Time window to populate with fingerprints after rotation
                else:
                    until_next_cycle(0, CONSENSUS_START_MIN)
                
                last_hour = 0
                continue

            with hsdir_state_lock:
                target_fingerprints = dict(tracked_target_hsdirs)

            if target_fingerprints:
                target_fingerprints = shuffle_container(target_fingerprints)
                print(f"[AUDITOR] Executing localized stability audit on {len(target_fingerprints)} target HSDirs...")
                audited_nodes = []

                hour_changed = (last_hour != now.hour)

                for fingerprint, meta in target_fingerprints.items():
                    if SHUTDOWN_FLAG: break
                    if fingerprint not in state_updates:
                        state_updates[fingerprint] = {"consecutive_hourly_absences": 0}
                    node = None
                    declared_family_id = None
                    family_list = None
                    try:
                        node = conn.get_network_status(fingerprint)
                        # Fetch server full and micro descriptors to query Sybil identity markers (effective family info)
                        server_desc = conn.get_microdescriptor(fingerprint, default=None)
                        declared_family = getattr(server_desc, 'family', None) if server_desc else []
                        family_list = [entry.lstrip('$') for entry in (list(declared_family) if isinstance(declared_family, (set, list)) else [])]
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

                        if "HSDir" in node.flags:

                            state_updates[fingerprint] = {"consecutive_hourly_absences": 0}
                            fp_status = "ACTIVE_IN_RING"
                            consider_dropped = None

                        else:
                            # Node exists but was stripped of active capabilities
                            if hour_changed:
                                state_updates[fingerprint]["consecutive_hourly_absences"] += 1
                                
                            if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                                fp_status = "DEAD_OFFLINE_CHURN"
                                consider_dropped = timestamp
                            else:
                                fp_status = "STRIPPED_HSDIR_FLAG"
                                consider_dropped = None

                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname if node else None,
                            "ip_address": node.address if node else None,
                            "or_port": node.or_port if node else None,
                            "flags": node.flags if node else None,
                            "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                            "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                            "mapped_onion": meta["associated_onion"],
                            "mapped_onion_index_hex": meta["onion_target_index_hex"],
                            "mapped_onion_index_100": meta["onion_target_index_100"],
                            "service_type": meta["service_type"],
                            "first_discovery": meta["first_discovery"],
                            "last_timestamp": meta["last_timestamp"],
                            "descriptor_id_b64": meta["descriptor_id_b64"],
                            "hsdir_index_hrt_hex": meta["hsdir_index_hrt_hex"],
                            "hsdir_index_hrt_100": meta["hsdir_index_hrt_100"],
                            "hsdir_to_onion_ring_distance": meta["hsdir_to_onion_ring_distance"],
                            "hsdir_to_onion_ring_position": meta["hsdir_to_onion_ring_position"],
                            "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                            "timestamp_dropped": consider_dropped,
                            "status": fp_status
                        })
                    
                    except Exception:
                        # Node has fallen out of the active consensus entirely (churn event)
                        if hour_changed:
                            state_updates[fingerprint]["consecutive_hourly_absences"] += 1
                            
                        if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                            fp_status = "DEAD_OFFLINE_CHURN"
                            consider_dropped = timestamp
                        else:
                            fp_status = "OFFLINE_CHURN"
                            consider_dropped = None

                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname if node else None,
                            "ip_address": node.address if node else None,
                            "or_port": node.or_port if node else None,
                            "flags": node.flags if node else None,
                            "declared_family_id": declared_family_id if declared_family_id else None,   # Structural Sybil Flag Vector
                            "declared_family": family_list if family_list else None,                    # Structural Sybil Flag Vector
                            "mapped_onion": meta["associated_onion"],
                            "mapped_onion_index_hex": meta["onion_target_index_hex"],
                            "mapped_onion_index_100": meta["onion_target_index_100"],
                            "service_type": meta["service_type"],
                            "first_discovery": meta["first_discovery"],
                            "last_timestamp": meta["last_timestamp"],
                            "descriptor_id_b64": meta["descriptor_id_b64"],
                            "hsdir_index_hrt_hex": meta["hsdir_index_hrt_hex"],
                            "hsdir_index_hrt_100": meta["hsdir_index_hrt_100"],
                            "hsdir_to_onion_ring_distance": meta["hsdir_to_onion_ring_distance"],
                            "hsdir_to_onion_ring_position": meta["hsdir_to_onion_ring_position"],
                            "consecutive_hourly_absences": state_updates[fingerprint]["consecutive_hourly_absences"],
                            "timestamp_dropped": consider_dropped,
                            "status": fp_status
                        })

                    if state_updates[fingerprint]["consecutive_hourly_absences"] >= 2:
                        with hsdir_state_lock:
                            tracked_target_hsdirs.pop(fingerprint, None)
                            state_updates.pop(fingerprint, None)

                    time.sleep(random.uniform(1.2,2.2))

                save_json_log("tracked_hsdir_consensus_snapshots.jsonl", {
                    "timestamp": timestamp,
                    "total_active_hsdirs": len(audited_nodes),
                    "relays": audited_nodes
                })

                print(f"[AUDITOR] Audit completed. Logs committed to tracked_hsdir_consensus_snapshots.jsonl.")

                if hour_changed:
                    last_hour = now.hour            

            else:
                save_json_log("tracked_hsdir_consensus_snapshots.jsonl", {
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

    try:
        conn = get_authenticated_controller()
    except Exception as e:
        print(f"[CRITICAL] Ephemeral controller link dropped: {e}")
        return

    until_next_cycle(0, EPHEMERAL_START_MIN)

    print("[INIT] Launching ephemeral cryptographic generator...")

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

                time.sleep(random.uniform(4.5,8.5))
            
            print(f"[SCALE] Deployment complete. {len(active_ephemerals)} active monitoring targets.")
            
        except Exception as e:
            print(f"[ERROR - ENGINE LOOP]: {e}")
            traceback.print_exc()
            
        until_next_cycle(0, EPHEMERAL_START_MIN)

    print("[CLEANUP] Finalizing framework teardown...")
    with ephemeral_lock:
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

    started = datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0, tzinfo=None).isoformat(timespec="minutes")

    threads = [
        threading.Thread(target=run_event_listener, daemon=True),           # Event listener for target onions
        threading.Thread(target=consensus_monitor, daemon=True),            # Consensus for tracked HSDirs
        threading.Thread(target=network_consensus, daemon=True),            # Consensus for network-wide HSDirs
        threading.Thread(target=scaled_ephemeral_manager, daemon=True),     # Ephemeral onion creator
        threading.Thread(target=active_prober, daemon=True)                 # Tracked onions prober
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

    ended = datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0, tzinfo=None).isoformat(timespec="minutes")

    uptime = {
        "started": started,
        "ended": ended
    }

    filepath = os.path.join(DATA_DIR, "program_execution_period.jsonl")
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(json.dumps(uptime) + ",\n")
        
    print("[INFO] All threads stopped. Exiting cleanly.")
    sys.exit(0) 

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main()
