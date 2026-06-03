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
from stem.control import Controller, EventType

# Configuration
CONTROL_PORT = 9051
NUM_EPHEMERALS = 50              # Expanded overview array scale
ROTATION_INTERVAL = 86400         # 24h before sweeping and creating fresh ephemerals
CONSENSUS_INTERVAL = 600        # 1 hour tracking snapshots
PROBE_INTERVAL = 1200               # 20 mins (Respectful frequency for checking third-party HSDirs)

with open('target_onions/owned_onions.json', 'r', encoding='utf-8') as f: OWNED_STATIC_ONIONS = json.load(f)
with open('target_onions/third_party_onions.json', 'r', encoding='utf-8') as f: THIRD_PARTY_ONIONS = json.load(f)

SHUTDOWN_FLAG = False
DATA_DIR = "./hsdir_research_data"
os.makedirs(DATA_DIR, exist_ok=True)

# Thread-safe logging helpers
state_lock = threading.Lock()

# Global state tracking for active ephemerals
active_ephemerals = set()
tracked_target_hsdirs = {}  # Format: { fingerprint: {"onion_address": x, "first_seen": x} }

def save_json_log(filename, data):
    try:
        with state_lock:
            filepath = os.path.join(DATA_DIR, filename)
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data) + ",\n")

    except Exception as e:
        print(f"[LOG EXCEPTION] Failed writing entry to {filename}: {e}")

def get_authenticated_controller():
    """Generates isolated socket connections to prevent control channel deadlocks."""
    controller = Controller.from_port(port=CONTROL_PORT)
    controller.authenticate()
    return controller

def signal_handler(sig, frame):
    global SHUTDOWN_FLAG
    print("\n[EXIT] Shutdown signal received. Cleaning up and exiting...")
    SHUTDOWN_FLAG = True

# 1. Asynchronous Event Monitor: Catches Hash Ring positions and targets
def hs_desc_event_listener(event):
    global tracked_target_hsdirs

    try:
        onion_id = getattr(event, 'address', None)
        if not onion_id:
            return
        
        # Classify the service identity profile
        if onion_id in OWNED_STATIC_ONIONS:
            service_type = "STATIC_CONTROL"
        elif onion_id in active_ephemerals:
            service_type = "EPHEMERAL_VARIABLE"
        elif onion_id in THIRD_PARTY_ONIONS:
            service_type = "THIRD_PARTY_PROBE"
        else:
            print(f"[ERROR] Onions service id @ {onion_id} dropped")
            return

        hsdir_fp = getattr(event, 'hsdir', None)

        if not hsdir_fp:
            return

        with state_lock:
            if hsdir_fp not in tracked_target_hsdirs:
                tracked_target_hsdirs[hsdir_fp] = {
                    "associated_onion": f"{onion_id}.onion",
                    "service_type": service_type,
                    "first_discovery": datetime.datetime.now().isoformat()
                }

        log_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "service_type": service_type,
            "onion_address": f"{onion_id}.onion",
            "action": event.action,
            "hsdir_fingerprint": hsdir_fp,
            "replica": getattr(event, 'replica', None),
            "hsdir_index_hrt": getattr(event, 'hsdir_index', None), # Position on the Hash Ring Table
            "reason": getattr(event, 'reason', None)
        }
        print(f"[HRT EVENT] {event.action} -> Onion: {event.hs_address}... on HRT Position: {event.hsdir_index_hrt}")
        save_json_log("hsdir_ring_events.json", log_entry)

    except Exception as e:
        print(f"[EXC-EVENT-LISTENER] Error parsing incoming stream frame: {e}")
        traceback.print_exc()

# 2. Third-Party Active Prober Engine: Forces Un-cached Network Lookups
def active_third_party_prober(controller):
    global SHUTDOWN_FLAG
    print("[PROBER] Active tracking loop initialized for third-party endpoints.")

    ONIONS_TO_QUERY = THIRD_PARTY_ONIONS + OWNED_STATIC_ONIONS
    
    while not SHUTDOWN_FLAG:
        for onion_id in ONIONS_TO_QUERY:
            if SHUTDOWN_FLAG: break

            # Record explicit lookup intent to measure latency and tracking windows
            # probe_intent = {
            #     "timestamp_initiated": datetime.datetime.now().isoformat(),
            #     "target_onion": f"{onion_id}.onion",
            #     "execution_type": "UNCACHED_HSFETCH_PROBE"
            # }
            # save_json_log("prober_intents.json", probe_intent)

            try:
                # Use raw HSFETCH via control port to clear memory dependencies and force network checks
                # This causes Tor to locate and interact with the remote service's current HSDirs
                controller.msg(f"HSFETCH {onion_id}")
            except Exception as e:
                # Captures cases where the command cannot execute locally
                save_json_log("prober_errors.json", {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "onion": onion_id,
                    "error": str(e)
                })
            time.sleep(int(random.uniform(2, 10))) # Slight spacer between unique domain fetches
            
        # Wait for the next monitoring block interval safely
        for _ in range(PROBE_INTERVAL):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

# 3.Consensus Thread: Tracks network-wide HSDir churn, IP ranges, and stability
def consensus_monitor(controller):
    global SHUTDOWN_FLAG, tracked_target_hsdirs
    print("[CONSENSUS] Consensus tracking engine started.")
    
    while not SHUTDOWN_FLAG:

        with state_lock:
            target_fingerprints = list(tracked_target_hsdirs.keys())

        if target_fingerprints:
            print(f"[AUDITOR] Executing localized stability audit on {len(target_fingerprints)} target HSDirs...")
            audited_nodes = []

            for fingerprint in target_fingerprints:
                try:
                    node = controller.get_network_status(fingerprint)
                    if "HSDir" in node.flags:
                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "nickname": node.nickname,
                            "ip_address": node.address,
                            "or_port": node.or_port,
                            "flags": node.flags,
                            "published": node.published.isoformat() if node.published else None,
                            "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"],
                            "service_type": tracked_target_hsdirs[fingerprint]["service_type"]
                        })
                
                except Exception:
                        # Node has fallen out of the active consensus entirely (Churn Event)
                        audited_nodes.append({
                            "fingerprint": fingerprint,
                            "status": "OFFLINE_CHURN",
                            "timestamp_dropped": datetime.datetime.now().isoformat(),
                            "mapped_onion": tracked_target_hsdirs[fingerprint]["associated_onion"]
                        })

            save_json_log("hsdir_consensus_snapshots.json", {
                "timestamp": datetime.datetime.now().isoformat(),
                "total_active_hsdirs": len(audited_nodes),
                "relays": audited_nodes
            })

            print(f"[AUDITOR] Audit completed. Logs committed to targeted_hsdir_consensus.json.")            

        else:
            # Prevent silent gaps: Log a baseline verification record if no nodes are discovered yet
            save_json_log("targeted_hsdir_consensus.json", {
                "timestamp": datetime.datetime.now().isoformat(),
                "active_targets_count": 0,
                "status": "AWAITING_FIRST_DESCRIPTOR_EVENT"
            })
            print("[AUDITOR] Baseline snapshot logged. Awaiting discovery vectors.")   

        # Passive wait tracking interval cleanly
        for _ in range(CONSENSUS_INTERVAL):
            if SHUTDOWN_FLAG: break
            time.sleep(1)

# 4. Scale-Optimized Loop: Drives 50 Ephemeral Onions Simultaneously = Scales out 800 HR reference points
def scaled_ephemeral_loop(controller):
    global SHUTDOWN_FLAG, active_ephemerals

    while not SHUTDOWN_FLAG:
        try:
            # Step A: Clean up any old ephemerals safely
            if active_ephemerals:
                print(f"[SCALE] Clearing {len(active_ephemerals)} expired ephemeral profiles...")
                for onion_id in list(active_ephemerals):
                    try:
                        controller.remove_ephemeral_hidden_service(onion_id)
                    except Exception:
                        pass
                active_ephemerals.clear()
            
            # Step B: Bulk instantiate 50 fresh services across the ring spectrum
            print(f"[SCALE] Spawning {NUM_EPHEMERALS} concurrent v3 ephemeral targets...")
            for i in range(NUM_EPHEMERALS):
                if SHUTDOWN_FLAG: break
                try:
                    # await_publication=False avoids blocking execution threads
                    response = controller.create_ephemeral_hidden_service({80: 8130 + i}, key_content = 'ED25519-V3', await_publication=False)
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
        try: controller.remove_ephemeral_hidden_service(onion_id)
        except: pass

def main():
    print("=" * 60)
    print("""
        _   _ ____  ____  _       
        | | | / ___||  _ \(_)_ __     
        | |_| \___ \| | | | | '__|   
        |  _  |___) | |_| | | |     
        |_|_|_|____/|____/|_|_|    
        |  ______| |_ ___| |__   ___ _ __                 
        | |_ / _ | __/ __| '_ \ / _ | '__|                
        |  _|  __| || (__| | | |  __| |                   
        |_|  \___|\__\___|_| |_|\___|_|                   
    """)
    print("=" * 60)
    
    try:
        controller = Controller.from_port(port=CONTROL_PORT)
        controller.authenticate()
        print("[INIT] Connected and authenticated to Tor Control Port successfully.")
    except Exception as e:
        print(f"[CRITICAL] Could not map control interface: {e}")
        sys.exit(1)
        
    # Hook into Tor's internal descriptor events to catch the exact ring positions
    controller.add_event_listener(hs_desc_event_listener, EventType.HS_DESC)

    threads = [
        threading.Thread(target=consensus_monitor, args=(controller,), daemon=True),
        threading.Thread(target=scaled_ephemeral_loop, args=(controller,), daemon=True),
        threading.Thread(target=active_third_party_prober, args=(controller,), daemon=True)
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