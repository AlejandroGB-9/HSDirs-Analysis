#!/usr/bin/env python3

#  __    __   ______   _______   __                  _______                                    
# /  |  /  | /      \ /       \ /  |                /       \                                   
# $$ |  $$ |/$$$$$$  |$$$$$$$  |$$/   ______        $$$$$$$  |  ______    _______   _______     
# $$ |__$$ |$$ \__$$/ $$ |  $$ |/  | /      \       $$ |  $$ | /      \  /       | /       |    
# $$    $$ |$$      \ $$ |  $$ |$$ |/$$$$$$  |      $$ |  $$ |/$$$$$$  |/$$$$$$$/ /$$$$$$$/     
# $$$$$$$$ | $$$$$$  |$$ |  $$ |$$ |$$ |  $$/       $$ |  $$ |$$    $$ |$$      \ $$ |          
# $$ |  $$ |/  \__$$ |$$ |__$$ |$$ |$$ |            $$ |__$$ |$$$$$$$$/  $$$$$$  |$$ \_____  __ 
# $$ |  $$ |$$    $$/ $$    $$/ $$ |$$ |            $$    $$/ $$       |/     $$/ $$       |/  |
# $$/   $$/  $$$$$$/  $$$$$$$/  $$/ $$/             $$$$$$$/   $$$$$$$/ $$$$$$$/   $$$$$$$/ $$/ 
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
import stem
import json
import signal
import sys
import queue
import random
import traceback
import datetime
import threading

from temp_descParse import parse_descriptor_v3 as metadataParser
from temp_descParse import json_data_export as jsonExport

from stem.control import Controller
from stem.descriptor.hidden_service import HiddenServiceDescriptorV3

# Configuration
with open('target_onions/onions.json', 'r', encoding='utf-8') as f: OWNED_TARGET_ONIONS = json.load(f)
CONTROL_PORT = 9051  # Indicated and established in the torrc configuration of the client by default
THREAD_POOL = 6
MIN_DELAY = 10
MAX_DELAY = 20
DURATION_TIME_HOURS = 0.1

SHUTDOWN_FLAG = False
FETCH_QUEUE = queue.Queue()

def signal_handler(sig, frame):
    global SHUTDOWN_FLAG
    print("\n\n[EXIT] SIGINT received (Ctrl+C). Shutting down...")
    SHUTDOWN_FLAG = True
    # Allow the main loop to finish the current iteration or break immediately

# HSDir Descriptor Fetch - Information, re-parse and decyphering metadata
def desc_fetcher(thread_id, controller):
    global SHUTDOWN_FLAG

    while not SHUTDOWN_FLAG:

        try:

            try:

                # TARGET: ["ENTITY_NAME", "ONION_SERVICE"]
                target = FETCH_QUEUE.get(timeout=1)
                onion_service = target[1]
            except queue.Empty:
                continue

            print(f"[INFO] Thread-{thread_id} fetching descriptor for HS @ {onion_service}")

            try: 
                desc = controller.get_hidden_service_descriptor(onion_service) #Only returns V2 type by design even if mismatch with version (V3), requires manual re-parse
   
                #Descriptor - Information, re-parse and decyphering metadata
                if desc:
                    print(f"[INFO] Thread-{thread_id} fetching descriptor for HS @ {onion_service}")

                    # print("\n" + "=" * 40)
                    # print("[INFO] Descriptors retrieved...")
                    # print("=" * 40)

                    if hasattr(desc, 'get_bytes'):
                        raw_text = desc.get_bytes().decode('utf-8')
                    else:
                        raw_text = str(desc)
                                
                    # Re-parse as V3 (bypassing controller's V2-only parser)
                    # print("\n[INFO] Re-parsing as HiddenServiceDescriptorV3...")
                    desc_v3 = HiddenServiceDescriptorV3(raw_text.encode('utf-8'))

                    #print(f"[DEBUG] @ Descriptor Content:\n{desc}")
                    
                    # print("[INFO] Attempting decryption...")

                    try:
                        
                        decpt = desc_v3.decrypt(onion_service)
                        # print("\n[DEBUG] @ Decrypted Descriptor - Superencrypted:")
                        # print(decpt)
                        # print("[INFO] Parsing descriptor information...")
                        parse = metadataParser(str(desc_v3), str(decpt))
                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
                        file_name = f"./samples/{target[0]}_{timestamp}.json"
                        # print("[INFO] Exporting information to JSON file...")
                        jsonExport(file_name, parse)

                    except Exception as decrypt_error:
                        print(f"\n[ERROR] Failed to decrypt descriptor: {decrypt_error}")
                        traceback.print_exc()

                else:
                    print("\nDescriptor not found. Try waiting for some minutes.")
                    traceback.print_exc()

            except stem.exceptions.StemException as se:
                print(f"[ERROR] Unexpected stem error processing {onion_service}: {se}")
                traceback.print_exc()

            except Exception as e:
                print(f"[ERROR] Unexpected error processing {onion_service}: {e}")
                traceback.print_exc()

            FETCH_QUEUE.put(target)

        except Exception as e:
            print(f"[ERROR] Unexpected error on loop process: {e}")
            traceback.print_exc()

        if not SHUTDOWN_FLAG:
            for _ in range(int(random.uniform(MIN_DELAY, MAX_DELAY))):
                if SHUTDOWN_FLAG:
                    break
                time.sleep(1)

# Main
def main():

    global SHUTDOWN_FLAG, FETCH_QUEUE
    print("\n" + "=" * 60)
    print("""
 _   _ ____  ____  _        ____                  
| | | / ___||  _ \(_)_ __  |  _ \  ___ ___  ___   
| |_| \___ \| | | | | '__| | | | |/ _ / __|/ __|  
|  _  |___) | |_| | | |    | |_| |  __\__ | (__ _ 
|_|_|_|____/|____/|_|_|    |____/ \___|___/\___(_)
|  ______| |_ ___| |__   ___ _ __                 
| |_ / _ | __/ __| '_ \ / _ | '__|                
|  _|  __| || (__| | | |  __| |                   
|_|  \___|\__\___|_| |_|\___|_|                   
""")
    print("=" * 60)
    
    # Controller - Setup, authentication and descriptor gathering
    try:
        print(f"\n[INFO] Connecting Control-Port @ {CONTROL_PORT}...")
        controller = Controller.from_port(port=CONTROL_PORT)
        print("[INFO] Authenticating client...")
        controller.authenticate()
        print("[INFO] Authenticated successfully...")        

    except stem.SocketError as e:
        print(f"\n[ERROR] Connection Failed: {e}")
        traceback.print_exc()
        sys.exit(1)

    except stem.connection.AuthenticationFailure as e:
        print(f"\n[ERROR] Authentication Failed: {e}")
        traceback.print_exc()
        sys.exit(1)

    except Exception as e:
        print(f"\n[ERROR] Unexpected Error: {e}")
        traceback.print_exc()
        sys.exit(1)

    for onion_service in OWNED_TARGET_ONIONS:
            FETCH_QUEUE.put([onion_service["id"], onion_service["url"]])

    print("[INFO] Shutdown signal: Ctrl+C")

    threads = []
    for i in range(THREAD_POOL):
        t = threading.Thread(target=desc_fetcher, args=(i, controller), daemon=True)
        print(f"[INFO] Started Thread-{i}")
        t.start()
        threads.append(t)

    start_time = time.time()
    end_time=start_time + (DURATION_TIME_HOURS * 3600) if DURATION_TIME_HOURS > 0 else float('inf')

    try:
        while not SHUTDOWN_FLAG:
            if time.time() > end_time:
               SHUTDOWN_FLAG = True
               break
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[EXIT] Manual interruption.")
        SHUTDOWN_FLAG = True    

    print("\n[INFO] Waiting for threads to finish current tasks...")
    for t in threads:
        t.join(timeout=5) # Wait max 5s per thread
        
    print("[INFO] All threads stopped. Exiting cleanly.")
    sys.exit(0) 

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    state = main()
    