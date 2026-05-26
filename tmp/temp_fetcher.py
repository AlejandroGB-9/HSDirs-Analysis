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
from temp_descParse import parse_descriptor_v3 as metadataParser
from temp_descParse import json_data_export as jsonExport
import traceback
import datetime
from stem.control import Controller
from stem.descriptor.hidden_service import HiddenServiceDescriptorV3
import threading

# Configuration
with open('target_onions/owned_onions.json', 'r', encoding='utf-8') as f: OWNED_TARGET_ONIONS = json.load(f)
with open('target_onions/facebook_onions.json', 'r', encoding='utf-8') as f: FACEBOOK_TARGET_ONIONS = json.load(f)
CONTROL_PORT = 9051  # Indicated and established in the torrc configuration of the client by default
SHUTDOWN_FLAG = False
THREAD_POOL = []

def signal_handler(sig, frame):
    global SHUTDOWN_FLAG
    global THREAD_POOL
    print("\n\n[EXIT] SIGINT received (Ctrl+C). Shutting down...")
    SHUTDOWN_FLAG = True
    # Allow the main loop to finish the current iteration or break immediately

# HSDir Descriptor Fetch - Information, re-parse and decyphering metadata
def desc_fetcher(target_onion: list, controller, entity: str, target_file = None):
    global SHUTDOWN_FLAG

    if isinstance(target_onion, list):

        print("[INFO] Target onion list loaded")

    print("[INFO] Starting infinite fetch loop for target onions...")
    print("[INFO] Shutdown signal: Ctrl+C")

    while not SHUTDOWN_FLAG:

        i = 1

        for onion_service in target_onion:

            if SHUTDOWN_FLAG:
                break

            print(f"[INFO] Fetching descriptor for target onion HS @ {onion_service}...")

            try: 
                desc = controller.get_hidden_service_descriptor(onion_service) #Only returns V2 type by design even if mismatch with version (V3), requires manual re-parse
                    
                #Descriptor - Information, re-parse and decyphering metadata
                if desc:
                    # print("\n" + "=" * 40)
                    # print("[INFO] Descriptors retrieved...")
                    # print("=" * 40)

                    if hasattr(desc, 'get_bytes'):
                        raw_text = desc.get_bytes().decode('utf-8')
                    else:
                        raw_text = str(desc)
                                
                    # Re-parse as V3 (bypassing controller's V2-only parser)
                    # print("\n[INFO] Re-parsing as HiddenServiceDescriptorV3...")
                    desc = HiddenServiceDescriptorV3(raw_text.encode('utf-8'))

                    #print(f"[DEBUG] @ Descriptor Content:\n{desc}")
                    
                    # print("[INFO] Attempting decryption...")

                    try:
                        
                        decpt = desc.decrypt(onion_service)
                        # print("\n[DEBUG] @ Decrypted Descriptor - Superencrypted:")
                        # print(decpt)
                        # print("[INFO] Parsing descriptor information...")
                        parse = metadataParser(str(desc), str(decpt))
                        file_name = str(i) + "_" + entity + "_" + str(datetime.datetime.now()).strip(' ').split('.')[:-1][0].replace(' ','_') + ".json"
                        # print("[INFO] Exporting information to JSON file...")
                        jsonExport(file_name, parse)

                i += 1

                    except Exception as decrypt_error:
                        print(f"\n[ERROR] Failed to decrypt descriptor: {decrypt_error}")
                        traceback.print_exc()

                else:
                    print("\nDescriptor not found. Try waiting for some minutes.")
                    traceback.print_exc()
                    
            except Exception as e:
                print(f"[ERROR] Unexpected error processing {onion_service}: {e}")
                traceback.print_exc()

        if not SHUTDOWN_FLAG:
            for _ in range(300):
                if SHUTDOWN_FLAG:
                    break
                time.sleep(1)

# Main
def main():

    global THREAD_POOL
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

        facebook_thread = threading.Thread(target=desc_fetcher, args=(FACEBOOK_TARGET_ONIONS, controller, "FACEBOOK"))
        owned_thread = threading.Thread(target=desc_fetcher, args=(OWNED_TARGET_ONIONS, controller, "OWNED"))

        THREAD_POOL.append(facebook_thread)
        THREAD_POOL.append(owned_thread)

        for t in THREAD_POOL:
            print(f"[INFO] Starting thread {t}....")
            t.start()

        # desc_fetcher(OWN_TARGET_ONIONS, controller, str(REQUEST_COUNTER_INDEX) + "_" + "FACEBOOK" + "_" + str(datetime.datetime.now()).strip(' ').split('.')[:-1][0].replace(' ','_') + ".json")

        for t in THREAD_POOL:
            t.join()

        print("[INFO] All threads finished. Exiting cleanly.")
        sys.exit(0)

    except stem.SocketError as e:
        print(f"\n[ERROR] Connection Failed: {e}")
        print("\tRun './chutney status' to check if nodes are running.")
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

        

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    state = main()
    