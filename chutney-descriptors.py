#!/usr/bin/env python3

"""
Fetch HSDir descriptors with Stem - Chutney network.
"""

import os
import time
import stem
import traceback
from stem.control import Controller
from stem.descriptor.hidden_service import HiddenServiceDescriptorV3

# Configuration - Client, control port, cookie path and target HSDir onion.
CHUTNEY_DIR = os.path.join(os.getcwd(), "chutney")
NODE_DIR = os.path.join(CHUTNEY_DIR, "net")
NODE_CLIENT = os.path.join(NODE_DIR, os.listdir(NODE_DIR)[0] + "/009c")
CONTROL_PORT = 8009  # Indicated and established in the torrc configuration of the client by default
COOKIE_PATH = os.path.join(NODE_CLIENT, "control_auth_cookie")
HSDIR_HOSTNAME = os.path.join(NODE_DIR, os.listdir(NODE_DIR)[0] + "/010h/hidden_service/hostname")
with open(HSDIR_HOSTNAME) as f: TARGET_ONION = f.read().rstrip("\n")

# Main
def main():
    print("=" * 40)
    print("HSDir Descriptor Fetcher - Chutney")
    print("=" * 40)
    
    # Verification - Cookie existance
    if not os.path.exists(COOKIE_PATH):
        print(f"ERROR: Cookie file not found at {COOKIE_PATH}")
        print("\tVerify that Chutney is running and all nodes are set up")
        return 1
    
    try:
        
        print(f"\nConnecting Control-Port @ {CONTROL_PORT}...")
        controller = Controller.from_port(port=CONTROL_PORT)
        print("\nAuthenticating client ...")
        controller.authenticate()
        print("Authenticated successfully ...")
        
        print(f"Fetching descriptor for target onion HS @ {TARGET_ONION}...")
        desc = controller.get_hidden_service_descriptor(TARGET_ONION) #Only returns V2 type by design even if mismatch with version (V3), requires manual re-parse
        
        if desc:
            print("\n" + "=" * 40)
            print("Descriptor Retrieved")
            print("=" * 40)

            print()

            if hasattr(desc, 'get_bytes'):
                raw_text = desc.get_bytes().decode('utf-8')
            else:
                raw_text = str(desc)
                        
            # Re-parse as V3 (bypassing controller's V2-only parser)
            print("\n[INFO] Re-parsing as HiddenServiceDescriptorV3...\n")
            desc = HiddenServiceDescriptorV3(raw_text.encode('utf-8'))

            print(f"@ Descriptor Content:\n{desc}")
            
            try:
                print("=" * 40)
                print("Decrypting Descriptor...")
                print("=" * 40)
                decpt = desc.decrypt(TARGET_ONION)
                print("\n@ Decrypted Descriptor - Superencrypted:")
                print(decpt)

            except Exception as decrypt_error:
                print(f"\n[ERROR] Failed to decrypt descriptor: {decrypt_error}")
                traceback.print_exc()
                return 1
            
            print("=" * 40)
            return 0

        else:
            print("\nDescriptor not found. Try waiting for a minute.")
            traceback.print_exc()
            return 1
                
    except stem.SocketError as e:
        print(f"\n[ERROR] Connection Failed: {e}")
        print("\tRun './chutney status' to check if nodes are running.")
        traceback.print_exc()
        return 1
    except stem.connection.AuthenticationFailure as e:
        print(f"\n[ERROR] Authentication Failed: {e}")
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n[ERROR] Unexpected Error: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    success = main()
    print(f"Exiting with code {success}...\n")
    exit(0 if success else 1)
    