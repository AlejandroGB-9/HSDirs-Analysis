#!/usr/bin/env python3

"""
Fetch HSDir descriptors with Stem - Chutney network.
"""

import os
import time
import stem
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
        #controller.authenticate_cookie(COOKIE_PATH)
        print("Authenticated successfully ...")
        
        print(f"Fetching descriptor for target onion HS @ {TARGET_ONION}...")
        desc = controller.get_hidden_service_descriptor(TARGET_ONION)
        
        if desc:
            print("\n" + "=" * 60)
            print("Descriptor Retrieved")
            print("=" * 60)
            print("\nDescriptor Content:\n")
            print(desc)
            
            print("\n" + "=" * 60)
            return 0
        else:
            print("\nDescriptor not found. Try waiting for a minute.")
            return 1
                
    except stem.SocketError as e:
        print(f"\nConnection Failed: {e}")
        print("\tRun './chutney status' to check if nodes are running.")
        return 1
    except stem.connection.AuthenticationFailure as e:
        print(f"\nAuthentication Failed: {e}")
        print("\tCheck cookie file permissions: chmod 644 " + COOKIE_PATH)
        return 1
    except Exception as e:
        print(f"\nUnexpected Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    success = main()
    print(f"Exiting with code {success}...\n")
    exit(0 if success else 1)
    