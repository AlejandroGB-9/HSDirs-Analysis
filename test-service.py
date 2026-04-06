from stem.control import Controller
from stem.descriptor.hidden_service import HiddenServiceDescriptorV3

print("INFO: Setting up controller and proceeding to authenticate")
controller = Controller.from_port(port=9051)
controller.authenticate()

if controller.is_alive():
    print("INFO: Service Tor is acitve and working correctly")

print("TEST: Getting .onion service:")
print()

onions = []

for onion in onions:
    print("SERVICE: " + onion)
    try:
        descriptor = controller.get_hidden_service_descriptor(onion)
        print("\tTEST - STATUS: OK\n")
    except Exception as e:
        print("\tTEST: FAIL - Descriptor not found for " + onion)
        print("\tDEBUG-ERROR: \n\t\t" + str(e) + "\n")
