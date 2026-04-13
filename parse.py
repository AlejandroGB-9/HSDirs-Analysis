import json
import base64
import socket
import struct
from typing import Dict, List, Any, Optional

def decode_base64_field(value: str) -> bytes:
    """Helper to safely decode base64 strings."""
    try:
        clean_val = "".join(value.split())
        return base64.b64decode(clean_val)
    except Exception:
        return b""

def parse_introduction_point_blob(blob_b64: str) -> Dict[str, Any]:
    """
    Parses a v3 Introduction Point blob.
    Supports both 62-byte (21-byte hash) and 65-byte (24-byte hash) variants.
    """
    try:
        data = base64.b64decode(blob_b64)
    except Exception as e:
        return {"error": f"Base64 decode failed: {e}"}

    length = len(data)
    
    if length == 62:
        # Variant 1: 3 + 4 + 2 + 21 + 32
        version_flags = data[0:3].hex()
        ip_bytes = data[3:7]
        port = struct.unpack(">H", data[7:9])[0]
        hash_nonce = data[9:30].hex()  # 21 bytes
        identity_key = data[30:62].hex()
    elif length == 65:
        # Variant 2: 3 + 4 + 2 + 24 + 32
        version_flags = data[0:3].hex()
        ip_bytes = data[3:7]
        port = struct.unpack(">H", data[7:9])[0]
        hash_nonce = data[9:33].hex()  # 24 bytes
        identity_key = data[33:65].hex()
    else:
        return {"error": f"Unexpected length: {length} (expected 62 or 65 bytes)"}

    ip_str = ".".join(str(b) for b in ip_bytes)

    return {
        "length": length,
        "version_flags": version_flags,
        "ip_address": ip_str,
        "port": port,
        "hash_nonce_hex": hash_nonce,
        "identity_key_hex": identity_key,
        "raw_hex": data.hex()
    }

def parse_ed25519_cert(cert_block: str) -> Dict[str, Any]:
    """Parses the ED25519 certificate block."""
    lines = cert_block.strip().split('\n')
    content_lines = [l for l in lines if not l.startswith('-----')]
    content = "".join(content_lines)
    
    try:
        data = base64.b64decode(content)
        return {
            "raw_hex": data.hex(),
            "length_bytes": len(data),
            "note": "Full signature verification requires the signing key."
        }
    except Exception as e:
        return {"error": str(e)}

def parse_descriptor_v3(text: str) -> Dict[str, Any]:
    lines = text.strip().split('\n')
    output = {
        "version": "V3",
        "metadata": {},
        "introduction_points": []
    }
    current_ip = None
    collecting_cert = False
    cert_buffer = []
    cert_type = ""

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        if collecting_cert:
            cert_buffer.append(line)
            if line.startswith("-----END"):
                cert_content = "\n".join(cert_buffer)
                parsed_cert = parse_ed25519_cert(cert_content)
                
                if cert_type == "auth-key":
                    current_ip["certs"]["auth_key"] = parsed_cert
                elif cert_type == "enc-key-cert":
                    current_ip["certs"]["enc_key_cert"] = parsed_cert
                
                collecting_cert = False
                cert_buffer = []
                cert_type = ""
            i += 1
            continue

        if line.startswith("create2-formats"):
            output["metadata"]["create2-formats"] = line.split(None, 1)[1]
        elif line.startswith("flow-control"):
            output["metadata"]["flow-control"] = line.split(None, 1)[1]
        elif line.startswith("introduction-point"):
            if current_ip:
                output["introduction_points"].append(current_ip)
            
            val = line.split(None, 1)[1]
            # DECODE THE BLOB HERE
            decoded_info = parse_introduction_point_blob(val)
            print(f"Decoded info: {decoded_info}")
            
            current_ip = {
                "index": len(output["introduction_points"]) + 1,
                "raw_value": val,
                "routing_info": decoded_info, # Renamed to 'routing_info' for clarity
                "keys": {},
                "certs": {}
            }
        elif line.startswith("onion-key ntor"):
            if current_ip:
                val = line.split(None, 2)[2]
                current_ip["keys"]["onion_key_ntor"] = {
                    "algorithm": "ntor",
                    "raw_base64": val,
                    "decoded_hex": decode_base64_field(val).hex()
                }
        elif line.startswith("enc-key ntor"):
            if current_ip:
                val = line.split(None, 2)[2]
                current_ip["keys"]["enc_key_ntor"] = {
                    "algorithm": "ntor",
                    "raw_base64": val,
                    "decoded_hex": decode_base64_field(val).hex()
                }
        elif line.startswith("auth-key"):
            if i + 1 < len(lines) and lines[i+1].strip().startswith("-----BEGIN"):
                collecting_cert = True
                cert_type = "auth-key"
                cert_buffer = [lines[i+1].strip()]
                i += 1
        elif line.startswith("enc-key-cert"):
            if i + 1 < len(lines) and lines[i+1].strip().startswith("-----BEGIN"):
                collecting_cert = True
                cert_type = "enc-key-cert"
                cert_buffer = [lines[i+1].strip()]
                i += 1
        
        i += 1

    if current_ip:
        output["introduction_points"].append(current_ip)

    return output

# --- Main Execution ---

descriptor_text = """@ Decrypted Descriptor - Superencrypted:
create2-formats 2
flow-control 1-2 31
introduction-point AwAGfwAAARPwAhS9p8E4mrZTupXzNuKZeRGlWIwAUgMgrmowkV2ld2eacE+CJOA2/q/RJ83iymco/kWcVL8WDDY=
onion-key ntor JDL3uwvVOskBT87Wvb+olfO3mi+qMceSHWWicdwBZSA=
auth-key
-----BEGIN ED25519 CERT-----
AQkAB4deAQzi5m/686wDsNHXTIS5hPrO0/0M9r3PPPwOzaHkYPI2AQAgBAA/Nri9
MelmC5CchyZt0gQA9dtElNfAODPBml2XVz9v/5SsReM46FihYsHbhbb3+zPlzqiz
dbSv3Z0f4im51TYJStS+/awy10r53zIJXTeCUTLM/+eKTruSbrPmniPAog0=
-----END ED25519 CERT-----
enc-key ntor EOQvtORGzbTm3rw99XV07cE9urYVea020r+kt4fxrF4=
enc-key-cert
-----BEGIN ED25519 CERT-----
AQsAB4deAV27ZQImga/Gp+HEPzrcvvedCFA4UFTOkUdIwgaGDIIbAQAgBAA/Nri9
MelmC5CchyZt0gQA9dtElNfAODPBml2XVz9v/1O9O7bdZ7Zvpb6x2qiDTt/lUFi+
C+yqNS+g9WvDa+R4J+igQaJoXqFBp2PDb6pSmHN47eXm5znXeOUbOZLWWwg=
-----END ED25519 CERT-----
introduction-point AwAGfwAAARPtAhS+g3qekSkUm6sxjJhru8eF+do32gMgLhQdxtqCDcpUrkE4/ev8UjP6env5bDjbm9tUomwi4Jc=
onion-key ntor PrfRrmVdHPDD9N+uFgnj+alwdDTLjS6HoANog+Hpkws=
auth-key
-----BEGIN ED25519 CERT-----
AQkAB4deAcpETVFE7EuaMIrzM8gEQpq+gWGGI8i+0VmKA8Kdjb8aAQAgBAA/Nri9
MelmC5CchyZt0gQA9dtElNfAODPBml2XVz9v/zobm6eb7tSKl26eIqRsdN2sLXFJ
6zN/txLdCVaZOfLStU37cs7897uppHB+qFpSrMQVdA0zEMUA8SeDu6M8/As=
-----END ED25519 CERT-----
enc-key ntor 79Mni0eTMTRWu2CcbjnCO/hLQG4TuvvGUIhCjjrHYiY=
enc-key-cert
-----BEGIN ED25519 CERT-----
AQsAB4deATkVtSoetULgtOdYOE3UubDZ0ntiUCGYMfq/h9psWpolAQAgBAA/Nri9
MelmC5CchyZt0gQA9dtElNfAODPBml2XVz9v/1OP8Hq9QX7SnV+oNYM8spt4fL7i
ESq08MEgZJhnwYX80Zii5K/Z02a+SnJCFkiIPrU90fssfxkgCAZqUnGcGAk=
-----END ED25519 CERT-----
introduction-point AwAGfwAAARPvAhSQVvwPyN1LeKCcDPq7J2PTl8k4oQMgln3A+op6v9q7+McAIUDv/lmqEs9zdw/Qbc1zyVIWYPw=
onion-key ntor goPqp+OkVDdAPZQuSLuKByhdWOvk5+vfu4qIvgubPnw=
auth-key
-----BEGIN ED25519 CERT-----
AQkAB4deAau0NsMdseb4+KGCdDTs8kjX3MSu8k5xwKMZwRNjHioaAQAgBAA/Nri9
MelmC5CchyZt0gQA9dtElNfAODPBml2XVz9v/0FkxeRMj0l3/78Qctr+uUPnVFhJ
I9i46N+0WevgoP4CJKPHZlOeLlcC4d4zno5KxScxYhcvaZYVFTuAHkqY+g0=
-----END ED25519 CERT-----
enc-key ntor r70NuUL8Wh2gACuDoT3NaWezZV0rSnt7EBZQ5C0c8Hs=
enc-key-cert
-----BEGIN ED25519 CERT-----
AQsAB4deAdLlVV2/piFdsyeUhwLpfc691oaOUES2za+K/wn3UTlJAQAgBAA/Nri9
MelmC5CchyZt0gQA9dtElNfAODPBml2XVz9v/2TU8lwPfjHgx2Zw4EU03MMS+iyL
Z6m0gFjH8GlSATThriAKmRdv2lgTxfVH9dinS7sO2mtpY1LFa3hWZgEvdgY=
-----END ED25519 CERT-----"""

if __name__ == "__main__":
    json_data = parse_descriptor_v3(descriptor_text)
    
    output_filename = "tor_descriptor_decoded.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)
    
    print(f"Successfully created {output_filename}")
    # print(f"Parsed {len(json_data['introduction_points'])} introduction points.")
    
    print(json_data)
    #Display the first introduction point to verify decoding
    if json_data['introduction_points']:
        ip1 = json_data['introduction_points'][0]['routing_info']
        print("\n--- Sample Decoded Routing Info (First Intro Point) ---")
        print(f"Relay ID (Hex): {ip1.get('relay_id_hex')}")
        print(f"IP Address: {ip1.get('ip_address')}")
        print(f"Port: {ip1.get('port')}")
        print(f"Address Type Byte: {ip1.get('address_type_byte')}")
        print(f"Key Length: {ip1.get('key_length')}")
        print(f"Full Hex: {ip1.get('raw_hex')}")