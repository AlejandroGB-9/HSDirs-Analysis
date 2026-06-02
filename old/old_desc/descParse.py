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
    Parse v3 Introduction Point blob; Supports both 62-byte (21-byte hash) and 65-byte (24-byte hash) variants.
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
            "cert_raw_base64": content,
            "cert_raw_hex": data.hex(),
            "cert_length_bytes": len(data)
            #"note": "[PENDING-IF] Signature verification via signing key."
        }
    except Exception as e:
        return {"error": str(e)}

def parse_metadata_descriptor(lines: list) -> Dict[str, Any]:

    output_metadata = {
        "create2_formats": None,
        "flow_control": None,
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
            output_metadata["create2_formats"] = line.split(None, 1)[1]
        elif line.startswith("flow-control"):
            output_metadata["flow_control"] = line.split(None, 1)[1]
        elif line.startswith("introduction-point"):
            if current_ip:
                output_metadata["introduction_points"].append(current_ip)
            
            val = line.split(None, 1)[1]
            # DECODE THE BLOB HERE
            decoded_info = parse_introduction_point_blob(val)
            
            current_ip = {
                "index": len(output_metadata["introduction_points"]) + 1,
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
        output_metadata["introduction_points"].append(current_ip)

    return output_metadata

def parse_header_descriptor(lines: list) -> Dict[str, Any]:

    output_descriptor = {
        "hs_version": None,
        "descriptor_lifetime": None,
        "signing_key_cert": None,
        "revision_counter": None,
        "superencrypted": {},
        "signature": None
    }

    i = 0
    collecting_cert = False
    cert_buffer = []
    
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
                output_descriptor["signing_key_cert"] = parsed_cert
                collecting_cert = False
                cert_buffer = []
            i += 1
            continue

        if line.startswith("hs-descriptor"):
            try:
                output_descriptor["hs_version"] = int(line.split()[1])
            except (IndexError, ValueError):
                output_descriptor["hs_version"] = None

        elif line.startswith("descriptor-lifetime"):
            try:
                output_descriptor["descriptor_lifetime"] = int(line.split()[1])
            except (IndexError, ValueError):
                output_descriptor["descriptor_lifetime"] = None

        elif line.startswith("descriptor-signing-key-cert"):
            # Start collecting the certificate
            if i + 1 < len(lines) and lines[i+1].strip().startswith("-----BEGIN"):
                collecting_cert = True
                cert_buffer = [lines[i+1].strip()]
                i += 1

        elif line.startswith("revision-counter"):
            try:
                output_descriptor["revision_counter"] = int(line.split()[1])
            except (IndexError, ValueError):
                output_descriptor["revision_counter"] = None

        elif line.startswith("superencrypted"):
            pass

        elif line.startswith("signature"):
            try:
                sig_value = line.split(None, 1)[1] if len(line.split()) > 1 else None
                output_descriptor["signature"] = sig_value
            except Exception:
                output_descriptor["signature"] = None

        i += 1

    return output_descriptor

def parse_descriptor_v3(header: str, metadata: str) -> Dict[str, Any]:

    init = header.strip().split('\n')
    output = parse_header_descriptor(init)
    lines = metadata.strip().split('\n')
    meta = parse_metadata_descriptor(lines)
    output["superencrypted"] = meta

    return output

def json_data_export(name: str, json_data):
    with open(name, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)
