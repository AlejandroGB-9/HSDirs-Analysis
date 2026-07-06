#!/usr/bin/env python3

import os
import json
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# ENVIRONMENT SETUP & GLOBAL CONFIGURATION

sns.set_theme(style="whitegrid")
plt.rcParams.update({
    'figure.figsize': (12, 6),
    'axes.labelsize': 12,
    'axes.titlesize': 14,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'figure.titlesize': 16,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight'
})

target_files = [
    "network_hsdir_consensus_snapshots.jsonl",
    "hsdir_ring_events.jsonl",
    "tracked_hsdir_consensus_snapshots.jsonl",
    "rotated_network_hsdir_consensus_snapshots.jsonl",
    "rotated_tracked_hsdir_consensus_snapshots.jsonl"
]

print("[INIT] Environment configurations and path parameters established.")

# INGESTION, CLEANSING, AND CACHING PIPELINE
def extract_and_flatten_telemetry(target_filename):
    output_csv = target_filename.replace('.jsonl', '.csv')
    print(f"[PROCESS] Ingesting log arrays from source stream: {target_filename}")
    
    consolidated_records = []
    
    with open(target_filename, 'r') as source_file:
        for index, text_line in enumerate(source_file):
            if not text_line.strip():
                continue
            try:
                parsed_json = json.loads(text_line)
            except json.JSONDecodeError as err:
                print(f"[METRIC ERROR] Skipping malformed schema line entry {index}: {err}")
                continue

            base_timestamp = parsed_json.get("timestamp")
            captured_onion = parsed_json.get("onion_address", None)
            
            nested_array_key = None
            if "relays" in parsed_json:
                nested_array_key = "relays"
                
            if nested_array_key:
                sub_records = parsed_json.get(nested_array_key, [])
                for entry in sub_records:
                    flattened_item = entry.copy()
                    flattened_item["timestamp"] = base_timestamp
                    if captured_onion:
                        flattened_item["onion_address"] = captured_onion
                    consolidated_records.append(flattened_item)
            else:
                consolidated_records.append(parsed_json)
                
    if not consolidated_records:
        print(f"[WARNING] Zero usable records extracted from {target_filename}.")
        return False
        
    df_processed = pd.DataFrame(consolidated_records)
    
    if "timestamp" in df_processed.columns:
        parsed_datetimes = pd.to_datetime(df_processed["timestamp"], errors='coerce')
        df_processed["date"] = parsed_datetimes.dt.strftime("%m-%d")
        df_processed["hour"] = parsed_datetimes.dt.strftime("%H:%M")
        
    if "or_port" in df_processed.columns:
        df_processed.drop(columns=["or_port"], inplace=True, errors='ignore')
        
    for technical_flag in ["Fast", "HSDir", "Stable", "Valid"]:
        if "flags" in df_processed.columns:
            df_processed[technical_flag] = df_processed["flags"].apply(
                lambda val: 1 if (isinstance(val, list) and technical_flag in val) or \
                                 (isinstance(val, str) and technical_flag in val) else 0
            )
        else:
            df_processed[technical_flag] = 0
            
    if "flags" in df_processed.columns:
        df_processed.drop(columns=["flags"], inplace=True, errors='ignore')
        
    for col in df_processed.columns:
        if col in ['ip', 'fingerprint', 'status', 'onion_address', 'action', 'reason', 'family_ids', 'timestamp_dropped']:
            df_processed[col] = df_processed[col].astype(str).replace(['nan', 'None', '0', '0.0'], '')
        elif pd.api.types.is_numeric_dtype(df_processed[col]):
            df_processed[col] = df_processed[col].fillna(0)
        else:
            df_processed[col] = df_processed[col].fillna('')
    
    df_processed.to_csv(output_csv, index=False)
    print(f"[SUCCESS] Normalized and written to disk: {output_csv} [Rows: {len(df_processed)}]")
    return True

# VERIFICATION, SCHEMA VALIDATION, AND CHRONOLOGICAL SORT
def verify_and_load_cached_matrix(csv_path):
    if not os.path.exists(csv_path):
        print(f"[CRITICAL] Operational vector file unavailable on path: {csv_path}")
        return None
    
    df = pd.read_csv(csv_path, low_memory=False)
    
    for col in df.columns:
        if col in ['ip', 'fingerprint', 'status', 'onion_address', 'action', 'reason', 'family_ids', 'timestamp_dropped']:
            df[col] = df[col].fillna('').astype(str)
    
    sort_columns = []
    if "date" in df.columns:
        sort_columns.append("date")
    if "hour" in df.columns:
        sort_columns.append("hour")
    if "fingerprint" in df.columns:
        sort_columns.append("fingerprint")
        
    if sort_columns:
        df.sort_values(by=sort_columns, inplace=True)
        df.reset_index(drop=True, inplace=True)
        
    print(f"[VERIFIED] File loaded: {csv_path} | Dimension Profile: {df.shape}")
    return df

# ANALYSIS 1: NET-WIDE AND TRACKED (+ROATED) STABILITY ANALYSIS (REVISED)
def calculate_stability_matrices(df, context_label):
    print(f"=== PROCESSING {context_label.upper()} CONSENSUS RESILIENCE ===")
    if df is None or df.empty:
        print(f"[ERROR] DataFrame for context '{context_label}' is empty or missing.")
        return None
        
    computed_metrics = []
    grouped_epochs = df.groupby("date")
    
    for observation_day, data_slice in grouped_epochs:
        if observation_day == '':
            continue
        total_unique_identities = data_slice["fingerprint"].nunique()
        
        # Isolate entries showing structural drop-offs or consecutive baseline absences
        offline_indicators = (data_slice["status"] == "offline") | \
                             (data_slice.get("consecutive_hourly_absences", pd.Series(0, index=data_slice.index)) > 0)
        total_offline_identities = data_slice[offline_indicators]["fingerprint"].nunique()
        
        total_active_identities = total_unique_identities - total_offline_identities
        
        ratio_active = (total_active_identities / total_unique_identities * 100) if total_unique_identities > 0 else 0
        ratio_offline = 100.0 - ratio_active
        
        computed_metrics.append({
            "Observation Date": observation_day,
            "Total Unique HSDirs": total_unique_identities,
            "Active Directories": total_active_identities,
            "Active Ratio (%)": round(ratio_active, 2),
            "Offline Attrition": total_offline_identities,
            "Offline Ratio (%)": round(ratio_offline, 2)
        })

    df_summary_matrix = pd.DataFrame(computed_metrics)
    
    matrix_csv_name = f"analysis-results/{context_label}_hsdir_daily_stability_matrix.csv"
    df_summary_matrix.to_csv(matrix_csv_name, index=False)
    print(f"[DATA EXPORT] Saved {context_label} daily stability matrix table to: {matrix_csv_name}")
    
    if not df_summary_matrix.empty:
        plt.figure(figsize=(14, 6))
        plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Active Directories"], marker='o', color='green', linewidth=2, label='Active HSDirs Count')
        plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Offline Attrition"], marker='o', color='red', linewidth=2, label='Offline HSDirs Count')

        for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix["Active Directories"]):
            plt.annotate(
                f"{y}",
                (x, y),
                textcoords="offset points",
                xytext=(0, 8),   # 8 points above the marker
                ha='center',
                fontsize=9
            )

        for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix["Offline Attrition"]):
            plt.annotate(
                f"{y}",
                (x, y),
                textcoords="offset points",
                xytext=(0, 8),   # 8 points above the marker
                ha='center',
                fontsize=9
            )

        plt.title(f"Tor v3 Active HSDirs vs Attrition Churn ({context_label.replace('_', ' ').title()} - 30-Day Timeline)")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Unique Fingerprint Directory Count")
        plt.xticks(rotation=45)
        plt.legend(loc="upper right")
        plt.tight_layout()
        
        figure_png_name = f"analysis-results/{context_label}_hsdir_daily_stability_progression.png"
        plt.savefig(figure_png_name)
        plt.close()
        print(f"[GRAPHIC EXPORT] Saved {context_label} timeline visualization profile to: {figure_png_name}")
        
    return df_summary_matrix

if __name__ == "__main__":
    print("="*60)
    print("STARTING TOR V3 HSDIR DATA EXTRACTION AND PROCESSING")
    print("="*60)
    
    # Run data Ingestion across log profiles
    for filename in target_files:
        if os.path.exists(filename):
            extract_and_flatten_telemetry(filename)
        else:
            print(f"[SKIPPED] Target telemetry log not found in working path: {filename}")
            
    print("="*60)
    print("PROCESSING FILE VERIFICATION")
    print("="*60)
    
    # Reload cleaned frames with explicit index checking and multi-index sorting
    df_net_consensus   = verify_and_load_cached_matrix("network_hsdir_consensus_snapshots.csv")
    df_ring_events     = verify_and_load_cached_matrix("hsdir_ring_events.csv")
    df_track_consensus = verify_and_load_cached_matrix("tracked_hsdir_consensus_snapshots.csv")
    df_rot_net         = verify_and_load_cached_matrix("rotated_network_hsdir_consensus_snapshots.csv")
    df_rot_track       = verify_and_load_cached_matrix("rotated_tracked_hsdir_consensus_snapshots.csv")

    print("="*60)
    print("PROCESSING CAPACITY STABILITY ANALYSIS")
    print("="*60)
    
    # Analysis for Network-Wide and Tracked HSDirs over the 30-day timeline
    if df_net_consensus is not None:
        calculate_stability_matrices(df_net_consensus, context_label="network_wide")
        
    if df_track_consensus is not None:
        calculate_stability_matrices(df_track_consensus, context_label="tracked_targets")

    if df_rot_net is not None:
        calculate_stability_matrices(df_rot_net, context_label="rotated_network_wide")

    if df_rot_track is not None:
        calculate_stability_matrices(df_rot_track, context_label="rotated_tracked_targets")

    print("="*60)
    print("ANALYSIS COMPLETED")
    print("="*60)
