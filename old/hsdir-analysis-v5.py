#!/usr/bin/env python3


#  /$$   /$$  /$$$$$$  /$$$$$$$  /$$                                  
# | $$  | $$ /$$__  $$| $$__  $$|__/                                  
# | $$  | $$| $$  \__/| $$  \ $$ /$$  /$$$$$$                         
# | $$$$$$$$|  $$$$$$ | $$  | $$| $$ /$$__  $$                        
# | $$__  $$ \____  $$| $$  | $$| $$| $$  \__/                        
# | $$  | $$ /$$  \ $$| $$  | $$| $$| $$                              
# | $$  | $$|  $$$$$$/| $$$$$$$/| $$| $$                              
# |__/  |__/ \______/ |_______/ |__/|__/                              
                                                                    
                                                                    
                                                                    
#   /$$$$$$                      /$$                     /$$          
#  /$$__  $$                    | $$                    |__/          
# | $$  \ $$ /$$$$$$$   /$$$$$$ | $$ /$$   /$$  /$$$$$$$ /$$  /$$$$$$$
# | $$$$$$$$| $$__  $$ |____  $$| $$| $$  | $$ /$$_____/| $$ /$$_____/
# | $$__  $$| $$  \ $$  /$$$$$$$| $$| $$  | $$|  $$$$$$ | $$|  $$$$$$ 
# | $$  | $$| $$  | $$ /$$__  $$| $$| $$  | $$ \____  $$| $$ \____  $$
# | $$  | $$| $$  | $$|  $$$$$$$| $$|  $$$$$$$ /$$$$$$$/| $$ /$$$$$$$/
# |__/  |__/|__/  |__/ \_______/|__/ \____  $$|_______/ |__/|_______/ 
#                                    /$$  | $$                        
#                                   |  $$$$$$/                        
#                                    \______/                         


import os
import json
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
from matplotlib.lines import Line2D
from scipy.stats import kstest

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

# Try importing GeoIP2 reader for MaxMind MMDB lookup support
try:
    import geoip2.database
    GEOIP2_AVAILABLE = True
except ImportError:
    GEOIP2_AVAILABLE = False


class FastEntityResolver:
    """
    Optimized In-Memory IP/Subnet/Entity Resolver.
    Performs O(1) hash map lookups with batch disk-flushing at script end.
    Supports MaxMind GeoLite2-ASN .mmdb database lookups if present.
    """
    def __init__(self, db_path="ip_operator_db.csv", mmdb_path="GeoLite2-ASN.mmdb"):
        self.db_path = db_path
        self.mmdb_path = mmdb_path
        self.cache = {}
        self.dirty = False
        self.mmdb_reader = None

        if GEOIP2_AVAILABLE and os.path.exists(self.mmdb_path):
            try:
                self.mmdb_reader = geoip2.database.Reader(self.mmdb_path)
            except Exception as e:
                print(f"[RESOLVER-WARN] Could not initialize GeoIP2 reader: {e}")

        if os.path.exists(self.db_path):
            try:
                df_db = pd.read_csv(self.db_path)
                for _, row in df_db.iterrows():
                    self.cache[str(row['ip'])] = (str(row['subnet_24']), str(row['entity_name']))
            except Exception:
                pass

    def resolve(self, ip_str):
        if not isinstance(ip_str, str) or '.' not in ip_str:
            return "Unknown_Subnet", "Unknown_Entity"

        ip_str = ip_str.strip()

        if ip_str in self.cache:
            return self.cache[ip_str]

        parts = ip_str.split('.')
        subnet_24 = f"{parts[0]}.{parts[1]}.{parts[2]}.0/24" if len(parts) >= 3 else "Unknown_Subnet"

        entity_name = "Unknown_Operator"
        if self.mmdb_reader:
            try:
                resp = self.mmdb_reader.asn(ip_str)
                entity_name = resp.autonomous_system_organization or f"AS{resp.autonomous_system_number}"
            except Exception:
                entity_name = f"Operator_{abs(hash(subnet_24)) % 1000:03d}"
        else:
            entity_name = f"Operator_{abs(hash(subnet_24)) % 1000:03d}"

        self.cache[ip_str] = (subnet_24, entity_name)
        self.dirty = True
        return subnet_24, entity_name

    def flush_to_disk(self):
        if self.dirty and self.cache:
            data = [{"ip": k, "subnet_24": v[0], "entity_name": v[1]} for k, v in self.cache.items()]
            df_export = pd.DataFrame(data)
            df_export.to_csv(self.db_path, index=False)
            self.dirty = False

# INGESTION AND CLEANSING
def extract_telemetry(target_filename):
    output_csv = target_filename.replace('.jsonl', '.csv')
    print(f"[EXTRACT-INFO] Ingesting log from source stream: {target_filename}")
    
    consolidated_records = []
    
    with open(target_filename, 'r') as source_file:
        for index, text_line in enumerate(source_file):
            if not text_line.strip():
                continue
            try:
                parsed_json = json.loads(text_line)
            except json.JSONDecodeError as err:
                print(f"[EXTRACT-ERROR] Malformed line entry skipped {index}: {err}")
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
        print(f"[EXTRACT-WARNING] No usable records extracted from {target_filename}.")
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
            df_processed[technical_flag] = df_processed["flags"].apply(lambda val: 1 if (isinstance(val, list) and technical_flag in val) or (isinstance(val, str) and technical_flag in val) else 0)
        else:
            df_processed[technical_flag] = 0
            
    if "flags" in df_processed.columns:
        df_processed.drop(columns=["flags"], inplace=True, errors='ignore')

    if "declared_family_id" in df_processed.columns:
        df_processed["declared_family_id"] = df_processed["declared_family_id"].astype(str).replace(['nan', 'NaN', 'None', '0', '0.0', '', ' '], 'None')
        df_processed["declared_family_id"] = df_processed["declared_family_id"].fillna('None')
    else:
        df_processed["declared_family_id"] = 'None'

    if "declared_family" in df_processed.columns:
        def clean_initial_family(val):
            if isinstance(val, (list, tuple, np.ndarray)):
                return list(val)
                
            if pd.isna(val) or val is None or val in ['None', 'nan', 'NaN', '[]', '', ' ']:
                return []
                
            if isinstance(val, str):
                val_stripped = val.strip()
                if val_stripped.startswith('[') and val_stripped.endswith(']'):
                    try:
                        parsed = ast.literal_eval(val_stripped)
                        if isinstance(parsed, list):
                            return parsed
                    except Exception:
                        pass
                return [val_stripped]
            return []
            
        df_processed["declared_family"] = df_processed["declared_family"].apply(clean_initial_family)
    else:
        df_processed["declared_family"] = [[] for _ in range(len(df_processed))]
        
    for col in df_processed.columns:
        if col == "declared_family":
            continue
        if df_processed[col].dtype == 'object':
            df_processed[col] = df_processed[col].astype(str).replace(['nan', '0', '0.0', '', ' '], 'None')
        elif pd.api.types.is_numeric_dtype(df_processed[col]):
            df_processed[col] = df_processed[col].fillna(0)
        else:
            df_processed[col] = df_processed[col].fillna('None')
    
    df_processed.to_csv(output_csv, index=False)
    print(f"    --> Normalized and written to disk: {output_csv} [Rows: {len(df_processed)}]")
    return True


# VERIFICATION, VALIDATION, CHRONOLOGICAL SORT AND DAY ARRANGEMENT
def validate_sort_data(csv_path):
    if not os.path.exists(csv_path):
        print(f"[VERIFY-ERROR] Target file unavailable: {csv_path}")
        return None
    
    df = pd.read_csv(csv_path, low_memory=False)
    
    for col in df.columns:
        if col in ['ip_address', 'fingerprint', 'status', 'onion_address', 'action', 'reason', 'declared_family_id', 'timestamp_dropped', 'service_type']:
            df[col] = df[col].fillna('None').astype(str)
            df[col] = df[col].replace(['nan', 'NaN', 'None', ''], 'None')
            
    if "declared_family" in df.columns:
        def parse_csv_family_list(val):
            if isinstance(val, (list, tuple, np.ndarray)):
                return list(val)
            if pd.isna(val) or val is None:
                return []
            val_str = str(val).strip()
            if val_str in ['None', 'nan', 'NaN', '', '[]']:
                return []
            if val_str.startswith('[') and val_str.endswith(']'):
                try:
                    parsed = ast.literal_eval(val_str)
                    if isinstance(parsed, list):
                        return parsed
                except Exception:
                    pass
            return [val_str]
        df["declared_family"] = df["declared_family"].apply(parse_csv_family_list)
    else:
        df["declared_family"] = [[] for _ in range(len(df))]

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    
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
        
    if "date" in df.columns and not df.empty:
        unique_calendar_dates = df["date"].unique()
        unique_calendar_dates = [d for d in unique_calendar_dates if d not in ['', 'nan', 'None']]
        
        date_normalization_map = {date_str: f"Day {idx + 1}" for idx, date_str in enumerate(unique_calendar_dates)}
        df["date"] = df["date"].map(date_normalization_map).fillna(df["date"])
        
    print(f"[VERIFIED-INFO] File loaded: {csv_path} | Dimension Profile: {df.shape}")
    return df

def compute_onion_stability(df_slice, context_label):
    os.makedirs("analysis-results/onion_stability", exist_ok=True)
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice.empty:
        print(f"[ONION-STABILITY-WARNING] Empty snapshot slice received for context: {context_label}")
        return None

    # Determine onion identifier column (mapped_onion or last_known_onion or onion_address)
    onion_col = None
    for candidate in ["mapped_onion", "last_known_onion", "onion_address"]:
        if candidate in df_slice.columns:
            onion_col = candidate
            break
            
    if not onion_col or "fingerprint" not in df_slice.columns:
        print(f"[ONION-STABILITY-ERROR] Missing onion identifier or fingerprint column in {context_label}")
        return None

    df_working = df_slice[
        (df_slice[onion_col].notna()) & (df_slice[onion_col] != 'None') &
        (df_slice['fingerprint'].notna()) & (df_slice['fingerprint'] != 'None')
    ].copy()

    if df_working.empty:
        print(f"[ONION-STABILITY-WARNING] No valid onion/fingerprint entries in {context_label}")
        return None

    unique_days = [d for d in df_working["date"].unique() if d not in ['', 'None']]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    onion_daily_records = []

    for obs_day in unique_days_sorted:
        day_data = df_working[df_working["date"] == obs_day]
        unique_onions = day_data[onion_col].unique()

        for onion in unique_onions:
            onion_data = day_data[day_data[onion_col] == onion]
            
            hours_sorted = sorted(onion_data["hour"].unique())
            snapshot_node_counts = []
            snapshot_fp_sets = []

            for hr in hours_sorted:
                fps = set(onion_data[onion_data["hour"] == hr]["fingerprint"].unique())
                snapshot_fp_sets.append(fps)
                snapshot_node_counts.append(len(fps))

            if not snapshot_node_counts:
                continue

            # Points 1.1 & 1.2: Avg, Min, Max HSDirs for this onion on this day
            avg_nodes = np.mean(snapshot_node_counts)
            min_nodes = np.min(snapshot_node_counts)
            max_nodes = np.max(snapshot_node_counts)

            # Point 1.3: Transitions between consecutive consensus snapshot entries
            new_counts = []
            disappeared_counts = []

            for i in range(1, len(snapshot_fp_sets)):
                prev_set = snapshot_fp_sets[i-1]
                curr_set = snapshot_fp_sets[i]
                new_counts.append(len(curr_set - prev_set))
                disappeared_counts.append(len(prev_set - curr_set))

            avg_new = np.mean(new_counts) if new_counts else 0.0
            avg_disappeared = np.mean(disappeared_counts) if disappeared_counts else 0.0
            c_new = np.sum(new_counts) if new_counts else 0.0
            c_disappeared = np.sum(disappeared_counts) if disappeared_counts else 0.0

            onion_daily_records.append({
                "Observation Date": obs_day,
                "Onion Service": onion,
                "Avg HSDirs": round(float(avg_nodes), 2),
                "Min HSDirs": int(min_nodes),
                "Max HSDirs": int(max_nodes),
                "New HSDirs": int(c_new),
                "Disappeared HSDirs": int(c_disappeared),
                "Avg New HSDirs": round(float(avg_new), 2),
                "Avg Disappeared HSDirs": round(float(avg_disappeared), 2)
            })

    df_onion_daily = pd.DataFrame(onion_daily_records)
    if df_onion_daily.empty:
        print(f"[ONION-STABILITY-ERROR] No daily records computed for context: {context_label}")
        return None

    # Point 1.4: Capacity of Charge per day based on average disappeared nodes threshold
    capacity_records = []
    df_onion_daily["Capacity Charge Status"] = "Unknown"

    for obs_day in unique_days_sorted:
        day_slice_idx = df_onion_daily["Observation Date"] == obs_day
        day_onions_df = df_onion_daily[day_slice_idx]
        
        if day_onions_df.empty:
            continue

        daily_avg_disappeared_threshold = day_onions_df["Avg Disappeared HSDirs"].mean()
        
        better_mask = day_onions_df["Avg Disappeared HSDirs"] < daily_avg_disappeared_threshold
        worse_mask = day_onions_df["Avg Disappeared HSDirs"] >= daily_avg_disappeared_threshold

        df_onion_daily.loc[day_slice_idx & better_mask, "Capacity Charge Status"] = "Better"
        df_onion_daily.loc[day_slice_idx & worse_mask, "Capacity Charge Status"] = "Worse"

        total_onions_day = len(day_onions_df)
        count_better = better_mask.sum()
        count_worse = worse_mask.sum()

        pct_better = round((count_better / total_onions_day) * 100, 2) if total_onions_day > 0 else 0.0
        pct_worse = round((count_worse / total_onions_day) * 100, 2) if total_onions_day > 0 else 0.0

        capacity_records.append({
            "Observation Date": obs_day,
            "Total Unique Onions": total_onions_day,
            "Pct Better Capacity (%)": pct_better,
            "Pct Worse Capacity (%)": pct_worse,
            "Disappeared Node Avg Threshold": round(float(daily_avg_disappeared_threshold), 2)
        })

    df_capacity_daily = pd.DataFrame(capacity_records)

    # Export CSV matrices
    df_onion_daily.to_csv(f"analysis-results/onion_stability/{context_label}_onion_daily_stability_matrix.csv", index=False)
    df_capacity_daily.to_csv(f"analysis-results/onion_stability/{context_label}_onion_capacity_daily_matrix.csv", index=False)

    # -------------------------------------------------------------
    # PLOTTING SPECIFICATIONS
    # -------------------------------------------------------------
    unique_onions = df_onion_daily["Onion Service"].unique()

    # --- Plot 1: Points 1.1 & 1.2 (Avg, Max, Min HSDir Nodes per Onion) ---
    plt.figure(figsize=(15, 8))
    
    warm_colors = plt.cm.YlOrRd(np.linspace(0.5, 0.9, max(1, len(unique_onions))))
    cold_colors = plt.cm.PuBuGn(np.linspace(0.5, 0.9, max(1, len(unique_onions))))
    avg_colors  = plt.cm.Dark2(np.linspace(0.0, 1.0, max(1, len(unique_onions))))

    for idx, onion in enumerate(unique_onions):
        sub = df_onion_daily[df_onion_daily["Onion Service"] == onion]
        plt.plot(sub["Observation Date"], sub["Avg HSDirs"], linestyle='-', color=avg_colors[idx % len(avg_colors)], alpha=0.7, linewidth=1.5)
        plt.plot(sub["Observation Date"], sub["Max HSDirs"], linestyle='--', color=warm_colors[idx % len(warm_colors)], alpha=0.5, linewidth=1.0)
        plt.plot(sub["Observation Date"], sub["Min HSDirs"], linestyle=':', color=cold_colors[idx % len(cold_colors)], alpha=0.5, linewidth=1.0)

    legend_elements_counts = [
        Line2D([0], [0], color='black', linestyle='-', label='Avg HSDir Count (Continuous Line)'),
        Line2D([0], [0], color='red', linestyle='--', label='Max HSDir Count (Discontinuous - Warm Colors)'),
        Line2D([0], [0], color='teal', linestyle=':', label='Min HSDir Count (Discontinuous - Cold Colors)')
    ]
    plt.title(f"Tor v3 Daily HSDir Nodes per Onion Service (Avg, Min, Max)\nSegment: {context_label.replace('_', ' ').title()}")
    plt.xlabel("Observation Timeline")
    plt.ylabel("HSDir Node Count")
    plt.xticks(rotation=45)
    plt.legend(handles=legend_elements_counts, loc="upper right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/onion_stability/{context_label}_onion_hsdir_counts.png")
    plt.close()

    # --- Plot 2: Point 1.3 (Avg New vs Avg Disappeared HSDir Nodes) ---
    plt.figure(figsize=(15, 8))
    
    for idx, onion in enumerate(unique_onions):
        sub = df_onion_daily[df_onion_daily["Onion Service"] == onion]
        plt.plot(sub["Observation Date"], sub["Avg New HSDirs"], linestyle='-', markersize=3, color=cold_colors[idx % len(cold_colors)], alpha=0.6)
        plt.plot(sub["Observation Date"], sub["Avg Disappeared HSDirs"], linestyle='--', markersize=3, color=warm_colors[idx % len(warm_colors)], alpha=0.6)

    legend_elements_churn = [
        Line2D([0], [0], color='teal', marker='o', linestyle='-', label='Avg New HSDirs (Cold Colors)'),
        Line2D([0], [0], color='darkred', marker='x', linestyle='-', label='Avg Disappeared HSDirs (Warm Colors)')
    ]
    plt.title(f"Tor v3 Daily HSDir Churn per Onion Service (New vs Disappeared)\nSegment: {context_label.replace('_', ' ').title()}")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Mean Node Count Delta per Consensus Entry")
    plt.xticks(rotation=45)
    plt.legend(handles=legend_elements_churn, loc="upper right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/onion_stability/{context_label}_onion_hsdir_churn_dlt.png")
    plt.close()

    # --- Plot 2: Point 1.3 (New vs Disappeared HSDir Nodes) ---
    plt.figure(figsize=(15, 8))
    
    for idx, onion in enumerate(unique_onions):
        sub = df_onion_daily[df_onion_daily["Onion Service"] == onion]
        plt.plot(sub["Observation Date"], sub["New HSDirs"], linestyle='-', markersize=3, color=cold_colors[idx % len(cold_colors)], alpha=0.6)
        plt.plot(sub["Observation Date"], sub["Disappeared HSDirs"], linestyle='--', markersize=3, color=warm_colors[idx % len(warm_colors)], alpha=0.6)

    legend_elements_churn = [
        Line2D([0], [0], color='teal', marker='o', linestyle='-', label='Avg New HSDirs (Cold Colors)'),
        Line2D([0], [0], color='darkred', marker='x', linestyle='-', label='Avg Disappeared HSDirs (Warm Colors)')
    ]
    plt.title(f"Tor v3 Daily HSDir Churn per Onion Service (New vs Disappeared)\nSegment: {context_label.replace('_', ' ').title()}")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Absolute Node Count Delta per Consensus Entry")
    plt.xticks(rotation=45)
    plt.legend(handles=legend_elements_churn, loc="upper right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/onion_stability/{context_label}_onion_hsdir_churn_abs.png")
    plt.close()

    # --- Plot 3: Point 1.4 (Capacity of Charge Percentage: Better vs Worse) ---
    if not df_capacity_daily.empty:
        plt.figure(figsize=(12, 6))
        plt.plot(df_capacity_daily["Observation Date"], df_capacity_daily["Pct Better Capacity (%)"], marker='o', color='#2ca02c', linewidth=2.5, label='Better Capacity (% < Daily Avg Churn)')
        plt.plot(df_capacity_daily["Observation Date"], df_capacity_daily["Pct Worse Capacity (%)"], marker='s', color='#d62728', linewidth=2.5, label='Worse Capacity (% >= Daily Avg Churn)')

        for x, y in zip(df_capacity_daily["Observation Date"], df_capacity_daily["Pct Better Capacity (%)"]):
            plt.annotate(f"{y}%", (x, y), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=9, color='#2ca02c', fontweight='bold')
        for x, y in zip(df_capacity_daily["Observation Date"], df_capacity_daily["Pct Worse Capacity (%)"]):
            plt.annotate(f"{y}%", (x, y), textcoords="offset points", xytext=(0, -12), ha='center', fontsize=9, color='#d62728', fontweight='bold')

        plt.title(f"Tor v3 Onion Services Capacity of Charge Distribution\nSegment: {context_label.replace('_', ' ').title()}")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Percentage of Onion Services (%)")
        plt.ylim(-5, 105)
        plt.xticks(rotation=45)
        plt.legend(loc="center right")
        plt.tight_layout()
        plt.savefig(f"analysis-results/onion_stability/{context_label}_onion_capacity_of_charge.png")
        plt.close()

    print(f"[ONION-STABILITY-INFO] Onion stability analysis saved for context: '{context_label}'")
    return df_onion_daily


def onion_stability_analysis(df, context_label):
    print(f"=== PROCESSING {context_label.upper()} ONION SERVICE STABILITY ANALYSIS ===")
    if df is None or df.empty:
        print(f"[ONION-STABILITY-ERROR] Missing data slice for context '{context_label}'.")
        return None

    global_reporting_matrix = compute_onion_stability(df, context_label)

    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]

        for service in unique_services:
            print(f"    --> Isolating onion stability for service type: {service}")
            sub_pop = df[df["service_type"] == service]
            if not sub_pop.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_onion_stability(sub_pop, segmented_label)

    return global_reporting_matrix

def compute_onion_to_hsdir_uniformity(df_slice, context_label):
    """
    Computes distance uniformity metrics for onion services and their assigned HSDirs,
    correlating ring distance dispersion with churn stability metrics.
    """
    os.makedirs("analysis-results/onion_uniformity", exist_ok=True)

    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice is None or df_slice.empty:
        print(f"[UNIFORMITY-WARNING] Empty snapshot slice received for context: {context_label}")
        return None

    # Identify onion column
    onion_col = None
    for candidate in ["mapped_onion", "last_known_onion", "onion_address"]:
        if candidate in df_slice.columns:
            onion_col = candidate
            break

    dist_col = "hsdir_to_onion_ring_distance"
    pos_col = "hsdir_to_onion_ring_position"

    if not onion_col or "fingerprint" not in df_slice.columns:
        print(f"[UNIFORMITY-ERROR] Missing onion identifier or fingerprint in context: {context_label}")
        return None

    df_working = df_slice[
        (df_slice[onion_col].notna()) & (df_slice[onion_col] != 'None') &
        (df_slice['fingerprint'].notna()) & (df_slice['fingerprint'] != 'None')
    ].copy()

    if df_working.empty:
        print(f"[UNIFORMITY-WARNING] No valid entries in context: {context_label}")
        return None

    # Compute normalized ring distance in [0, 1]
    TWO_POW_256 = 2**256
    if dist_col in df_working.columns:
        def parse_dist(val):
            try:
                return float(val) / TWO_POW_256
            except (ValueError, TypeError):
                return np.nan
        df_working["norm_distance"] = df_working[dist_col].apply(parse_dist)
    elif pos_col in df_working.columns:
        df_working["norm_distance"] = pd.to_numeric(df_working[pos_col], errors='coerce') / 100.0
    else:
        print(f"[UNIFORMITY-ERROR] Neither distance nor position column found in {context_label}")
        return None

    df_working = df_working.dropna(subset=["norm_distance"])

    unique_days = [d for d in df_working["date"].unique() if d not in ['', 'None']]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    daily_uniformity_records = []

    for obs_day in unique_days_sorted:
        day_data = df_working[df_working["date"] == obs_day]
        unique_onions = day_data[onion_col].unique()

        for onion in unique_onions:
            onion_data = day_data[day_data[onion_col] == onion]
            distances = onion_data["norm_distance"].values

            if len(distances) == 0:
                continue

            mean_dist = np.mean(distances)
            std_dist = np.std(distances) if len(distances) > 1 else 0.0
            min_dist = np.min(distances)
            max_dist = np.max(distances)
            dist_range = max_dist - min_dist

            # KS-test against Uniform distribution U(0,1)
            ks_stat, _ = kstest(distances, 'uniform') if len(distances) >= 3 else (np.nan, np.nan)
            uniformity_score = 1.0 - ks_stat if not np.isnan(ks_stat) else np.nan

            # Calculate daily churn metrics for correlation
            hours_sorted = sorted(onion_data["hour"].unique())
            snapshot_fp_sets = [
                set(onion_data[onion_data["hour"] == hr]["fingerprint"].unique())
                for hr in hours_sorted
            ]

            new_counts, disappeared_counts = [], []
            for i in range(1, len(snapshot_fp_sets)):
                prev_s, curr_s = snapshot_fp_sets[i-1], snapshot_fp_sets[i]
                new_counts.append(len(curr_s - prev_s))
                disappeared_counts.append(len(prev_s - curr_s))

            tot_new = sum(new_counts)
            tot_disappeared = sum(disappeared_counts)
            total_churn = tot_new + tot_disappeared

            daily_uniformity_records.append({
                "Observation Date": obs_day,
                "Onion Service": onion,
                "Sample Count": len(distances),
                "Mean Distance": round(float(mean_dist), 4),
                "Std Distance": round(float(std_dist), 4),
                "Min Distance": round(float(min_dist), 4),
                "Max Distance": round(float(max_dist), 4),
                "Distance Range": round(float(dist_range), 4),
                "Uniformity Score": round(float(uniformity_score), 4) if not np.isnan(uniformity_score) else np.nan,
                "New HSDirs": tot_new,
                "Disappeared HSDirs": tot_disappeared,
                "Total Churn": total_churn
            })

    df_uniformity = pd.DataFrame(daily_uniformity_records)
    if df_uniformity.empty:
        print(f"[UNIFORMITY-ERROR] No uniformity records calculated for {context_label}")
        return None

    # Calculate correlation matrix between distance metrics and churn
    corr_cols = ["Mean Distance", "Std Distance", "Distance Range", "Uniformity Score", "New HSDirs", "Disappeared HSDirs", "Total Churn"]
    df_corr = df_uniformity[corr_cols].corr(method='pearson').round(4)

    # Save CSV Matrices
    df_uniformity.to_csv(f"analysis-results/onion_uniformity/{context_label}_onion_daily_uniformity_matrix.csv", index=False)
    df_corr.to_csv(f"analysis-results/onion_uniformity/{context_label}_onion_churn_uniformity_correlation_matrix.csv")

    # -------------------------------------------------------------
    # PLOTTING SPECIFICATIONS
    # -------------------------------------------------------------
    unique_onions = df_uniformity["Onion Service"].unique()

    # --- Plot 1: Daily Distance Mean & Standard Deviation (Dispersion) ---
    plt.figure(figsize=(15, 8))
    avg_colors = plt.cm.Dark2(np.linspace(0.0, 1.0, max(1, len(unique_onions))))
    warm_colors = plt.cm.YlOrRd(np.linspace(0.5, 0.9, max(1, len(unique_onions))))

    for idx, onion in enumerate(unique_onions):
        sub = df_uniformity[df_uniformity["Onion Service"] == onion]
        plt.plot(sub["Observation Date"], sub["Mean Distance"], linestyle='-', color=avg_colors[idx % len(avg_colors)], alpha=0.8, linewidth=1.5)
        plt.fill_between(
            sub["Observation Date"],
            np.maximum(0, sub["Mean Distance"] - sub["Std Distance"]),
            np.minimum(1, sub["Mean Distance"] + sub["Std Distance"]),
            color=warm_colors[idx % len(warm_colors)], alpha=0.15
        )

    legend_elements = [
        Line2D([0], [0], color='black', linestyle='-', label='Mean Relative Ring Distance'),
        Line2D([0], [0], color='orange', linestyle='--', label='±1 Std Dev Dispersion (Shaded)')
    ]
    plt.title(f"Tor v3 Daily HSDir Distance Uniformity & Dispersion per Onion Service\nSegment: {context_label.replace('_', ' ').title()}")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Normalized Ring Distance [0, 1]")
    plt.ylim(-0.05, 1.05)
    plt.xticks(rotation=45)
    plt.legend(handles=legend_elements, loc="upper right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/onion_uniformity/{context_label}_onion_distance_uniformity.png")
    plt.close()

    # --- Plot 2: Total Churn vs. Distance Dispersion (Scatter & Trend) ---
    plt.figure(figsize=(12, 7))
    plt.scatter(df_uniformity["Total Churn"], df_uniformity["Std Distance"], color='#1f77b4', alpha=0.6, edgecolors='none', s=40)

    if len(df_uniformity) > 1 and df_uniformity["Total Churn"].nunique() > 1:
        z = np.polyfit(df_uniformity["Total Churn"], df_uniformity["Std Distance"].fillna(0), 1)
        p = np.poly1d(z)
        x_vals = np.linspace(df_uniformity["Total Churn"].min(), df_uniformity["Total Churn"].max(), 100)
        plt.plot(x_vals, p(x_vals), color='#d62728', linestyle='--', linewidth=2.0, label=f'Linear Trend (slope={z[0]:.4f})')

    plt.title(f"HSDir Daily Churn vs. Target Ring Distance Dispersion ($\sigma_d$)\nSegment: {context_label.replace('_', ' ').title()}")
    plt.xlabel("Daily Total Churn (New + Disappeared HSDirs)")
    plt.ylabel("Distance Standard Deviation ($\sigma_d$)")
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/onion_uniformity/{context_label}_onion_churn_vs_distance_dispersion.png")
    plt.close()

    # --- Plot 3: Cumulative Distribution Function (ECDF vs Ideal Uniform) ---
    plt.figure(figsize=(10, 6))
    all_distances = np.sort(df_working["norm_distance"].values)
    ecdf = np.arange(1, len(all_distances) + 1) / len(all_distances)

    plt.plot(all_distances, ecdf, color='#2ca02c', linewidth=2.0, label='Empirical Cumulative Dist (ECDF)')
    plt.plot([0, 1], [0, 1], color='gray', linestyle='--', linewidth=1.5, label='Ideal Uniform Dist U(0,1)')

    plt.title(f"Empirical CDF of HSDir-to-Onion Ring Distance vs. Uniformity\nSegment: {context_label.replace('_', ' ').title()}")
    plt.xlabel("Normalized Distance [0, 1]")
    plt.ylabel("Cumulative Probability")
    plt.xlim(0, 1)
    plt.ylim(0, 1)
    plt.legend(loc="lower right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/onion_uniformity/{context_label}_onion_distance_ecdf.png")
    plt.close()

    print(f"[UNIFORMITY-INFO] Distance uniformity analysis saved for context: '{context_label}'")
    return df_uniformity


def onion_to_hsdir_uniformity_analysis(df, context_label):
    """
    Main entry point for Onion Service to HSDir distance uniformity analysis,
    including sub-population segmentation by service type.
    """
    print(f"=== PROCESSING {context_label.upper()} ONION TO HSDIR DISTANCE UNIFORMITY ANALYSIS ===")
    if df is None or df.empty:
        print(f"[UNIFORMITY-ERROR] Missing data slice for context '{context_label}'.")
        return None

    global_reporting_matrix = compute_onion_to_hsdir_uniformity(df, context_label)

    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]

        for service in unique_services:
            print(f"    --> Isolating distance uniformity for service type: {service}")
            sub_pop = df[df["service_type"] == service]
            if not sub_pop.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_onion_to_hsdir_uniformity(sub_pop, segmented_label)


def compute_ip_sybil_concentration(df_slice, context_label, resolver):
    output_dir = "analysis-results/sybil_ip_concentration"
    os.makedirs(output_dir, exist_ok=True)

    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice.empty or "ip_address" not in df_slice.columns or "fingerprint" not in df_slice.columns:
        print(f"[SYBIL-CONC-ERROR] Missing critical fields for label: {context_label}")
        return None

    df_valid = df_slice[df_slice["fingerprint"] != "None"].copy()
    if df_valid.empty:
        return None

    # Resolve IPs to Subnets and Entity Names
    subnets_and_entities = [resolver.resolve(ip) for ip in df_valid["ip_address"]]
    df_valid["subnet_24"] = [s[0] for s in subnets_and_entities]
    df_valid["entity_name"] = [s[1] for s in subnets_and_entities]

    unique_days = [d for d in df_valid["date"].unique() if d not in ['', 'None', None]]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    daily_summary_list = []
    daily_entity_shares, daily_subnet_shares, daily_ip_shares = [], [], []
    daily_entity_counts, daily_subnet_counts, daily_ip_counts = [], [], []

    for obs_day in unique_days_sorted:
        day_data = df_valid[df_valid["date"] == obs_day]
        day_unique = day_data.drop_duplicates(subset=["fingerprint"])
        total_hsdirs = day_unique["fingerprint"].nunique()
        if total_hsdirs == 0:
            continue

        # 1. Exact Full IP Distribution
        ip_counts = day_unique["ip_address"].value_counts()
        multi_ip_nodes = (ip_counts > 1).sum()
        
        ip_share_dict = {"Observation Date": obs_day}
        ip_cnt_dict = {"Observation Date": obs_day}
        for ip, cnt in ip_counts.items():
            ip_cnt_dict[str(ip)] = cnt
            ip_share_dict[str(ip)] = round((cnt / total_hsdirs) * 100, 2)
        daily_ip_shares.append(ip_share_dict)
        daily_ip_counts.append(ip_cnt_dict)

        # 2. /24 Subnet Distribution
        subnet_counts = day_unique["subnet_24"].value_counts()
        multi_subnet_nodes = (subnet_counts > 1).sum()

        sub_share_dict = {"Observation Date": obs_day}
        sub_cnt_dict = {"Observation Date": obs_day}
        for sub, cnt in subnet_counts.items():
            sub_cnt_dict[str(sub)] = cnt
            sub_share_dict[str(sub)] = round((cnt / total_hsdirs) * 100, 2)
        daily_subnet_shares.append(sub_share_dict)
        daily_subnet_counts.append(sub_cnt_dict)

        # 3. Entity Operator Distribution
        entity_counts = day_unique["entity_name"].value_counts()
        multi_entity_nodes = (entity_counts > 1).sum()

        ent_share_dict = {"Observation Date": obs_day}
        ent_cnt_dict = {"Observation Date": obs_day}
        for ent, cnt in entity_counts.items():
            ent_cnt_dict[str(ent)] = cnt
            ent_share_dict[str(ent)] = round((cnt / total_hsdirs) * 100, 2)
        daily_entity_shares.append(ent_share_dict)
        daily_entity_counts.append(ent_cnt_dict)

        daily_summary_list.append({
            "Observation Date": obs_day,
            "Total Unique HSDirs": total_hsdirs,
            "Unique IPs": len(ip_counts),
            "Co-located IP Nodes (>1 Relay)": multi_ip_nodes,
            "Unique /24 Subnets": len(subnet_counts),
            "Multi-Node Subnets": multi_subnet_nodes,
            "Unique Entities": len(entity_counts),
            "Multi-Node Entities": multi_entity_nodes
        })

    if not daily_summary_list:
        return None

    # Convert to DataFrames
    df_summary = pd.DataFrame(daily_summary_list)
    df_entity_share = pd.DataFrame(daily_entity_shares).fillna(0.0)
    df_entity_count = pd.DataFrame(daily_entity_counts).fillna(0)
    df_subnet_share = pd.DataFrame(daily_subnet_shares).fillna(0.0)
    df_subnet_count = pd.DataFrame(daily_subnet_counts).fillna(0)
    df_ip_share = pd.DataFrame(daily_ip_shares).fillna(0.0)
    df_ip_count = pd.DataFrame(daily_ip_counts).fillna(0)

    # Standardize column headers to strings
    df_entity_share.columns = [str(c) for c in df_entity_share.columns]
    df_entity_count.columns = [str(c) for c in df_entity_count.columns]
    df_subnet_share.columns = [str(c) for c in df_subnet_share.columns]
    df_subnet_count.columns = [str(c) for c in df_subnet_count.columns]
    df_ip_share.columns = [str(c) for c in df_ip_share.columns]
    df_ip_count.columns = [str(c) for c in df_ip_count.columns]

    # Export CSV Matrices
    df_summary.to_csv(f"{output_dir}/{context_label}_sybil_summary_daily.csv", index=False)
    df_entity_share.to_csv(f"{output_dir}/{context_label}_entity_share_matrix_daily.csv", index=False)
    df_entity_count.to_csv(f"{output_dir}/{context_label}_entity_raw_count_matrix_daily.csv", index=False)
    df_subnet_share.to_csv(f"{output_dir}/{context_label}_subnet24_share_matrix_daily.csv", index=False)
    df_subnet_count.to_csv(f"{output_dir}/{context_label}_subnet24_raw_count_matrix_daily.csv", index=False)
    df_ip_share.to_csv(f"{output_dir}/{context_label}_full_ip_share_matrix_daily.csv", index=False)
    df_ip_count.to_csv(f"{output_dir}/{context_label}_full_ip_raw_count_matrix_daily.csv", index=False)

    # --- PLOTTING 1: FULL IP CO-LOCATION (CO-LOCATED ONLY, NO LEGEND) ---
    plt.figure(figsize=(12, 5))
    plt.plot(df_summary["Observation Date"], df_summary["Co-located IP Nodes (>1 Relay)"], marker='s', color='orange')
    plt.title(f"Full IP Co-location & Node Sharing ({context_label.replace('_', ' ').title()})")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{context_label}_full_ip_colocation_plot.png")
    plt.close()

    # --- PLOTTING 2: ALL ENTITIES CONCENTRATION (NO LEGEND) ---
    ent_cols = [c for c in df_entity_share.columns if c != "Observation Date"]
    if ent_cols:
        # 2a. All Entity Share Plot (%)
        plt.figure(figsize=(14, 6))
        for ent in ent_cols:
            plt.plot(df_entity_share["Observation Date"], df_entity_share[ent], marker='o')

        plt.title(f"All Entities Concentration Share Progression ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Network Share (%)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/{context_label}_all_entities_share_plot.png")
        plt.close()

        # 2b. All Entity Raw Count Plot
        plt.figure(figsize=(14, 6))
        for ent in ent_cols:
            plt.plot(df_entity_count["Observation Date"], df_entity_count[ent], marker='o')

        plt.title(f"Total HSDir Relay Count per Entity ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Absolute Node Count")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/{context_label}_all_entities_raw_count_plot.png")
        plt.close()

    # --- PLOTTING 3: ALL /24 SUBNETS CONCENTRATION (NO LEGEND) ---
    sub_cols = [c for c in df_subnet_share.columns if c != "Observation Date"]
    if sub_cols:
        # 3a. All Subnet Share Plot (%)
        plt.figure(figsize=(14, 6))
        for sub in sub_cols:
            plt.plot(df_subnet_share["Observation Date"], df_subnet_share[sub], marker='^')

        plt.title(f"/24 Subnet Concentration Share Progression ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Network Share (%)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/{context_label}_all_subnets_share_plot.png")
        plt.close()

        # 3b. All Subnet Raw Count Plot
        plt.figure(figsize=(14, 6))
        for sub in sub_cols:
            plt.plot(df_subnet_count["Observation Date"], df_subnet_count[sub], marker='^')

        plt.title(f"Total HSDir Relay Count per /24 Subnet ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Absolute Node Count")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/{context_label}_all_subnets_raw_count_plot.png")
        plt.close()

    print(f"[SYBIL-CONC-SUCCESS] Analyzed raw counts & shares for context: '{context_label}'")
    return df_summary


def ip_sybil_concentration_analysis(df, context_label):
    print(f"=== PROCESSING {context_label.upper()} FULL IP & ENTITY CONCENTRATION ANALYSIS ===")
    if df is None or df.empty:
        return None

    # Instantiate Resolver Object with in-memory caching
    resolver = FastEntityResolver(db_path="ip_operator_db.csv", mmdb_path="GeoLite2-ASN.mmdb")

    global_matrix = compute_ip_sybil_concentration(df, context_label, resolver)

    # Segment by service type if tracked
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = [s for s in df["service_type"].dropna().unique() if s not in ['', 'nan', 'None']]
        for service in unique_services:
            sub_df = df[df["service_type"] == service]
            if not sub_df.empty:
                compute_ip_sybil_concentration(sub_df, f"{context_label}_{str(service).lower()}", resolver)

    # Save cache ONCE at the end of execution
    resolver.flush_to_disk()
    return global_matrix


def _extract_family_key(row):
    """
    Extracts a primary family identifier for a relay row:
    1. Uses declared_family_id if present.
    2. Fallback to canonical hash of declared_family fingerprint list if present.
    3. Fallback to 'UNDECLARED_FAMILY'.
    """
    fam_id = row.get("declared_family_id")
    if pd.notna(fam_id) and str(fam_id).strip() not in ["", "None", "null", "nan"]:
        return str(fam_id).strip()

    fam_list = row.get("declared_family")
    if isinstance(fam_list, list) and len(fam_list) > 0:
        sorted_fps = sorted([str(fp).strip().upper() for fp in fam_list if str(fp).strip()])
        if sorted_fps:
            return f"FamilyList_{abs(hash(tuple(sorted_fps))) % 1000000:06d}"

    return "UNDECLARED_FAMILY"


def compute_family_id_to_ip_sybil_concentration(df_slice, context_label, resolver):
    """
    Core computation engine for Family ID to IP/Subnet/Entity Sybil Concentration.
    Processes daily unique HSDir fingerprints, correlates declared_family_id with physical IPs,
    /24 subnets, and Autonomous System Entity Operators, and exports matrices & plots.
    """
    output_dir = "analysis-results/family_ip_sybil_concentration"
    os.makedirs(output_dir, exist_ok=True)

    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice.empty or "fingerprint" not in df_slice.columns or "ip_address" not in df_slice.columns:
        print(f"[FAMILY-SYBIL-ERROR] Missing critical fields for label: {context_label}")
        return None

    df_valid = df_slice[df_slice["fingerprint"].astype(str) != "None"].copy()
    if df_valid.empty:
        return None

    # Standardize Observation Date
    if "date" not in df_valid.columns:
        if "timestamp" in df_valid.columns:
            df_valid["date"] = df_valid["timestamp"].astype(str).apply(lambda x: x.split("T")[0] if "T" in x else x)
        else:
            df_valid["date"] = "Day 01"

    # Resolve IP addresses using FastEntityResolver
    subnets_and_entities = [resolver.resolve(str(ip)) for ip in df_valid["ip_address"]]
    df_valid["subnet_24"] = [s[0] for s in subnets_and_entities]
    df_valid["entity_name"] = [s[1] for s in subnets_and_entities]
    df_valid["family_key"] = df_valid.apply(_extract_family_key, axis=1)

    unique_days = [d for d in df_valid["date"].unique() if d not in ['', 'None', None, 'nan']]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    daily_summary_list = []
    daily_fam_counts, daily_fam_shares = [], []
    daily_fam_to_ip, daily_fam_to_subnet, daily_fam_to_entity = [], [], []

    for obs_day in unique_days_sorted:
        day_data = df_valid[df_valid["date"] == obs_day]
        day_unique = day_data.drop_duplicates(subset=["fingerprint"])
        total_hsdirs = day_unique["fingerprint"].nunique()
        if total_hsdirs == 0:
            continue

        # Family distribution counts
        fam_counts = day_unique["family_key"].value_counts()
        declared_fam_unique = day_unique[day_unique["family_key"] != "UNDECLARED_FAMILY"]
        total_declared_families = declared_fam_unique["family_key"].nunique()
        total_declared_relays = len(declared_fam_unique)

        fam_cnt_dict = {"Observation Date": obs_day}
        fam_share_dict = {"Observation Date": obs_day}
        fam_ip_dict = {"Observation Date": obs_day}
        fam_sub_dict = {"Observation Date": obs_day}
        fam_ent_dict = {"Observation Date": obs_day}

        for fam, cnt in fam_counts.items():
            fam_str = str(fam)
            fam_cnt_dict[fam_str] = cnt
            fam_share_dict[fam_str] = round((cnt / total_hsdirs) * 100, 2)

            fam_nodes = day_unique[day_unique["family_key"] == fam]
            fam_ip_dict[fam_str] = fam_nodes["ip_address"].nunique()
            fam_sub_dict[fam_str] = fam_nodes["subnet_24"].nunique()
            fam_ent_dict[fam_str] = fam_nodes["entity_name"].nunique()

        daily_fam_counts.append(fam_cnt_dict)
        daily_fam_shares.append(fam_share_dict)
        daily_fam_to_ip.append(fam_ip_dict)
        daily_fam_to_subnet.append(fam_sub_dict)
        daily_fam_to_entity.append(fam_ent_dict)

        # Undeclared Sybil assessment
        undec_nodes = day_unique[day_unique["family_key"] == "UNDECLARED_FAMILY"]
        undec_multi_subnets = (undec_nodes["subnet_24"].value_counts() > 1).sum() if not undec_nodes.empty else 0
        undec_multi_entities = (undec_nodes["entity_name"].value_counts() > 1).sum() if not undec_nodes.empty else 0

        daily_summary_list.append({
            "Observation Date": obs_day,
            "Total Unique HSDirs": total_hsdirs,
            "Total Declared Families": total_declared_families,
            "Relays Declaring Family": total_declared_relays,
            "Relays Undeclared": total_hsdirs - total_declared_relays,
            "Declared Family Ratio (%)": round((total_declared_relays / total_hsdirs) * 100, 2),
            "Undeclared Multi-Relay /24 Subnets": undec_multi_subnets,
            "Undeclared Multi-Relay Entities": undec_multi_entities
        })

    if not daily_summary_list:
        return None

    # Convert to DataFrames
    df_summary = pd.DataFrame(daily_summary_list)
    df_fam_count = pd.DataFrame(daily_fam_counts).fillna(0)
    df_fam_share = pd.DataFrame(daily_fam_shares).fillna(0.0)
    df_fam_ip = pd.DataFrame(daily_fam_to_ip).fillna(0)
    df_fam_subnet = pd.DataFrame(daily_fam_to_subnet).fillna(0)
    df_fam_entity = pd.DataFrame(daily_fam_to_entity).fillna(0)

    # Standardize column headers
    for df_mat in [df_fam_count, df_fam_share, df_fam_ip, df_fam_subnet, df_fam_entity]:
        df_mat.columns = [str(c) for c in df_mat.columns]

    # Export CSV Matrices
    df_summary.to_csv(f"{output_dir}/{context_label}_family_sybil_summary_daily.csv", index=False)
    df_fam_count.to_csv(f"{output_dir}/{context_label}_top_family_ids_relay_count_matrix_daily.csv", index=False)
    df_fam_share.to_csv(f"{output_dir}/{context_label}_top_family_ids_share_matrix_daily.csv", index=False)
    df_fam_ip.to_csv(f"{output_dir}/{context_label}_family_to_ip_colocation_matrix_daily.csv", index=False)
    df_fam_subnet.to_csv(f"{output_dir}/{context_label}_family_to_subnet24_colocation_matrix_daily.csv", index=False)
    df_fam_entity.to_csv(f"{output_dir}/{context_label}_family_to_entity_colocation_matrix_daily.csv", index=False)

    # Identify Top Families for Visualization (excluding UNDECLARED_FAMILY for family plots)
    all_fam_cols = [c for c in df_fam_count.columns if c not in ["Observation Date", "UNDECLARED_FAMILY"]]
    if all_fam_cols:
        top_fam_cols = df_fam_count[all_fam_cols].max().sort_values(ascending=False).head(8).index.tolist()
    else:
        top_fam_cols = []

    # --- PLOTTING 1: TOP DECLARED FAMILY IDS RELAY COUNT PROGRESSION ---
    if top_fam_cols:
        plt.figure(figsize=(13, 6))
        for fam in top_fam_cols:
            plt.plot(df_fam_count["Observation Date"], df_fam_count[fam], marker='o', label=f"Family: {fam[:12]}...")

        plt.title(f"Top Declared Family IDs HSDir Relay Count Progression ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Active HSDir Relay Count")
        plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/{context_label}_top_family_ids_count_plot.png")
        plt.close()

        # --- PLOTTING 2: TOP DECLARED FAMILY IDS NETWORK SHARE (%) ---
        plt.figure(figsize=(13, 6))
        for fam in top_fam_cols:
            plt.plot(df_fam_share["Observation Date"], df_fam_share[fam], marker='s', label=f"Family: {fam[:12]}...")

        plt.title(f"Top Declared Family IDs Network Share Progression (%) ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline")
        plt.ylabel("HSDir Network Share (%)")
        plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/{context_label}_family_network_share_plot.png")
        plt.close()

        # --- PLOTTING 3: FAMILY DISPERSION OVER SUBNETS/ENTITIES ---
        plt.figure(figsize=(13, 6))
        primary_fam = top_fam_cols[0]
        plt.plot(df_fam_count["Observation Date"], df_fam_count[primary_fam], marker='o', color='navy', label="Relay Count")
        plt.plot(df_fam_subnet["Observation Date"], df_fam_subnet[primary_fam], marker='^', color='teal', label="Unique /24 Subnets")
        plt.plot(df_fam_entity["Observation Date"], df_fam_entity[primary_fam], marker='x', color='crimson', label="Unique Entities")

        plt.title(f"Infrastructure Dispersion for Top Family ({primary_fam[:14]}...) - {context_label.replace('_', ' ').title()}")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Count")
        plt.legend(loc="upper right")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/{context_label}_family_dispersion_plot.png")
        plt.close()

    # --- PLOTTING 4: UNDECLARED SYBIL CO-LOCATION PROGRESSION ---
    plt.figure(figsize=(12, 5))
    plt.plot(df_summary["Observation Date"], df_summary["Undeclared Multi-Relay /24 Subnets"], marker='d', color='darkorange', label="Undeclared Multi-Relay /24 Subnets")
    plt.plot(df_summary["Observation Date"], df_summary["Undeclared Multi-Relay Entities"], marker='v', color='firebrick', label="Undeclared Multi-Relay Entities")

    plt.title(f"Undeclared Sybil Potential: Co-located Non-Family HSDirs ({context_label.replace('_', ' ').title()})")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Co-located Operator Count")
    plt.legend(loc="upper left")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{context_label}_undeclared_sybil_entities_plot.png")
    plt.close()

    print(f"[FAMILY-SYBIL-SUCCESS] Analyzed Family ID to IP concentration for context: '{context_label}'")
    return df_summary


def family_id_to_ip_sybil_concentration_analysis(df, context_label):
    """
    Main entry point for Family ID to IP Sybil Concentration Analysis.
    Performs global and per-service-type processing, using FastEntityResolver.
    """
    print(f"=== PROCESSING {context_label.upper()} FAMILY ID TO IP SYBIL CONCENTRATION ANALYSIS ===")
    if df is None or df.empty:
        print(f"[FAMILY-SYBIL-WARN] Empty DataFrame provided for context: {context_label}")
        return None

    # Instantiate Resolver Object with in-memory caching
    resolver = FastEntityResolver(db_path="ip_operator_db.csv", mmdb_path="GeoLite2-ASN.mmdb")

    global_matrix = compute_family_id_to_ip_sybil_concentration(df, context_label, resolver)

    # Segment by service type if present
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = [s for s in df["service_type"].dropna().unique() if str(s).strip() not in ['', 'nan', 'None']]
        for service in unique_services:
            sub_df = df[df["service_type"] == service]
            if not sub_df.empty:
                compute_family_id_to_ip_sybil_concentration(sub_df, f"{context_label}_{str(service).lower()}", resolver)

    # Flush cache updates to disk
    resolver.flush_to_disk()
    return global_matrix


def compute_chronological_uptime(df_slice, context_label):
    os.makedirs("analysis-results/chronological_uptime", exist_ok=True)
    computed_metrics = []

    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if "hour" not in df_slice.columns or "fingerprint" not in df_slice.columns:
        print(f"[CHRONO-UPTIME-ERROR] Missing critical structural fields ('hour' or 'fingerprint') for label: {context_label}")
        return None

    df_working = df_slice.copy()
    
    # Hour numerical normalization
    if df_working["hour"].dtype == object or isinstance(df_working["hour"].iloc[0], str):
        if df_working["hour"].str.contains(":").any():
            hour_parts = df_working["hour"].str.split(":", expand=True)
            df_working["hour_num"] = hour_parts[0].astype(int) + hour_parts[1].astype(int) / 60.0
        else:
            df_working["hour_num"] = pd.to_numeric(df_working["hour"], errors='coerce').fillna(0)
    else:
        df_working["hour_num"] = df_working["hour"].astype(float)

    # Key identification: onion target field mapping vs general HSDir fallback
    onion_col = None
    if "mapped_onion" in df_working.columns:
        onion_col = "mapped_onion"
    elif "last_known_onion" in df_working.columns:
        onion_col = "last_known_onion"
    elif "onion_address" in df_working.columns:
        onion_col = "onion_address"

    unique_days = [d for d in df_working["date"].unique() if d not in ['', 'None', None]]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    for observation_day in unique_days_sorted:
        day_data = df_working[df_working["date"] == observation_day]
        all_session_durations = []

        if onion_col and day_data[onion_col].nunique() > 0 and (day_data[onion_col] != 'None').any():
            # Grouped execution per onion target and fingerprint
            unique_onions = [o for o in day_data[onion_col].unique() if o not in ['None', '', None]]
            for onion in unique_onions:
                onion_data = day_data[day_data[onion_col] == onion]
                for fp in onion_data["fingerprint"].unique():
                    if fp in ['None', '', None]:
                        continue
                    df_fp = onion_data[onion_data["fingerprint"] == fp].sort_values(by="hour_num")
                    
                    current_start = None
                    current_last = None
                    
                    for _, row in df_fp.iterrows():
                        is_active = row.get("status", "ACTIVE_IN_RING") in ["ACTIVE_IN_RING", "None"]
                        has_temporal_gap = (current_last is not None) and (row["hour_num"] - current_last > 2.5)
                        
                        if is_active and not has_temporal_gap:
                            if current_start is None:
                                current_start = row["hour_num"]
                            current_last = row["hour_num"]
                        else:
                            if current_start is not None:
                                duration = max(1.0, current_last - current_start)
                                all_session_durations.append(duration)
                            
                            if is_active:
                                current_start = row["hour_num"]
                                current_last = row["hour_num"]
                            else:
                                current_start = None
                                current_last = None
                    
                    if current_start is not None:
                        duration = max(1.0, current_last - current_start)
                        all_session_durations.append(duration)
        else:
            # Fallback: General daily HSDir uptime distribution analysis
            for fp in day_data["fingerprint"].unique():
                if fp in ['None', '', None]:
                    continue
                df_fp = day_data[day_data["fingerprint"] == fp].sort_values(by="hour_num")
                
                current_start = None
                current_last = None
                
                for _, row in df_fp.iterrows():
                    is_active = row.get("status", "ACTIVE_IN_RING") in ["ACTIVE_IN_RING", "None"]
                    has_temporal_gap = (current_last is not None) and (row["hour_num"] - current_last > 2.5)
                    
                    if is_active and not has_temporal_gap:
                        if current_start is None:
                            current_start = row["hour_num"]
                        current_last = row["hour_num"]
                    else:
                        if current_start is not None:
                            duration = max(1.0, current_last - current_start)
                            all_session_durations.append(duration)
                        
                        if is_active:
                            current_start = row["hour_num"]
                            current_last = row["hour_num"]
                        else:
                            current_start = None
                            current_last = None
                
                if current_start is not None:
                    duration = max(1.0, current_last - current_start)
                    all_session_durations.append(duration)

        total_sessions = len(all_session_durations)
        if total_sessions == 0:
            continue

        durations_arr = np.array(all_session_durations)

        computed_metrics.append({
            "Observation Date": observation_day,
            "Total Sessions": total_sessions,
            "Mean Lifespan (hrs)": round(float(durations_arr.mean()), 2),
            "P15 Lifespan (hrs)": round(float(np.percentile(durations_arr, 15)), 2),
            "P25 Lifespan (hrs)": round(float(np.percentile(durations_arr, 25)), 2),
            "Median/P50 Lifespan (hrs)": round(float(np.median(durations_arr)), 2),
            "P75 Lifespan (hrs)": round(float(np.percentile(durations_arr, 75)), 2),
            "P85 Lifespan (hrs)": round(float(np.percentile(durations_arr, 85)), 2),
            "P90 Lifespan (hrs)": round(float(np.percentile(durations_arr, 90)), 2),
            "P95 Lifespan (hrs)": round(float(np.percentile(durations_arr, 95)), 2),
            "P99 Lifespan (hrs)": round(float(np.percentile(durations_arr, 99)), 2),
            "Max Lifespan (hrs)": int(durations_arr.max()),
            "Min Lifespan (hrs)": int(durations_arr.min()),
            "Full Uptime (>=20h)": int(np.sum(durations_arr >= 20)),
            "High Uptime (12-19h)": int(np.sum((durations_arr >= 12) & (durations_arr < 20))),
            "Moderate Uptime (6-11h)": int(np.sum((durations_arr >= 6) & (durations_arr < 12))),
            "Low Uptime (<6h)": int(np.sum(durations_arr < 6))
        })

    df_summary_matrix = pd.DataFrame(computed_metrics)
    if df_summary_matrix.empty:
        print(f"[CHRONO-UPTIME-WARNING] Summary matrix empty for context: {context_label}")
        return None

    # Daily Plot Generation
    plt.figure(figsize=(14, 6))
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Mean Lifespan (hrs)"], marker='o', color='purple', linewidth=2, label='Mean Session Duration')
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Median/P50 Lifespan (hrs)"], marker='s', color='teal', linewidth=2, label='Median (P50)')
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["P85 Lifespan (hrs)"], marker='^', color='crimson', linewidth=1.5, linestyle='--', label='P85 Upper Percentile')
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["P15 Lifespan (hrs)"], marker='v', color='darkgreen', linewidth=1.5, linestyle='--', label='P15 Lower Percentile')

    for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix["Mean Lifespan (hrs)"]):
        plt.annotate(f"{y}h", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=8)

    plt.title(f"Tor v3 Chronological HSDir Operational Lifespan & Percentile Profile ({context_label.replace('_', ' ').title()})")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Lifespan (Hours)")
    plt.xticks(rotation=45)
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/chronological_uptime/{context_label}_daily_chronological_uptime.png")
    plt.close()

    daily_totals = {"Observation Date": "Overall Average"}
    for col in df_summary_matrix.columns:
        if col != "Observation Date":
            daily_totals[col] = round(df_summary_matrix[col].mean(), 2)

    df_summary_export = pd.concat([df_summary_matrix, pd.DataFrame([daily_totals])], ignore_index=True)
    df_summary_export.to_csv(f"analysis-results/chronological_uptime/{context_label}_daily_chronological_uptime_matrix.csv", index=False)

    print(f"[CHRONO-UPTIME-INFO] Complete chronological matrices and percentiles saved under label: '{context_label}'")
    return df_summary_matrix


def chronological_uptime_analysis(df, context_label):
    print(f"=== PROCESSING {context_label.upper()} CHRONOLOGICAL UPTIME & PERCENTILE ANALYSIS ===")
    if df is None or df.empty:
        print(f"[CHRONO-UPTIME-ERROR] Missing valid data slice for context '{context_label}'.")
        return None
    
    global_reporting_matrix = compute_chronological_uptime(df, context_label)

    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating chronological uptime for onion type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_chronological_uptime(sub_population_df, segmented_label)
                
    return global_reporting_matrix


def compute_churn_reconnect_stability(df_slice, context_label):
    """
    Computes daily HSDir reconnection rates, correlates them with hourly node stability
    (New vs Disappeared HSDirs based on ACTIVE_IN_RING state), and outputs a dual-axis plot 
    and aggregated matrix CSV.
    """
    output_dir = "analysis-results/churn_reconnection"
    os.makedirs(output_dir, exist_ok=True)

    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice is None or df_slice.empty:
        print(f"[CHURN-STABILITY-WARNING] Empty data slice received for context: {context_label}")
        return None

    if "hour" not in df_slice.columns or "fingerprint" not in df_slice.columns:
        print(f"[CHURN-STABILITY-ERROR] Missing 'hour' or 'fingerprint' columns in: {context_label}")
        return None

    # Identify Onion Service Identifier column
    onion_col = None
    for candidate in ["mapped_onion", "last_known_onion", "onion_address"]:
        if candidate in df_slice.columns:
            onion_col = candidate
            break

    df_working = df_slice.copy()
    
    # Filter valid fingerprints and onions (if onion column exists)
    if onion_col:
        df_working = df_working[
            df_working[onion_col].notna() & (df_working[onion_col] != 'None') &
            df_working['fingerprint'].notna() & (df_working['fingerprint'] != 'None')
        ]
    else:
        df_working = df_working[
            df_working['fingerprint'].notna() & (df_working['fingerprint'] != 'None')
        ]

    if df_working.empty:
        print(f"[CHURN-STABILITY-WARNING] No valid records after filtering for context: {context_label}")
        return None

    # Standardize hour numeric values
    if df_working["hour"].dtype == object or isinstance(df_working["hour"].iloc[0], str):
        if df_working["hour"].str.contains(":").any():
            hour_parts = df_working["hour"].str.split(":", expand=True)
            df_working["hour_num"] = hour_parts[0].astype(int) + hour_parts[1].astype(int) / 60.0
        else:
            df_working["hour_num"] = pd.to_numeric(df_working["hour"], errors='coerce').fillna(0)
    else:
        df_working["hour_num"] = df_working["hour"].astype(float)

    unique_days = [d for d in df_working["date"].unique() if d not in ['', 'None']]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    daily_metrics = []

    for obs_day in unique_days_sorted:
        day_data = df_working[df_working["date"] == obs_day]
        total_unique_fps = day_data["fingerprint"].nunique()
        
        if total_unique_fps == 0:
            continue

        # --- 1. Reconnection Events Calculation ---
        total_reconnections = 0
        for fp in day_data["fingerprint"].unique():
            fp_timeline = day_data[day_data["fingerprint"] == fp].sort_values(by="hour_num")
            state = None
            reconnect_occurred = 0
            
            for _, row in fp_timeline.iterrows():
                curr_status = row.get("status", row.get("state", "ACTIVE_IN_RING"))
                is_active = (curr_status == "ACTIVE_IN_RING") or pd.isna(curr_status) or curr_status == "None"
                
                if state is None:
                    state = "ACTIVE" if is_active else "OFFLINE"
                elif state == "ACTIVE" and not is_active:
                    state = "OFFLINE"
                elif state == "OFFLINE" and is_active:
                    reconnect_occurred = 1
                    state = "ACTIVE"
            
            total_reconnections += reconnect_occurred

        reconnect_rate_pct = (total_reconnections / total_unique_fps * 100.0) if total_unique_fps > 0 else 0.0

        # --- 2. Hourly State Transition Stability (New vs Disappeared) ---
        hours_sorted = sorted(day_data["hour_num"].unique())
        hourly_fp_sets = []

        for hr in hours_sorted:
            hr_slice = day_data[day_data["hour_num"] == hr]
            active_fps = set(
                hr_slice[
                    hr_slice.get("status", hr_slice.get("state", "ACTIVE_IN_RING")).isin(["ACTIVE_IN_RING", "None"]) | 
                    hr_slice.get("status", hr_slice.get("state", "ACTIVE_IN_RING")).isna()
                ]["fingerprint"].unique()
            )
            hourly_fp_sets.append(active_fps)

        daily_new_count = 0
        daily_disappeared_count = 0

        for i in range(1, len(hourly_fp_sets)):
            prev_set = hourly_fp_sets[i-1]
            curr_set = hourly_fp_sets[i]
            
            daily_new_count += len(curr_set - prev_set)
            daily_disappeared_count += len(prev_set - curr_set)

        daily_metrics.append({
            "Observation Date": obs_day,
            "Total Unique HSDirs": total_unique_fps,
            "Reconnecting HSDirs": total_reconnections,
            "Reconnection Rate (%)": round(reconnect_rate_pct, 2),
            "New HSDirs": daily_new_count,
            "Disappeared HSDirs": daily_disappeared_count
        })

    df_daily = pd.DataFrame(daily_metrics)
    if df_daily.empty:
        print(f"[CHURN-STABILITY-ERROR] No daily metrics produced for context: {context_label}")
        return None

    # Save CSV Matrix
    df_daily.to_csv(f"{output_dir}/{context_label}_daily_churn_reconnection_matrix.csv", index=False)

    # -------------------------------------------------------------
    # PLOT: Daily Reconnections & Stability Correlation (Dual-Axis)
    # -------------------------------------------------------------
    fig, ax1 = plt.subplots(figsize=(14, 7))

    x_labels = df_daily["Observation Date"]
    x_indices = np.arange(len(x_labels))

    # Primary Axis: Node Counts
    line1 = ax1.plot(x_indices, df_daily["Reconnecting HSDirs"], marker='o', color='#8a2be2', linewidth=2.5, label='Reconnecting HSDir Count')
    line2 = ax1.plot(x_indices, df_daily["New HSDirs"], marker='^', color='#2ca02c', linestyle='--', linewidth=1.8, label='New HSDirs (Joined)')
    line3 = ax1.plot(x_indices, df_daily["Disappeared HSDirs"], marker='v', color='#d62728', linestyle=':', linewidth=1.8, label='Disappeared HSDirs (Left)')

    ax1.set_xlabel("Observation Timeline", fontsize=11, fontweight='bold')
    ax1.set_ylabel("HSDir Relay Count", fontsize=11, fontweight='bold')
    ax1.set_xticks(x_indices)
    ax1.set_xticklabels(x_labels, rotation=45, ha='right')
    ax1.grid(True, linestyle=':', alpha=0.6)

    # Secondary Axis: Reconnection Percentage Overlay
    ax2 = ax1.twinx()
    line4 = ax2.plot(x_indices, df_daily["Reconnection Rate (%)"], marker='s', color='#ff7f0e', linewidth=2.0, linestyle='-.', label='Reconnection Rate (%)')
    ax2.set_ylabel("Reconnection Rate over Total Unique HSDirs (%)", fontsize=11, color='#ff7f0e', fontweight='bold')
    ax2.set_ylim(-5, max(100.0, df_daily["Reconnection Rate (%)"].max() + 15))

    # Percentage Annotations
    for x, y in zip(x_indices, df_daily["Reconnection Rate (%)"]):
        ax2.annotate(f"{y}%", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=8, color='#ff7f0e', fontweight='bold')

    # Combined Legend
    lines = line1 + line2 + line3 + line4
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left", framealpha=0.9)

    plt.title(f"Tor v3 HSDir Churn Reconnections & Node Stability Correlation\nContext: {context_label.replace('_', ' ').title()}", fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{context_label}_daily_churn_reconnection_correlation.png", dpi=300)
    plt.close()

    print(f"[CHURN-STABILITY-INFO] Saved matrix CSV and dual-axis correlation plot for context: '{context_label}'")
    return df_daily


def churn_reconnection_analysis(df, context_label):
    """
    Main entry point for Churn Reconnection & Node Stability Analysis.
    Performs global segmentation as well as service-type level breakdowns if available.
    """
    print(f"=== PROCESSING {context_label.upper()} CHURN RECONNECTION ANALYSIS ===")
    if df is None or df.empty:
        print(f"[CHURN-STABILITY-ERROR] Missing or empty data slice for context '{context_label}'.")
        return None

    # Primary Slice Execution
    global_churn_df = compute_churn_reconnect_stability(df, context_label)

    # Breakdown by service_type if available
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if str(s) not in ['', 'nan', 'None']]

        for service in unique_services:
            print(f"    --> Isolating churn reconnections for service type: {service}")
            sub_pop = df[df["service_type"] == service]
            if not sub_pop.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_churn_reconnect_stability(sub_pop, segmented_label)

    return global_churn_df


def compute_strange_churn_behaviour_analysis(df_slice, df_events, context_label):
    output_dir = "analysis-results/strange_churn_behaviour"
    os.makedirs(output_dir, exist_ok=True)
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    def clear_hour_to_float(hour_str):
        if pd.isna(hour_str) or not isinstance(hour_str, str) or ":" not in hour_str:
            return 0.0
        try:
            parts = hour_str.split(":")
            return int(parts[0]) + int(parts[1]) / 60.0
        except Exception:
            return 0.0

    if df_slice.empty:
        print(f"[STRANGE-WARNING] Empty snapshot data slice received for context: {context_label}")
        return None

    # Filter failed ring events (action == FAILED and reason == NOT_FOUND)
    if df_events is None or df_events.empty:
        df_failed_events = pd.DataFrame(columns=["date", "hour", "action", "reason", "fingerprint"])
    else:
        df_failed_events = df_events[
            (df_events["action"] == "FAILED") & (df_events["reason"] == "NOT_FOUND")
        ].copy()

    df_working_snapshots = df_slice.copy()
    df_working_snapshots["hour_float"] = df_working_snapshots["hour"].apply(clear_hour_to_float)
    df_failed_events["hour_float"] = df_failed_events["hour"].apply(clear_hour_to_float) if "hour" in df_failed_events.columns else 0.0

    unique_days = [d for d in df_working_snapshots["date"].unique() if d not in ['', 'None', None]]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)
    
    computed_daily_metrics = []

    for observation_day in unique_days_sorted:
        day_snapshots = df_working_snapshots[df_working_snapshots["date"] == observation_day]
        day_events = df_failed_events[df_failed_events["date"] == observation_day]
        
        # Total uniquely identified HSDir fingerprints overall for this observation day
        total_unique_day_fps = day_snapshots["fingerprint"].nunique()
        
        active_snapshots = day_snapshots[day_snapshots["status"] == "ACTIVE_IN_RING"]
        inactive_snapshots = day_snapshots[day_snapshots["status"] != "ACTIVE_IN_RING"]
        
        total_unique_active_fps = active_snapshots["fingerprint"].nunique()
        total_unique_inactive_fps = inactive_snapshots["fingerprint"].nunique()
        
        strange_fps_this_day = set()
        not_strange_fps_this_day = set()
        
        if not day_events.empty and not day_snapshots.empty:
            snapshot_hours = np.array(sorted(day_snapshots["hour_float"].unique()))
            
            for _, event_row in day_events.iterrows():
                ev_hour = event_row["hour_float"]
                closest_snap_hour = snapshot_hours[np.argmin(np.abs(snapshot_hours - ev_hour))]
                
                # Snapshots matching closest hour
                hour_snaps = day_snapshots[day_snapshots["hour_float"] == closest_snap_hour]
                hour_active_snaps = hour_snaps[hour_snaps["status"] == "ACTIVE_IN_RING"]
                hour_inactive_snaps = hour_snaps[hour_snaps["status"] != "ACTIVE_IN_RING"]
                
                ev_fp = event_row.get("fingerprint", "None")
                if ev_fp != "None" and pd.notna(ev_fp):
                    matched_active = hour_active_snaps[hour_active_snaps["fingerprint"] == ev_fp]["fingerprint"].unique()
                    matched_inactive = hour_inactive_snaps[hour_inactive_snaps["fingerprint"] == ev_fp]["fingerprint"].unique()
                    
                    strange_fps_this_day.update(matched_active)
                    not_strange_fps_this_day.update(matched_inactive)
                    
                    if len(matched_active) == 0 and len(matched_inactive) == 0:
                        if ev_fp in active_snapshots["fingerprint"].values:
                            strange_fps_this_day.add(ev_fp)
                        else:
                            not_strange_fps_this_day.add(ev_fp)
                else:
                    strange_fps_this_day.update(hour_active_snaps["fingerprint"].unique())
                    not_strange_fps_this_day.update(hour_inactive_snaps["fingerprint"].unique())
        
        strange_count = len(strange_fps_this_day)
        not_strange_count = len(not_strange_fps_this_day)
        
        # Percentages over total uniquely identified HSDir nodes ("fingerprint")
        pct_strange_of_total = (strange_count / total_unique_day_fps * 100) if total_unique_day_fps > 0 else 0.0
        pct_not_strange_of_total = (not_strange_count / total_unique_day_fps * 100) if total_unique_day_fps > 0 else 0.0
        
        computed_daily_metrics.append({
            "Observation Date": observation_day,
            "Total Unique HSDirs": total_unique_day_fps,
            "Total Unique Active HSDirs": total_unique_active_fps,
            "Total Unique Inactive HSDirs": total_unique_inactive_fps,
            "Strange Behaviour Count": strange_count,
            "Strange % of Total HSDirs": round(pct_strange_of_total, 2),
            "Not Strange Behaviour Count": not_strange_count,
            "Not Strange % of Total HSDirs": round(pct_not_strange_of_total, 2)
        })

    df_daily_matrix = pd.DataFrame(computed_daily_metrics)
    if df_daily_matrix.empty:
        print(f"[STRANGE-ERROR] Core metric matrix is empty for context: {context_label}")
        return None

    # -------------------------------------------------------------------------
    # PLOT 1: GENERAL HSDIR NODES DAILY COUNT PROGRESSION
    # -------------------------------------------------------------------------
    plt.figure(figsize=(14, 6))
    
    plt.plot(df_daily_matrix["Observation Date"], df_daily_matrix["Strange Behaviour Count"], 
             marker='o', color='darkred', linewidth=2.5, label='Strange Behaviour (Active status, Failed action)')
    plt.plot(df_daily_matrix["Observation Date"], df_daily_matrix["Not Strange Behaviour Count"], 
             marker='s', color='skyblue', linewidth=2, label='Not Strange Behaviour (Inactive status, Failed action)')
    
    for x, y in zip(df_daily_matrix["Observation Date"], df_daily_matrix["Strange Behaviour Count"]):
        plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9, fontweight='bold')
    for x, y in zip(df_daily_matrix["Observation Date"], df_daily_matrix["Not Strange Behaviour Count"]):
        plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, -12), ha='center', fontsize=9)
        
    plt.title(f"Tor v3 HSDir Strange vs Normal Failure Count Progression ({context_label.replace('_', ' ').title()} - Daily)")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Unique Relay Count (Fingerprints)")
    plt.xticks(rotation=45)
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{context_label}_strange_vs_normal_daily_progression.png")
    plt.close()

    # -------------------------------------------------------------------------
    # PLOT 2: PERCENTAGE OF STRANGE & NON-STRANGE OVER TOTAL UNIQUE HSDIRS
    # -------------------------------------------------------------------------
    plt.figure(figsize=(14, 6))
    
    plt.plot(df_daily_matrix["Observation Date"], df_daily_matrix["Strange % of Total HSDirs"], 
             marker='o', color='darkred', linewidth=2.5, label='Strange Behaviour (% of Total HSDirs)')
    plt.plot(df_daily_matrix["Observation Date"], df_daily_matrix["Not Strange % of Total HSDirs"], 
             marker='s', color='skyblue', linewidth=2, label='Not Strange Behaviour (% of Total HSDirs)')
    
    for x, y in zip(df_daily_matrix["Observation Date"], df_daily_matrix["Strange % of Total HSDirs"]):
        plt.annotate(f"{y:.2f}%", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9, fontweight='bold')
    for x, y in zip(df_daily_matrix["Observation Date"], df_daily_matrix["Not Strange % of Total HSDirs"]):
        plt.annotate(f"{y:.2f}%", (x, y), textcoords="offset points", xytext=(0, -12), ha='center', fontsize=9)
        
    max_pct = max(df_daily_matrix["Strange % of Total HSDirs"].max(), df_daily_matrix["Not Strange % of Total HSDirs"].max())
    plt.ylim(0, max_pct * 1.15 + 1)
    
    plt.title(f"Tor v3 HSDir Strange vs Normal Behaviour Percentage ({context_label.replace('_', ' ').title()} - Daily)")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Percentage of Total Unique HSDir Nodes (%)")
    plt.xticks(rotation=45)
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{context_label}_strange_vs_normal_daily_percentage_progression.png")
    plt.close()

    # Export General Daily Matrix
    daily_totals = {
        "Observation Date": "Overall Average",
        "Total Unique HSDirs": round(df_daily_matrix["Total Unique HSDirs"].mean(), 2),
        "Total Unique Active HSDirs": round(df_daily_matrix["Total Unique Active HSDirs"].mean(), 2),
        "Total Unique Inactive HSDirs": round(df_daily_matrix["Total Unique Inactive HSDirs"].mean(), 2),
        "Strange Behaviour Count": round(df_daily_matrix["Strange Behaviour Count"].mean(), 2),
        "Strange % of Total HSDirs": round(df_daily_matrix["Strange % of Total HSDirs"].mean(), 2),
        "Not Strange Behaviour Count": round(df_daily_matrix["Not Strange Behaviour Count"].mean(), 2),
        "Not Strange % of Total HSDirs": round(df_daily_matrix["Not Strange % of Total HSDirs"].mean(), 2)
    }
    df_daily_export = pd.concat([df_daily_matrix, pd.DataFrame([daily_totals])], ignore_index=True)
    df_daily_export.to_csv(f"{output_dir}/{context_label}_strange_vs_normal_daily_matrix.csv", index=False)

    # -------------------------------------------------------------------------
    # PLOT 3: PER ONION SERVICE STRANGE/NORMAL BEHAVIOUR ANALYSIS
    # -------------------------------------------------------------------------
    onion_col = "mapped_onion" if "mapped_onion" in df_working_snapshots.columns else (
        "last_known_onion" if "last_known_onion" in df_working_snapshots.columns else "onion_address"
    )

    if onion_col in df_working_snapshots.columns:
        unique_onions = [o for o in df_working_snapshots[onion_col].unique() if o not in ['', 'None', None, 'nan']]
        onion_daily_records = []
        
        for onion_addr in unique_onions:
            onion_snaps = df_working_snapshots[df_working_snapshots[onion_col] == onion_addr]
            
            if df_events is not None and not df_events.empty and onion_col in df_events.columns:
                onion_events = df_failed_events[df_failed_events[onion_col] == onion_addr]
            elif df_events is not None and not df_events.empty and "onion_address" in df_events.columns:
                onion_events = df_failed_events[df_failed_events["onion_address"] == onion_addr]
            else:
                onion_events = pd.DataFrame()
                
            for obs_day in unique_days_sorted:
                day_o_snaps = onion_snaps[onion_snaps["date"] == obs_day]
                day_o_events = onion_events[onion_events["date"] == obs_day] if not onion_events.empty else pd.DataFrame()
                
                o_strange_fps = set()
                o_not_strange_fps = set()
                
                if not day_o_events.empty and not day_o_snaps.empty:
                    snap_hours = np.array(sorted(day_o_snaps["hour_float"].unique()))
                    for _, ev_row in day_o_events.iterrows():
                        ev_h = ev_row["hour_float"]
                        closest_h = snap_hours[np.argmin(np.abs(snap_hours - ev_h))]
                        
                        hr_snaps = day_o_snaps[day_o_snaps["hour_float"] == closest_h]
                        ev_fp = ev_row.get("fingerprint", "None")
                        
                        if ev_fp != "None" and pd.notna(ev_fp):
                            if not hr_snaps[hr_snaps["fingerprint"] == ev_fp].empty:
                                is_act = hr_snaps[hr_snaps["fingerprint"] == ev_fp]["status"].iloc[0] == "ACTIVE_IN_RING"
                                (o_strange_fps if is_act else o_not_strange_fps).add(ev_fp)
                            else:
                                o_strange_fps.add(ev_fp)
                        else:
                            o_strange_fps.update(hr_snaps[hr_snaps["status"] == "ACTIVE_IN_RING"]["fingerprint"].unique())
                            o_not_strange_fps.update(hr_snaps[hr_snaps["status"] != "ACTIVE_IN_RING"]["fingerprint"].unique())
                            
                onion_daily_records.append({
                    "Onion Service": onion_addr,
                    "Observation Date": obs_day,
                    "Strange Behaviour Count": len(o_strange_fps),
                    "Not Strange Behaviour Count": len(o_not_strange_fps)
                })

        df_onion_daily = pd.DataFrame(onion_daily_records)
        
        if not df_onion_daily.empty:
            plt.figure(figsize=(14, 7))
            unique_onion_list = list(df_onion_daily["Onion Service"].unique())
            
            for onion_addr in unique_onion_list:
                sub_o = df_onion_daily[df_onion_daily["Onion Service"] == onion_addr]
                
                # Plot lines without displaying individual onion names in legend
                plt.plot(sub_o["Observation Date"], sub_o["Strange Behaviour Count"], 
                         marker='o', linestyle='-', color='navy', alpha=0.7, linewidth=1.8)
                plt.plot(sub_o["Observation Date"], sub_o["Not Strange Behaviour Count"], 
                         marker='x', linestyle='--', color='lightseagreen', alpha=0.6, linewidth=1.5)

            # Custom Legend without onion service names
            legend_elements = [
                Line2D([0], [0], color='navy', marker='o', lw=2, label='Strange Behaviour'),
                Line2D([0], [0], color='lightseagreen', marker='x', lw=2, linestyle='--', label='Not Strange Behaviour')
            ]

            plt.title(f"Tor v3 Per-Onion Strange vs Normal Behaviour Progression ({context_label.replace('_', ' ').title()})")
            plt.xlabel("Observation Timeline")
            plt.ylabel("Absolute Node Count")
            plt.xticks(rotation=45)
            plt.legend(handles=legend_elements, loc="upper right", fontsize=10)
            plt.tight_layout()
            plt.savefig(f"{output_dir}/{context_label}_per_onion_strange_behaviour_progression.png")
            plt.close()

            # Save clean Onion Analysis Matrix
            df_onion_daily.to_csv(f"{output_dir}/{context_label}_onion_strange_behaviour_matrix.csv", index=False)

    print(f"[STRANGE-INFO] Strange vs Normal behaviour analysis successfully executed for context: '{context_label}'")
    return df_daily_matrix


def strange_churn_behaviour_analysis(df, df_events, context_label):
    print(f"=== PROCESSING {context_label.upper()} STRANGE CHURN BEHAVIOUR ANALYSIS ===")
    if df is None or df.empty:
        print(f"[STRANGE-ERROR] Missing input data layout for context '{context_label}'.")
        return None
        
    global_reporting_matrix = compute_strange_churn_behaviour_analysis(df, df_events, context_label)
    
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None', None]]
        
        for service in unique_services:
            print(f"    --> Isolating strange churn failures for hidden service type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_strange_churn_behaviour_analysis(sub_population_df, df_events, segmented_label)
                
    return global_reporting_matrix


def compute_entity_drop_analysis(df_slice, context_label):
    """
    Computes daily entity drop metrics (Total and Partial drops) evaluated at hourly granularity 
    across three entity groupings: Family ID, Complete IP, and /24 Subnet.
    
    Omits nodes with consecutive_hourly_absences > 1 as ghost metrics.
    """
    output_dir = "analysis-results/entity_drop_analysis"
    os.makedirs(output_dir, exist_ok=True)

    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice.empty:
        print(f"[ENTITY-DROP-WARNING] Empty snapshot data slice received for context: {context_label}")
        return None

    df_working = df_slice.copy()

    # Filter out ghost nodes (consecutive_hourly_absences > 1) prior to analysis
    if "consecutive_hourly_absences" in df_working.columns:
        initial_count = len(df_working)
        df_working = df_working[
            df_working["consecutive_hourly_absences"].isna() | 
            (df_working["consecutive_hourly_absences"] <= 1)
        ]
        removed_ghosts = initial_count - len(df_working)

    # Automatically derive /24 Subnet if not explicitly pre-computed
    if "subnet_24" not in df_working.columns and "ip_address" in df_working.columns:
        def get_subnet(ip):
            if isinstance(ip, str) and "." in ip:
                return ".".join(ip.split(".")[:3]) + ".0/24"
            return None
        df_working["subnet_24"] = df_working["ip_address"].apply(get_subnet)

    unique_days = [d for d in df_working["date"].unique() if d not in ['', 'None', None] and pd.notna(d)]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    computed_daily_metrics = []

    entity_configs = [
        ("declared_family_id", "Family ID"),
        ("ip_address", "Complete IP"),
        ("subnet_24", "/24 Subnet")
    ]

    for observation_day in unique_days_sorted:
        day_snapshots = df_working[df_working["date"] == observation_day]
        unique_hours = day_snapshots["hour"].unique() if "hour" in day_snapshots.columns else [0]

        for col_name, entity_label in entity_configs:
            if col_name not in day_snapshots.columns:
                continue

            # Filter out invalid or missing entity keys
            valid_day_snaps = day_snapshots[
                day_snapshots[col_name].notna() & 
                (~day_snapshots[col_name].isin(['', 'None', 'nan', None]))
            ]

            if valid_day_snaps.empty:
                continue

            total_unique_day_entities = valid_day_snaps[col_name].nunique()

            # Track unique entities experiencing total or partial drops on this observation day
            total_drop_entities_day = set()
            partial_drop_entities_day = set()

            # Measure drop conditions hour by hour for maximum precision
            for h in unique_hours:
                hour_snaps = valid_day_snaps[valid_day_snaps["hour"] == h] if "hour" in valid_day_snaps.columns else valid_day_snaps

                if hour_snaps.empty:
                    continue

                # Group by entity key and count active vs inactive node statuses
                grouped = hour_snaps.groupby(col_name)["status"].agg(
                    active=lambda s: (s == "ACTIVE_IN_RING").sum(),
                    dropped=lambda s: (s != "ACTIVE_IN_RING").sum()
                )

                for entity_key, row in grouped.iterrows():
                    active = row["active"]
                    dropped = row["dropped"]

                    # Total Drop: All remaining valid nodes dropped, zero active
                    if active == 0 and dropped > 0:
                        total_drop_entities_day.add(entity_key)
                    # Partial Drop: Strictly >= 2 nodes dropped, and at least 1 node active
                    elif dropped >= 2 and active > 0:
                        partial_drop_entities_day.add(entity_key)

            total_drops_cnt = len(total_drop_entities_day)
            partial_drops_cnt = len(partial_drop_entities_day)

            pct_total_drops = (total_drops_cnt / total_unique_day_entities * 100.0) if total_unique_day_entities > 0 else 0.0
            pct_partial_drops = (partial_drops_cnt / total_unique_day_entities * 100.0) if total_unique_day_entities > 0 else 0.0

            computed_daily_metrics.append({
                "Observation Date": observation_day,
                "Entity Type": entity_label,
                "Total Unique Entities": total_unique_day_entities,
                "Total Drops Count": total_drops_cnt,
                "Total Drops %": round(pct_total_drops, 2),
                "Partial Drops Count": partial_drops_cnt,
                "Partial Drops %": round(pct_partial_drops, 2)
            })

    df_daily_matrix = pd.DataFrame(computed_daily_metrics)
    if df_daily_matrix.empty:
        print(f"[ENTITY-DROP-ERROR] Core metric matrix is empty for context: {context_label}")
        return None

    color_palette = {
        "Family ID": {"total": "#d90429", "partial": "#f77f00"},
        "Complete IP": {"total": "#003049", "partial": "#669bbc"},
        "/24 Subnet": {"total": "#2a9d8f", "partial": "#e9c46a"}
    }
    entity_types = df_daily_matrix["Entity Type"].unique()

    # -------------------------------------------------------------------------
    # PLOT 1: DAILY ABSOLUTE COUNTS PROGRESSION
    # -------------------------------------------------------------------------
    plt.figure(figsize=(14, 7))

    for ent_type in entity_types:
        sub_df = df_daily_matrix[df_daily_matrix["Entity Type"] == ent_type]
        c_tot = color_palette.get(ent_type, {}).get("total", "red")
        c_part = color_palette.get(ent_type, {}).get("partial", "orange")

        plt.plot(sub_df["Observation Date"], sub_df["Total Drops Count"],
                 marker='o', linewidth=2.2, color=c_tot, label=f'{ent_type} - Total Drops')
        plt.plot(sub_df["Observation Date"], sub_df["Partial Drops Count"],
                 marker='x', linestyle='--', linewidth=1.8, color=c_part, label=f'{ent_type} - Partial Drops')

        for x, y in zip(sub_df["Observation Date"], sub_df["Total Drops Count"]):
            if y > 0:
                plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8, fontweight='bold')
        for x, y in zip(sub_df["Observation Date"], sub_df["Partial Drops Count"]):
            if y > 0:
                plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, -10), ha='center', fontsize=8)

    plt.title(f"Tor v3 HSDir Daily Entity Drop Absolute Counts ({context_label.replace('_', ' ').title()})")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Number of Dropped Entities (Daily)")
    plt.xticks(rotation=45)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{context_label}_entity_drops_daily_progression.png")
    plt.close()

    # -------------------------------------------------------------------------
    # PLOT 2: DAILY PERCENTAGE PROGRESSION
    # -------------------------------------------------------------------------
    plt.figure(figsize=(14, 7))

    for ent_type in entity_types:
        sub_df = df_daily_matrix[df_daily_matrix["Entity Type"] == ent_type]
        c_tot = color_palette.get(ent_type, {}).get("total", "red")
        c_part = color_palette.get(ent_type, {}).get("partial", "orange")

        plt.plot(sub_df["Observation Date"], sub_df["Total Drops %"],
                 marker='o', linewidth=2.2, color=c_tot, label=f'{ent_type} - Total Drops (%)')
        plt.plot(sub_df["Observation Date"], sub_df["Partial Drops %"],
                 marker='x', linestyle='--', linewidth=1.8, color=c_part, label=f'{ent_type} - Partial Drops (%)')

        for x, y in zip(sub_df["Observation Date"], sub_df["Total Drops %"]):
            if y > 0:
                plt.annotate(f"{y:.2f}%", (x, y), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8, fontweight='bold')
        for x, y in zip(sub_df["Observation Date"], sub_df["Partial Drops %"]):
            if y > 0:
                plt.annotate(f"{y:.2f}%", (x, y), textcoords="offset points", xytext=(0, -10), ha='center', fontsize=8)

    max_pct = max(df_daily_matrix["Total Drops %"].max(), df_daily_matrix["Partial Drops %"].max())
    plt.ylim(0, max(max_pct * 1.15 + 1, 5))

    plt.title(f"Tor v3 HSDir Daily Entity Drop Percentage Progression ({context_label.replace('_', ' ').title()})")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Percentage of Total Entities (%)")
    plt.xticks(rotation=45)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{context_label}_entity_drops_daily_percentage_progression.png")
    plt.close()

    # Save Clean Daily Entity Matrix
    df_daily_matrix.to_csv(f"{output_dir}/{context_label}_entity_drops_daily_matrix.csv", index=False)

    print(f"[ENTITY-DROP-INFO] Entity drop analysis successfully executed for context: '{context_label}'")
    return df_daily_matrix


def entity_drop_analysis(df, context_label):
    """
    Wrapper function matching hsdir-analysis-v3.py standard. Handles sub-population segmentation if applicable.
    """
    print(f"=== PROCESSING {context_label.upper()} ENTITY DROP ANALYSIS ===")
    if df is None or df.empty:
        print(f"[ENTITY-DROP-ERROR] Missing input data layout for context '{context_label}'.")
        return None

    global_reporting_matrix = compute_entity_drop_analysis(df, context_label)

    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None', None]]

        for service in unique_services:
            print(f"    --> Isolating entity drop analysis for hidden service type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_entity_drop_analysis(sub_population_df, segmented_label)

    return global_reporting_matrix


# MAIN
if __name__ == "__main__":
    print("=" * 60)
    print("""
  _   _ ____  ____  _         
 | | | / ___||  _ \(_)_ __
 | |_| \___ \| | | | | '__|
 |  _  |___) | |_| | | |
 |_| |_|____/|____/|_|_|
     _                _           _
    / \   _ __   __ _| |_   _ ___(_)___
   / _ \ | '_ \ / _` | | | | / __| / __|
  / ___ \| | | | (_| | | |_| \__ \ \__ \                              
 /_/   \_\_| |_|\__,_|_|\__, |___/_|___/
                        |___/
    """)
    print("="*60)
    print("STARTING TOR V3 HSDIR DATA EXTRACTION AND PROCESSING")
    print("="*60)
    
    # Run data Ingestion across log profiles (uncomment to process clean logs)
    # for filename in target_files:
    #     if os.path.exists(filename):
    #         extract_telemetry(filename)
    #     else:
    #         print(f"[INGESTION-ERROR] Target telemetry log not found: {filename}")
            
    print("="*60)
    print("PROCESSING FILE VERIFICATION")
    print("="*60)
    
    df_net_consensus   = validate_sort_data("network_hsdir_consensus_snapshots.csv")
    df_ring_events     = validate_sort_data("hsdir_ring_events.csv")
    df_track_consensus = validate_sort_data("tracked_hsdir_consensus_snapshots.csv")
    df_rot_net         = validate_sort_data("rotated_network_hsdir_consensus_snapshots.csv")
    df_rot_track       = validate_sort_data("rotated_tracked_hsdir_consensus_snapshots.csv")

    print("="*60)
    print("PROCESSING CAPACITY STABILITY ANALYSIS")
    print("="*60)
        
    if df_track_consensus is not None:
        onion_stability_analysis(df_track_consensus, context_label="tracked_targets")
        # onion_to_hsdir_uniformity_analysis(df_track_consensus, context_label="tracked_targets")
        # ip_sybil_concentration_analysis(df_track_consensus, context_label="tracked_targets")
        # family_id_to_ip_sybil_concentration_analysis(df_track_consensus, context_label="tracked_targets")
        # chronological_uptime_analysis(df_track_consensus, context_label="tracked_targets")
        # churn_reconnection_analysis(df_track_consensus, context_label="tracked_targets")
        # strange_churn_behaviour_analysis(df_track_consensus, df_ring_events, context_label="tracked_targets")
        # entity_drop_analysis(df_track_consensus, context_label="tracked_targets")
        

    print("="*60)
    print("ANALYSIS COMPLETED SUCCESSFULLY. ALL MATRIX ARTIFACTS EXPORTED.")
    print("="*60)
