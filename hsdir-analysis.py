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


# ANALYSIS 1: NET-WIDE AND TRACKED (+ ROATED) STABILITY ANALYSIS (DAILY & WEEKLY VIEW)
def compute_stability(df_slice, context_label):
    os.makedirs("analysis-results/stability", exist_ok=True)
    computed_metrics = []
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    unique_days = [d for d in df_slice["date"].unique() if d not in ['', 'None']]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)
    
    for observation_day in unique_days_sorted:
        day_data = df_slice[df_slice["date"] == observation_day]
        total_unique_identities = day_data["fingerprint"].nunique()
        
        offline_indicators = (day_data["status"] != "ACTIVE_IN_RING") | \
                             (day_data.get("consecutive_hourly_absences", pd.Series(0, index=day_data.index)) > 0)
        total_offline_identities = day_data[offline_indicators]["fingerprint"].nunique()
        
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
    if df_summary_matrix.empty:
        print(f"[STABILITY-ERROR] Summary matrix empty for context: {context_label}")
        return None
        
    plt.figure(figsize=(14, 6))
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Active Directories"], marker='o', color='green', linewidth=2, label='Active HSDirs Count')
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Offline Attrition"], marker='o', color='red', linewidth=2, label='Offline HSDirs Count')

    for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix["Active Directories"]):
        plt.annotate(f"{y}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
    for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix["Offline Attrition"]):
        plt.annotate(f"{y}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)

    plt.title(f"Tor v3 Active HSDirs vs Attrition Churn ({context_label.replace('_', ' ').title()} - Daily Baseline)")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Unique Fingerprint Directory Count")
    plt.xticks(rotation=45)
    plt.legend(loc="upper right")
    plt.tight_layout()
    
    daily_fig_name = f"analysis-results/stability/{context_label}_hsdir_daily_stability_progression.png"
    plt.savefig(daily_fig_name)
    plt.close()

    df_weekly_prep = df_summary_matrix.copy()
    df_weekly_prep["DayNum"] = df_weekly_prep["Observation Date"].apply(get_day_integer)
    df_weekly_prep["WeekIndex"] = df_weekly_prep["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")
    
    df_weekly_matrix = df_weekly_prep.groupby("WeekIndex").agg({
        "Total Unique HSDirs": "mean",
        "Active Directories": "mean",
        "Offline Attrition": "mean"
    }).reset_index()
    
    df_weekly_matrix["Active Ratio (%)"] = (df_weekly_matrix["Active Directories"] / df_weekly_matrix["Total Unique HSDirs"] * 100).round(2)
    df_weekly_matrix["Offline Ratio (%)"] = (100.0 - df_weekly_matrix["Active Ratio (%)"]).round(2)
    
    df_weekly_matrix["Total Unique HSDirs"] = df_weekly_matrix["Total Unique HSDirs"].round(2)
    df_weekly_matrix["Active Directories"] = df_weekly_matrix["Active Directories"].round(2)
    df_weekly_matrix["Offline Attrition"] = df_weekly_matrix["Offline Attrition"].round(2)
    df_weekly_matrix.rename(columns={"WeekIndex": "Observation Week"}, inplace=True)

    if not df_weekly_matrix.empty:
        plt.figure(figsize=(10, 6))
        plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Active Directories"], marker='s', color='blue', linewidth=2, label='Weekly Avg Active HSDirs')
        plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Offline Attrition"], marker='d', color='orange', linewidth=2, label='Weekly Avg Offline Attrition')
        
        for x, y in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix["Active Directories"]):
            plt.annotate(f"{y}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
        for x, y in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix["Offline Attrition"]):
            plt.annotate(f"{y}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
            
        plt.title(f"Tor v3 Weekly Averaged HSDir Stability Profile ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline (Weeks)")
        plt.ylabel("Mean Identity Instance Volume")
        plt.legend(loc="upper right")
        plt.tight_layout()
        
        weekly_fig_name = f"analysis-results/stability/{context_label}_hsdir_weekly_stability_progression.png"
        plt.savefig(weekly_fig_name)
        plt.close()
        
        weekly_totals = {
            "Observation Week": "Overall Weekly Average",
            "Total Unique HSDirs": round(df_weekly_matrix["Total Unique HSDirs"].mean(), 2),
            "Active Directories": round(df_weekly_matrix["Active Directories"].mean(), 2),
            "Active Ratio (%)": round(df_weekly_matrix["Active Ratio (%)"].mean(), 2),
            "Offline Attrition": round(df_weekly_matrix["Offline Attrition"].mean(), 2),
            "Offline Ratio (%)": round(df_weekly_matrix["Offline Ratio (%)"].mean(), 2)
        }
        df_weekly_matrix_export = pd.concat([df_weekly_matrix, pd.DataFrame([weekly_totals])], ignore_index=True)
        df_weekly_matrix_export.to_csv(f"analysis-results/stability/{context_label}_hsdir_weekly_stability_matrix.csv", index=False)

    daily_totals = {
        "Observation Date": "Overall Average",
        "Total Unique HSDirs": round(df_summary_matrix["Total Unique HSDirs"].mean(), 2),
        "Active Directories": round(df_summary_matrix["Active Directories"].mean(), 2),
        "Active Ratio (%)": round(df_summary_matrix["Active Ratio (%)"].mean(), 2),
        "Offline Attrition": round(df_summary_matrix["Offline Attrition"].mean(), 2),
        "Offline Ratio (%)": round(df_summary_matrix["Offline Ratio (%)"].mean(), 2)
    }
    df_summary_matrix_export = pd.concat([df_summary_matrix, pd.DataFrame([daily_totals])], ignore_index=True)
    matrix_csv_name = f"analysis-results/stability/{context_label}_hsdir_daily_stability_matrix.csv"
    df_summary_matrix_export.to_csv(matrix_csv_name, index=False)
    
    print(f"[STABILITY-INFO] Daily and weekly analysis saved for context: '{context_label}'")
    return df_summary_matrix


def calculate_stability_matrices(df, context_label):
    print(f"=== PROCESSING {context_label.upper()} CONSENSUS RESILIENCE ===")
    if df is None or df.empty:
        print(f"[STABILITY-ERROR] Missing data for context '{context_label}'.")
        return None
    
    # 1. Run core baseline execution logic across complete contextual layout
    global_reporting_matrix = compute_stability(df, context_label)
    
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating target tracking metrics for service type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_stability(sub_population_df, segmented_label)
                
    return global_reporting_matrix


# ANALYSIS 2: TRACKED (+ ROATED) HRT DISTANCE BETWEEN ONION-TO-HSDIR ANALYSIS (DAILY & WEEKLY VIEW)
def compute_hrt_onion_to_hsdir(df_slice, context_label):
    os.makedirs("analysis-results/hrt_onion_to_hsdir", exist_ok=True)
    computed_metrics = []
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if "hsdir_to_onion_ring_position" not in df_slice.columns:
        print(f"[HRT-ERROR] Field 'hsdir_to_onion_ring_position' missing for context {context_label}")
        return None

    unique_days = [d for d in df_slice["date"].unique() if d not in ['', 'None']]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)
    
    for observation_day in unique_days_sorted:
        day_data = df_slice[df_slice["date"] == observation_day].copy()
        
        day_data["parsed_position"] = pd.to_numeric(day_data["hsdir_to_onion_ring_position"], errors='coerce')
        
        valid_data = day_data.dropna(subset=["parsed_position", "fingerprint"])
        valid_data = valid_data[valid_data["fingerprint"] != "None"]
        
        unique_nodes = valid_data.drop_duplicates(subset=["fingerprint"]).copy()
        
        total_unique_identities = unique_nodes["fingerprint"].nunique()
        if total_unique_identities == 0:
            continue
            
        close_count = unique_nodes[unique_nodes["parsed_position"] <= 33.33]["fingerprint"].nunique()
        separate_count = unique_nodes[(unique_nodes["parsed_position"] > 33.33) & (unique_nodes["parsed_position"] <= 66.66)]["fingerprint"].nunique()
        far_count = unique_nodes[unique_nodes["parsed_position"] > 66.66]["fingerprint"].nunique()
        
        ratio_close = (close_count / total_unique_identities * 100)
        ratio_separate = (separate_count / total_unique_identities * 100)
        ratio_far = (far_count / total_unique_identities * 100)
        
        computed_metrics.append({
            "Observation Date": observation_day,
            "Total Unique HSDirs": total_unique_identities,
            "Close Count": close_count,
            "Close Ratio (%)": round(ratio_close, 2),
            "Separate Count": separate_count,
            "Separate Ratio (%)": round(ratio_separate, 2),
            "Far Count": far_count,
            "Far Ratio (%)": round(ratio_far, 2)
        })

    df_summary_matrix = pd.DataFrame(computed_metrics)
    if df_summary_matrix.empty:
        print(f"[HRT-WARNING] No valid unique-identity quadrant metrics for context: {context_label}")
        return None
        
    plt.figure(figsize=(14, 6))
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Close Count"], marker='o', color='green', linewidth=2, label='Close (<=33.33%)')
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Separate Count"], marker='s', color='blue', linewidth=2, label='Separate (33.33% - 66.66%)')
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Far Count"], marker='d', color='red', linewidth=2, label='Far (>66.66%)')

    for metric_col in ["Close Count", "Separate Count", "Far Count"]:
        for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix[metric_col]):
            plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)

    plt.title(f"Tor v3 Unique HSDir Position Quadrant Drift ({context_label.replace('_', ' ').title()} - Daily)")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Unique Fingerprint Directory Count")
    plt.xticks(rotation=45)
    plt.legend(loc="upper right")
    plt.tight_layout()
    
    plt.savefig(f"analysis-results/hrt_onion_to_hsdir/{context_label}_hrt_daily_progression.png")
    plt.close()

    df_weekly_prep = df_summary_matrix.copy()
    df_weekly_prep["DayNum"] = df_weekly_prep["Observation Date"].apply(get_day_integer)
    df_weekly_prep["WeekIndex"] = df_weekly_prep["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")
    
    df_weekly_matrix = df_weekly_prep.groupby("WeekIndex").agg({
        "Total Unique HSDirs": "mean",
        "Close Count": "mean",
        "Separate Count": "mean",
        "Far Count": "mean"
    }).reset_index()
    
    df_weekly_matrix["Close Ratio (%)"] = (df_weekly_matrix["Close Count"] / df_weekly_matrix["Total Unique HSDirs"] * 100).round(2)
    df_weekly_matrix["Separate Ratio (%)"] = (df_weekly_matrix["Separate Count"] / df_weekly_matrix["Total Unique HSDirs"] * 100).round(2)
    df_weekly_matrix["Far Ratio (%)"] = (df_weekly_matrix["Far Count"] / df_weekly_matrix["Total Unique HSDirs"] * 100).round(2)
    
    df_weekly_matrix["Total Unique HSDirs"] = df_weekly_matrix["Total Unique HSDirs"].round(2)
    df_weekly_matrix["Close Count"] = df_weekly_matrix["Close Count"].round(2)
    df_weekly_matrix["Separate Count"] = df_weekly_matrix["Separate Count"].round(2)
    df_weekly_matrix["Far Count"] = df_weekly_matrix["Far Count"].round(2)
    df_weekly_matrix.rename(columns={"WeekIndex": "Observation Week"}, inplace=True)

    if not df_weekly_matrix.empty:
        plt.figure(figsize=(10, 6))
        plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Close Count"], marker='o', color='green', linewidth=2, label='Weekly Close Avg')
        plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Separate Count"], marker='s', color='blue', linewidth=2, label='Weekly Separate Avg')
        plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Far Count"], marker='d', color='red', linewidth=2, label='Weekly Far Avg')
        
        for metric_col in ["Close Count", "Separate Count", "Far Count"]:
            for x, y in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix[metric_col]):
                plt.annotate(f"{y}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
                
        plt.title(f"Tor v3 Weekly HRT Topology Profile ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline (Weeks)")
        plt.ylabel("Mean Unique Identity Quadrant Footprint")
        plt.legend(loc="upper right")
        plt.tight_layout()
        
        plt.savefig(f"analysis-results/hrt_onion_to_hsdir/{context_label}_hrt_weekly_progression.png")
        plt.close()
        
        weekly_totals = {
            "Observation Week": "Overall Weekly Average",
            "Total Unique HSDirs": round(df_weekly_matrix["Total Unique HSDirs"].mean(), 2),
            "Close Count": round(df_weekly_matrix["Close Count"].mean(), 2),
            "Close Ratio (%)": round(df_weekly_matrix["Close Ratio (%)"].mean(), 2),
            "Separate Count": round(df_weekly_matrix["Separate Count"].mean(), 2),
            "Separate Ratio (%)": round(df_weekly_matrix["Separate Ratio (%)"].mean(), 2),
            "Far Count": round(df_weekly_matrix["Far Count"].mean(), 2),
            "Far Ratio (%)": round(df_weekly_matrix["Far Ratio (%)"].mean(), 2)
        }
        df_weekly_matrix_export = pd.concat([df_weekly_matrix, pd.DataFrame([weekly_totals])], ignore_index=True)
        df_weekly_matrix_export.to_csv(f"analysis-results/hrt_onion_to_hsdir/{context_label}_hrt_weekly_matrix.csv", index=False)

    daily_totals = {
        "Observation Date": "Overall Average",
        "Total Unique HSDirs": round(df_summary_matrix["Total Unique HSDirs"].mean(), 2),
        "Close Count": round(df_summary_matrix["Close Count"].mean(), 2),
        "Close Ratio (%)": round(df_summary_matrix["Close Ratio (%)"].mean(), 2),
        "Separate Count": round(df_summary_matrix["Separate Count"].mean(), 2),
        "Separate Ratio (%)": round(df_summary_matrix["Separate Ratio (%)"].mean(), 2),
        "Far Count": round(df_summary_matrix["Far Count"].mean(), 2),
        "Far Ratio (%)": round(df_summary_matrix["Far Ratio (%)"].mean(), 2)
    }
    df_summary_matrix_export = pd.concat([df_summary_matrix, pd.DataFrame([daily_totals])], ignore_index=True)
    df_summary_matrix_export.to_csv(f"analysis-results/hrt_onion_to_hsdir/{context_label}_hrt_daily_matrix.csv", index=False)
    
    print(f"[HRT-INFO] Verified unique spatial analysis matrix written for: '{context_label}'")
    return df_summary_matrix


def calculate_hrt_onion_to_hsdir_distribution(df, context_label):
    print(f"=== PROCESSING {context_label.upper()} HRT DISTRIBUTION SCATTER ===")
    if df is None or df.empty:
        print(f"[HRT-ERROR] Missing layout arrays for context '{context_label}'.")
        return None
    
    global_reporting_matrix = compute_hrt_onion_to_hsdir(df, context_label)
    
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating spatial metrics for specific service type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_hrt_onion_to_hsdir(sub_population_df, segmented_label)
                
    return global_reporting_matrix


# ANALYSIS 3: HRT INDEX DISTRIBUTIONS FOR ONION SERVICES AND HSDIRS (DAILY & WEEKLY VIEW)
def compute_hrt_index_distributions(df_slice, context_label):
    os.makedirs("analysis-results/hrt_index_distributions", exist_ok=True)
    computed_metrics = []
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    has_onion_idx = "mapped_onion_index_100" in df_slice.columns #mapped_onion
    has_rot_onion_idx = "last_known_onion_index_100" in df_slice.columns #last_known_onion
    has_hsdir_idx = "hsdir_index_hrt_100" in df_slice.columns

    if (not has_onion_idx or not has_rot_onion_idx) and not has_hsdir_idx :
        print(f"[HRT-INDEX-ERROR] Neither onion's index nor HSDir's index found for {context_label}")
        return None

    unique_days = [d for d in df_slice["date"].unique() if d not in ['', 'None']]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)
    
    for observation_day in unique_days_sorted:
        day_data = df_slice[df_slice["date"] == observation_day].copy()
        
        hsdir_1st, hsdir_2nd, hsdir_3rd = 0, 0, 0
        hsdir_1st_ratio, hsdir_2nd_ratio, hsdir_3rd_ratio = 0.0, 0.0, 0.0
        total_unique_hsdirs = 0
        
        if has_hsdir_idx:
            day_data["parsed_hsdir_idx"] = pd.to_numeric(day_data["hsdir_index_hrt_100"], errors='coerce')
            valid_hsdir = day_data.dropna(subset=["parsed_hsdir_idx", "fingerprint"])
            valid_hsdir = valid_hsdir[valid_hsdir["fingerprint"] != "None"]
            unique_hsdirs = valid_hsdir.drop_duplicates(subset=["fingerprint"])
            
            total_unique_hsdirs = unique_hsdirs["fingerprint"].nunique()
            if total_unique_hsdirs > 0:
                hsdir_1st = unique_hsdirs[unique_hsdirs["parsed_hsdir_idx"] <= 33.33]["fingerprint"].nunique()
                hsdir_2nd = unique_hsdirs[(unique_hsdirs["parsed_hsdir_idx"] > 33.33) & (unique_hsdirs["parsed_hsdir_idx"] <= 66.66)]["fingerprint"].nunique()
                hsdir_3rd = unique_hsdirs[unique_hsdirs["parsed_hsdir_idx"] > 66.66]["fingerprint"].nunique()
                
                hsdir_1st_ratio = (hsdir_1st / total_unique_hsdirs * 100)
                hsdir_2nd_ratio = (hsdir_2nd / total_unique_hsdirs * 100)
                hsdir_3rd_ratio = (hsdir_3rd / total_unique_hsdirs * 100)

        onion_1st, onion_2nd, onion_3rd = 0, 0, 0
        onion_1st_ratio, onion_2nd_ratio, onion_3rd_ratio = 0.0, 0.0, 0.0
        total_unique_onions = 0
        
        onion_col = "mapped_onion" if "mapped_onion" in day_data.columns else ("last_known_onion" if "last_known_onion" in day_data.columns else "fingerprint")
        
        if (has_onion_idx or has_rot_onion_idx) and onion_col in day_data.columns:
            if has_onion_idx:
                day_data["parsed_onion_idx"] = pd.to_numeric(day_data["mapped_onion_index_100"], errors='coerce')
            else:
                day_data["parsed_onion_idx"] = pd.to_numeric(day_data["last_known_onion_index_100"], errors='coerce')
            valid_onion = day_data.dropna(subset=["parsed_onion_idx", onion_col])
            valid_onion = valid_onion[valid_onion[onion_col] != "None"]
            unique_onions = valid_onion.drop_duplicates(subset=[onion_col])
            
            total_unique_onions = unique_onions[onion_col].nunique()
            if total_unique_onions > 0:
                onion_1st = unique_onions[unique_onions["parsed_onion_idx"] <= 33.33][onion_col].nunique()
                onion_2nd = unique_onions[(unique_onions["parsed_onion_idx"] > 33.33) & (unique_onions["parsed_onion_idx"] <= 66.66)][onion_col].nunique()
                onion_3rd = unique_onions[unique_onions["parsed_onion_idx"] > 66.66][onion_col].nunique()
                
                onion_1st_ratio = (onion_1st / total_unique_onions * 100)
                onion_2nd_ratio = (onion_2nd / total_unique_onions * 100)
                onion_3rd_ratio = (onion_3rd / total_unique_onions * 100)

        computed_metrics.append({
            "Observation Date": observation_day,
            "Total Unique HSDirs": total_unique_hsdirs,
            "HSDir 1st tertile HRT Count": hsdir_1st,
            "HSDir 1st tertile HRT Ratio (%)": round(hsdir_1st_ratio, 2),
            "HSDir 2nd tertile HRT Count": hsdir_2nd,
            "HSDir 2nd tertile HRT Ratio (%)": round(hsdir_2nd_ratio, 2),
            "HSDir 3rd tertile HRT Count": hsdir_3rd,
            "HSDir 3rd tertile HRT Ratio (%)": round(hsdir_3rd_ratio, 2),
            "Total Unique Onions": total_unique_onions,
            "Onion 1st tertile HRT Count": onion_1st,
            "Onion 1st tertile HRT Ratio (%)": round(onion_1st_ratio, 2),
            "Onion 2nd tertile HRT Count": onion_2nd,
            "Onion 2nd tertile HRT Ratio (%)": round(onion_2nd_ratio, 2),
            "Onion 3rd tertile HRT Count": onion_3rd,
            "Onion 3rd tertile HRT Ratio (%)": round(onion_3rd_ratio, 2)
        })

    df_summary_matrix = pd.DataFrame(computed_metrics)
    if df_summary_matrix.empty:
        print(f"[HRT-INDEX-WARNING] No data metrics generated for context: {context_label}")
        return None
            
    if has_hsdir_idx and df_summary_matrix["Total Unique HSDirs"].sum() > 0:
        plt.figure(figsize=(14, 6))
        plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["HSDir 1st tertile HRT Count"], marker='o', color='green', linewidth=2, label='HSDir 1st Tertile (<=33.33%)')
        plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["HSDir 2nd tertile HRT Count"], marker='s', color='blue', linewidth=2, label='HSDir 2nd Tertile (33.33-66.66%)')
        plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["HSDir 3rd tertile HRT Count"], marker='d', color='red', linewidth=2, label='HSDir 3rd Tertile (>66.66%)')
        
        for metric_col in ["HSDir 1st tertile HRT Count", "HSDir 2nd tertile HRT Count", "HSDir 3rd tertile HRT Count"]:
            for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix[metric_col]):
                plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)

        plt.title(f"Tor v3 HSDir HRT Index Tertile Distribution ({context_label.replace('_', ' ').title()} - Daily)")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Unique HSDir Count")
        plt.xticks(rotation=45)
        plt.legend(loc="upper right")
        plt.tight_layout()
        plt.savefig(f"analysis-results/hrt_index_distributions/{context_label}_hsdir_index_daily_progression.png")
        plt.close()
    
    if (has_onion_idx or has_rot_onion_idx) and df_summary_matrix["Total Unique Onions"].sum() > 0:
        plt.figure(figsize=(14, 6))
        plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Onion 1st tertile HRT Count"], marker='o', color='green', linewidth=2, label='Onion 1st Tertile (<=33.33%)')
        plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Onion 2nd tertile HRT Count"], marker='s', color='blue', linewidth=2, label='Onion 2nd Tertile (33.33-66.66%)')
        plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Onion 3rd tertile HRT Count"], marker='d', color='red', linewidth=2, label='Onion 3rd Tertile (>66.66%)')

        for metric_col in ["Onion 1st tertile HRT Count", "Onion 2nd tertile HRT Count", "Onion 3rd tertile HRT Count"]:
            for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix[metric_col]):
                plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)

        plt.title(f"Tor v3 Onion Target HRT Index Tertile Distribution ({context_label.replace('_', ' ').title()} - Daily)")
        plt.xlabel("Observation Timeline")
        plt.ylabel("Unique Onion Target Count")
        plt.xticks(rotation=45)
        plt.legend(loc="upper right")
        plt.tight_layout()
        plt.savefig(f"analysis-results/hrt_index_distributions/{context_label}_onion_index_daily_progression.png")
        plt.close()

    df_weekly_prep = df_summary_matrix.copy()
    df_weekly_prep["DayNum"] = df_weekly_prep["Observation Date"].apply(get_day_integer)
    df_weekly_prep["WeekIndex"] = df_weekly_prep["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")
    
    df_weekly_matrix = df_weekly_prep.groupby("WeekIndex").agg({
        "Total Unique HSDirs": "mean",
        "HSDir 1st tertile HRT Count": "mean",
        "HSDir 2nd tertile HRT Count": "mean",
        "HSDir 3rd tertile HRT Count": "mean",
        "Total Unique Onions": "mean",
        "Onion 1st tertile HRT Count": "mean",
        "Onion 2nd tertile HRT Count": "mean",
        "Onion 3rd tertile HRT Count": "mean"
    }).reset_index()
    
    df_weekly_matrix["HSDir 1st tertile HRT Ratio (%)"] = (df_weekly_matrix["HSDir 1st tertile HRT Count"] / df_weekly_matrix["Total Unique HSDirs"] * 100).round(2)
    df_weekly_matrix["HSDir 2nd tertile HRT Ratio (%)"] = (df_weekly_matrix["HSDir 2nd tertile HRT Count"] / df_weekly_matrix["Total Unique HSDirs"] * 100).round(2)
    df_weekly_matrix["HSDir 3rd tertile HRT Ratio (%)"] = (df_weekly_matrix["HSDir 3rd tertile HRT Count"] / df_weekly_matrix["Total Unique HSDirs"] * 100).round(2)
    
    df_weekly_matrix["Onion 1st tertile HRT Ratio (%)"] = (df_weekly_matrix["Onion 1st tertile HRT Count"] / df_weekly_matrix["Total Unique Onions"] * 100).round(2)
    df_weekly_matrix["Onion 2nd tertile HRT Ratio (%)"] = (df_weekly_matrix["Onion 2nd tertile HRT Count"] / df_weekly_matrix["Total Unique Onions"] * 100).round(2)
    df_weekly_matrix["Onion 3rd tertile HRT Ratio (%)"] = (df_weekly_matrix["Onion 3rd tertile HRT Count"] / df_weekly_matrix["Total Unique Onions"] * 100).round(2)
    
    for col in ["Total Unique HSDirs", "HSDir 1st tertile HRT Count", "HSDir 2nd tertile HRT Count", "HSDir 3rd tertile HRT Count",
                "Total Unique Onions", "Onion 1st tertile HRT Count", "Onion 2nd tertile HRT Count", "Onion 3rd tertile HRT Count"]:
        df_weekly_matrix[col] = df_weekly_matrix[col].round(2)
        
    df_weekly_matrix.rename(columns={"WeekIndex": "Observation Week"}, inplace=True)

    if not df_weekly_matrix.empty:
        
        if has_hsdir_idx and df_weekly_matrix["Total Unique HSDirs"].sum() > 0:
            plt.figure(figsize=(10, 6))
            plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["HSDir 1st tertile HRT Count"], marker='o', color='green', linewidth=2, label='Weekly Avg HSDir 1st Tertile')
            plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["HSDir 2nd tertile HRT Count"], marker='s', color='blue', linewidth=2, label='Weekly Avg HSDir 2nd Tertile')
            plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["HSDir 3rd tertile HRT Count"], marker='d', color='red', linewidth=2, label='Weekly Avg HSDir 3rd Tertile')
            
            for metric_col in ["HSDir 1st tertile HRT Count", "HSDir 2nd tertile HRT Count", "HSDir 3rd tertile HRT Count"]:
                for x, y in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix[metric_col]):
                    plt.annotate(f"{y}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
            
            plt.title(f"Tor v3 Weekly Averaged HSDir HRT Index Distribution ({context_label.replace('_', ' ').title()})")
            plt.xlabel("Observation Timeline (Weeks)")
            plt.ylabel("Mean Identity Volume")
            plt.legend(loc="upper right")
            plt.tight_layout()
            plt.savefig(f"analysis-results/hrt_index_distributions/{context_label}_hsdir_index_weekly_progression.png")
            plt.close()
            
        if (has_onion_idx or has_rot_onion_idx) and df_weekly_matrix["Total Unique Onions"].sum() > 0:
            plt.figure(figsize=(10, 6))
            plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Onion 1st tertile HRT Count"], marker='o', color='green', linewidth=2, label='Weekly Avg Onion 1st Tertile')
            plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Onion 2nd tertile HRT Count"], marker='s', color='blue', linewidth=2, label='Weekly Avg Onion 2nd Tertile')
            plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Onion 3rd tertile HRT Count"], marker='d', color='red', linewidth=2, label='Weekly Avg Onion 3rd Tertile')
            
            for metric_col in ["Onion 1st tertile HRT Count", "Onion 2nd tertile HRT Count", "Onion 3rd tertile HRT Count"]:
                for x, y in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix[metric_col]):
                    plt.annotate(f"{y}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
            
            plt.title(f"Tor v3 Weekly Averaged Onion HRT Index Distribution ({context_label.replace('_', ' ').title()})")
            plt.xlabel("Observation Timeline (Weeks)")
            plt.ylabel("Mean Identity Volume")
            plt.legend(loc="upper right")
            plt.tight_layout()
            plt.savefig(f"analysis-results/hrt_index_distributions/{context_label}_onion_index_weekly_progression.png")
            plt.close()
        
        weekly_totals = {
            "Observation Week": "Overall Weekly Average",
            "Total Unique HSDirs": round(df_weekly_matrix["Total Unique HSDirs"].mean(), 2),
            "HSDir 1st tertile HRT Count": round(df_weekly_matrix["HSDir 1st tertile HRT Count"].mean(), 2),
            "HSDir 1st tertile HRT Ratio (%)": round(df_weekly_matrix["HSDir 1st tertile HRT Ratio (%)"].mean(), 2),
            "HSDir 2nd tertile HRT Count": round(df_weekly_matrix["HSDir 2nd tertile HRT Count"].mean(), 2),
            "HSDir 2nd tertile HRT Ratio (%)": round(df_weekly_matrix["HSDir 2nd tertile HRT Ratio (%)"].mean(), 2),
            "HSDir 3rd tertile HRT Count": round(df_weekly_matrix["HSDir 3rd tertile HRT Count"].mean(), 2),
            "HSDir 3rd tertile HRT Ratio (%)": round(df_weekly_matrix["HSDir 3rd tertile HRT Ratio (%)"].mean(), 2),
            "Total Unique Onions": round(df_weekly_matrix["Total Unique Onions"].mean(), 2),
            "Onion 1st tertile HRT Count": round(df_weekly_matrix["Onion 1st tertile HRT Count"].mean(), 2),
            "Onion 1st tertile HRT Ratio (%)": round(df_weekly_matrix["Onion 1st tertile HRT Ratio (%)"].mean(), 2),
            "Onion 2nd tertile HRT Count": round(df_weekly_matrix["Onion 2nd tertile HRT Count"].mean(), 2),
            "Onion 2nd tertile HRT Ratio (%)": round(df_weekly_matrix["Onion 2nd tertile HRT Ratio (%)"].mean(), 2),
            "Onion 3rd tertile HRT Count": round(df_weekly_matrix["Onion 3rd tertile HRT Count"].mean(), 2),
            "Onion 3rd tertile HRT Ratio (%)": round(df_weekly_matrix["Onion 3rd tertile HRT Ratio (%)"].mean(), 2)
        }
        df_weekly_matrix_export = pd.concat([df_weekly_matrix, pd.DataFrame([weekly_totals])], ignore_index=True)
        df_weekly_matrix_export.to_csv(f"analysis-results/hrt_index_distributions/{context_label}_hrt_index_weekly_matrix.csv", index=False)

    daily_totals = {
        "Observation Date": "Overall Average",
        "Total Unique HSDirs": round(df_summary_matrix["Total Unique HSDirs"].mean(), 2),
        "HSDir 1st tertile HRT Count": round(df_summary_matrix["HSDir 1st tertile HRT Count"].mean(), 2),
        "HSDir 1st tertile HRT Ratio (%)": round(df_summary_matrix["HSDir 1st tertile HRT Ratio (%)"].mean(), 2),
        "HSDir 2nd tertile HRT Count": round(df_summary_matrix["HSDir 2nd tertile HRT Count"].mean(), 2),
        "HSDir 2nd tertile HRT Ratio (%)": round(df_summary_matrix["HSDir 2nd tertile HRT Ratio (%)"].mean(), 2),
        "HSDir 3rd tertile HRT Count": round(df_summary_matrix["HSDir 3rd tertile HRT Count"].mean(), 2),
        "HSDir 3rd tertile HRT Ratio (%)": round(df_summary_matrix["HSDir 3rd tertile HRT Ratio (%)"].mean(), 2),
        "Total Unique Onions": round(df_summary_matrix["Total Unique Onions"].mean(), 2),
        "Onion 1st tertile HRT Count": round(df_summary_matrix["Onion 1st tertile HRT Count"].mean(), 2),
        "Onion 1st tertile HRT Ratio (%)": round(df_summary_matrix["Onion 1st tertile HRT Ratio (%)"].mean(), 2),
        "Onion 2nd tertile HRT Count": round(df_summary_matrix["Onion 2nd tertile HRT Count"].mean(), 2),
        "Onion 2nd tertile HRT Ratio (%)": round(df_summary_matrix["Onion 2nd tertile HRT Ratio (%)"].mean(), 2),
        "Onion 3rd tertile HRT Count": round(df_summary_matrix["Onion 3rd tertile HRT Count"].mean(), 2),
        "Onion 3rd tertile HRT Ratio (%)": round(df_summary_matrix["Onion 3rd tertile HRT Ratio (%)"].mean(), 2)
    }
    df_summary_matrix_export = pd.concat([df_summary_matrix, pd.DataFrame([daily_totals])], ignore_index=True)
    df_summary_matrix_export.to_csv(f"analysis-results/hrt_index_distributions/{context_label}_hrt_index_daily_matrix.csv", index=False)
    
    print(f"[HRT-INDEX-INFO] Metric matrices saved successfully for context: '{context_label}'")
    return df_summary_matrix


def calculate_hrt_index_distributions(df, context_label):
    print(f"=== PROCESSING {context_label.upper()} HRT INDEX TERTILE ANALYSIS ===")
    if df is None or df.empty:
        print(f"[HRT-INDEX-ERROR] Missing layout arrays for context '{context_label}'.")
        return None
    
    global_reporting_matrix = compute_hrt_index_distributions(df, context_label)
    
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating metrics for specific service type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_hrt_index_distributions(sub_population_df, segmented_label)
                
    return global_reporting_matrix


# ANALYSIS 4: SYBIL PATTERN FOR /16 AND /24 IP BLOCKS DISTRIBUTIONS - OPERATOR IDENTIFICATION (DAILY & WEEKLY VIEW)
def compute_sybil_pattern_ip_distribution(df_slice, context_label):
    os.makedirs("analysis-results/sybil_ip_blocks", exist_ok=True)
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice.empty or "ip_address" not in df_slice.columns or "fingerprint" not in df_slice.columns:
        print(f"[SYBIL-IP-ERROR] Missing critical structural fields for label: {context_label}")
        return None
            
    df_unstable = df_slice[df_slice["status"] != "ACTIVE_IN_RING"].copy()
    if df_unstable.empty:
        print(f"[SYBIL-IP-INFO] No entries found matching status != 'ACTIVE_IN_RING' for label: {context_label}")
        return None

    def extract_v4_subnet(ip, prefix_mask):
        if isinstance(ip, str) and ip != 'None' and '.' in ip:
            segments = ip.split('.')
            if prefix_mask == 24 and len(segments) >= 3:
                return f"{segments[0]}.{segments[1]}.{segments[2]}.0/24"
            elif prefix_mask == 16 and len(segments) >= 2:
                return f"{segments[0]}.{segments[1]}.0.0/16"
        return "Unknown"

    df_unstable["subnet_24"] = df_unstable["ip_address"].apply(lambda ip: extract_v4_subnet(ip, 24))
    df_unstable["subnet_16"] = df_unstable["ip_address"].apply(lambda ip: extract_v4_subnet(ip, 16))

    unique_days = [d for d in df_unstable["date"].unique() if d not in ['', 'None']]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    daily_records_24 = []
    daily_records_16 = []

    for obs_day in unique_days_sorted:
        day_data = df_unstable[df_unstable["date"] == obs_day]
        
        day_unique = day_data.drop_duplicates(subset=["fingerprint"])
        
        counts_24 = day_unique["subnet_24"].value_counts()
        for subnet, count in counts_24.items():
            if subnet != "Unknown":
                daily_records_24.append({
                    "Observation Date": obs_day,
                    "Subnet": subnet,
                    "Count": count
                })
                
        counts_16 = day_unique["subnet_16"].value_counts()
        for subnet, count in counts_16.items():
            if subnet != "Unknown":
                daily_records_16.append({
                    "Observation Date": obs_day,
                    "Subnet": subnet,
                    "Count": count
                })

    df_daily_matrix = pd.DataFrame()

    for mask, records, prefix_suffix in [(24, daily_records_24, "24"), (16, daily_records_16, "16")]:
        if not records:
            print(f"[SYBIL-IP-INFO] No valid records generated for /{mask} under label: {context_label}")
            continue
            
        df_raw_counts = pd.DataFrame(records)
        
        df_pivot = df_raw_counts.pivot(index="Observation Date", columns="Subnet", values="Count").fillna(0)
        df_pivot = df_pivot.reindex(unique_days_sorted).fillna(0)
        
        all_subnets = list(df_pivot.columns)
        if not all_subnets:
            continue
        
        df_daily_matrix = df_pivot.reset_index()
        daily_summary_baseline = {"Observation Date": "Overall Average"}
        for col in all_subnets:
            daily_summary_baseline[col] = round(df_daily_matrix[col].mean(), 2)
        
        df_daily_export = pd.concat([df_daily_matrix, pd.DataFrame([daily_summary_baseline])], ignore_index=True)
        df_daily_export.to_csv(f"analysis-results/sybil_ip_blocks/{context_label}_daily_matrix_{prefix_suffix}.csv", index=False)
        
        plt.figure(figsize=(13, 7))
        has_plotted_daily = False
        for col in all_subnets:
            plot_series = df_daily_matrix[col].apply(lambda x: x if x >= 5 else np.nan)
            
            if plot_series.notna().any():
                has_plotted_daily = True
                plt.plot(df_daily_matrix["Observation Date"], plot_series, marker='o', linewidth=2, label=col)
                for x_val, y_val in zip(df_daily_matrix["Observation Date"], df_daily_matrix[col]):
                    if y_val >= 5:
                        plt.annotate(f"{int(y_val)}", (x_val, y_val), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)
                        
        if has_plotted_daily:
            plt.title(f"Tor v3 Daily Co-located Unstable Directory Density (/{mask} Blocks)\nDataset Segment: {context_label.replace('_', ' ').title()}")
            plt.xlabel("Observation Timeline")
            plt.ylabel("Unique Fingerprint Target Count (status != ACTIVE_IN_RING)")
            plt.xticks(rotation=45)
            plt.legend(title="Network Blocks", loc="upper left", bbox_to_anchor=(1.02, 1))
            plt.tight_layout()
            plt.savefig(f"analysis-results/sybil_ip_blocks/{context_label}_daily_plot_{prefix_suffix}.png")
        plt.close()
        
        df_weekly_prep = df_daily_matrix.copy()
        df_weekly_prep["DayNum"] = df_weekly_prep["Observation Date"].apply(get_day_integer)
        df_weekly_prep["WeekIndex"] = df_weekly_prep["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")
        
        df_weekly_matrix = df_weekly_prep.groupby("WeekIndex")[all_subnets].mean().reset_index()
        df_weekly_matrix.rename(columns={"WeekIndex": "Observation Week"}, inplace=True)
        
        for col in all_subnets:
            df_weekly_matrix[col] = df_weekly_matrix[col].round(2)
            
        weekly_summary_baseline = {"Observation Week": "Overall Weekly Average"}
        for col in all_subnets:
            weekly_summary_baseline[col] = round(df_weekly_matrix[col].mean(), 2)
            
        df_weekly_export = pd.concat([df_weekly_matrix, pd.DataFrame([weekly_summary_baseline])], ignore_index=True)
        df_weekly_export.to_csv(f"analysis-results/sybil_ip_blocks/{context_label}_weekly_matrix_{prefix_suffix}.csv", index=False)
        
        plt.figure(figsize=(11, 6))
        has_plotted_weekly = False
        for col in all_subnets:
            plot_series_weekly = df_weekly_matrix[col].apply(lambda x: x if x >= 5 else np.nan)
            
            if plot_series_weekly.notna().any():
                has_plotted_weekly = True
                plt.plot(df_weekly_matrix["Observation Week"], plot_series_weekly, marker='s', linewidth=2, label=col)
                for x_val, y_val in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix[col]):
                    if y_val >= 5:
                        plt.annotate(f"{y_val}", (x_val, y_val), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)
                        
        if has_plotted_weekly:
            plt.title(f"Tor v3 Weekly Averaged Unstable Directory Density (/{mask} Blocks)\nDataset Segment: {context_label.replace('_', ' ').title()}")
            plt.xlabel("Observation Timeline (Weeks)")
            plt.ylabel("Mean Unique Fingerprint Count")
            plt.legend(title="Network Blocks", loc="upper left", bbox_to_anchor=(1.02, 1))
            plt.tight_layout()
            plt.savefig(f"analysis-results/sybil_ip_blocks/{context_label}_weekly_plot_{prefix_suffix}.png")
        plt.close()

    print(f"[SYBIL-IP-INFO] Metric matrices saved successfully for context: '{context_label}'")
    return df_daily_matrix if not df_daily_matrix.empty else None


def calculate_sybil_pattern_ip_distributions(df, context_label):
    print(f"PROCESSING {context_label.upper()} SYBIL PATTERN IP DISTRIBUTION ANALYSIS")
    if df is None or df.empty:
        print(f"[SYBIL-IP-ERROR] Missing layout arrays for context '{context_label}'.")
        return None
    
    global_reporting_matrix = compute_sybil_pattern_ip_distribution(df, context_label)
    
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating metrics for specific service type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_sybil_pattern_ip_distribution(sub_population_df, segmented_label)
                
    return global_reporting_matrix


# ANALYSIS 5: SYBIL PATTERN FOR DECLARED FAMILY IDS - OPERATOR IDENTIFICATION (DAILY & WEEKLY VIEW)
def compute_sybil_family_id_distribution(df_slice, context_label):
    os.makedirs("analysis-results/sybil_family_ids", exist_ok=True)
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice.empty or "declared_family_id" not in df_slice.columns or "fingerprint" not in df_slice.columns:
        print(f"[SYBIL-FAM-ERROR] Missing critical structural fields for label: {context_label}")
        return None
            
    df_unstable = df_slice[df_slice["status"] != "ACTIVE_IN_RING"].copy()
    if df_unstable.empty:
        print(f"[SYBIL-FAM-INFO] No entries found matching status != 'ACTIVE_IN_RING' for label: {context_label}")
        return None

    unique_days = [d for d in df_unstable["date"].unique() if d not in ['', 'None', None]]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    daily_records = []

    for obs_day in unique_days_sorted:
        day_data = df_unstable[df_unstable["date"] == obs_day]
        
        # Avoid double-counting the same node on the same day
        day_unique = day_data.drop_duplicates(subset=["fingerprint"])
        
        counts = day_unique["declared_family_id"].value_counts()
        for fam_id, count in counts.items():
            # Filter out empty, missing, or explicitly null representations
            if fam_id not in ["Unknown", "None", None, "", "nan", "null"]:
                daily_records.append({
                    "Observation Date": obs_day,
                    "Family ID": fam_id,
                    "Count": count
                })

    df_daily_matrix = pd.DataFrame()

    for records, prefix_suffix in [(daily_records, "declared_family_id")]:
        if not records:
            print(f"[SYBIL-FAM-INFO] No valid cryptographic family records generated under label: {context_label}")
            continue
            
        df_raw_counts = pd.DataFrame(records)
        
        df_pivot = df_raw_counts.pivot(index="Observation Date", columns="Family ID", values="Count").fillna(0)
        df_pivot = df_pivot.reindex(unique_days_sorted).fillna(0)
        
        all_families = list(df_pivot.columns)
        if not all_families:
            continue
        
        df_daily_matrix = df_pivot.reset_index()
        daily_summary_baseline = {"Observation Date": "Overall Average"}
        for col in all_families:
            daily_summary_baseline[col] = round(df_daily_matrix[col].mean(), 2)
        
        df_daily_export = pd.concat([df_daily_matrix, pd.DataFrame([daily_summary_baseline])], ignore_index=True)
        df_daily_export.to_csv(f"analysis-results/sybil_family_ids/{context_label}_daily_matrix_{prefix_suffix}.csv", index=False)
        
        plt.figure(figsize=(13, 7))
        has_plotted_daily = False
        for col in all_families:
            # Filter low density lines if desired; lowered threshold slightly to catch smaller explicit clusters
            plot_series = df_daily_matrix[col].apply(lambda x: x if x >= 5 else np.nan)
            
            if plot_series.notna().any():
                has_plotted_daily = True
                plt.plot(df_daily_matrix["Observation Date"], plot_series, marker='o', linewidth=2, label=col)
                for x_val, y_val in zip(df_daily_matrix["Observation Date"], df_daily_matrix[col]):
                    if y_val >= 5:
                        plt.annotate(f"{int(y_val)}", (x_val, y_val), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)
                        
        if has_plotted_daily:
            plt.title(f"Tor v3 Daily Co-located Unstable Directory Density (by Cryptographic Family ID)\nDataset Segment: {context_label.replace('_', ' ').title()}")
            plt.xlabel("Observation Timeline")
            plt.ylabel("Unique Fingerprint Target Count (status != ACTIVE_IN_RING)")
            plt.xticks(rotation=45)
            plt.legend(title="Family Identifiers", loc="upper left", bbox_to_anchor=(1.02, 1))
            plt.tight_layout()
            plt.savefig(f"analysis-results/sybil_family_ids/{context_label}_daily_plot_{prefix_suffix}.png")
        plt.close()
        
        df_weekly_prep = df_daily_matrix.copy()
        df_weekly_prep["DayNum"] = df_weekly_prep["Observation Date"].apply(get_day_integer)
        df_weekly_prep["WeekIndex"] = df_weekly_prep["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")
        
        df_weekly_matrix = df_weekly_prep.groupby("WeekIndex")[all_families].mean().reset_index()
        df_weekly_matrix.rename(columns={"WeekIndex": "Observation Week"}, inplace=True)
        
        for col in all_families:
            df_weekly_matrix[col] = df_weekly_matrix[col].round(2)
            
        weekly_summary_baseline = {"Observation Week": "Overall Weekly Average"}
        for col in all_families:
            weekly_summary_baseline[col] = round(df_weekly_matrix[col].mean(), 2)
            
        df_weekly_export = pd.concat([df_weekly_matrix, pd.DataFrame([weekly_summary_baseline])], ignore_index=True)
        df_weekly_export.to_csv(f"analysis-results/sybil_family_ids/{context_label}_weekly_matrix_{prefix_suffix}.csv", index=False)
        
        plt.figure(figsize=(11, 6))
        has_plotted_weekly = False
        for col in all_families:
            plot_series_weekly = df_weekly_matrix[col].apply(lambda x: x if x >= 5 else np.nan)
            
            if plot_series_weekly.notna().any():
                has_plotted_weekly = True
                plt.plot(df_weekly_matrix["Observation Week"], plot_series_weekly, marker='s', linewidth=2, label=col)
                for x_val, y_val in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix[col]):
                    if y_val >= 5:
                        plt.annotate(f"{y_val}", (x_val, y_val), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)
                        
        if has_plotted_weekly:
            plt.title(f"Tor v3 Weekly Averaged Unstable Directory Density (by Cryptographic Family ID)\nDataset Segment: {context_label.replace('_', ' ').title()}")
            plt.xlabel("Observation Timeline (Weeks)")
            plt.ylabel("Mean Unique Fingerprint Count")
            plt.legend(title="Family Identifiers", loc="upper left", bbox_to_anchor=(1.02, 1))
            plt.tight_layout()
            plt.savefig(f"analysis-results/sybil_family_ids/{context_label}_weekly_plot_{prefix_suffix}.png")
        plt.close()

    print(f"[SYBIL-FAM-INFO] Family ID matrices saved successfully for context: '{context_label}'")
    return df_daily_matrix if not df_daily_matrix.empty else None


def calculate_sybil_family_id_distributions(df, context_label):
    print(f"PROCESSING {context_label.upper()} SYBIL PATTERN FAMILY ID DISTRIBUTION ANALYSIS")
    if df is None or df.empty:
        print(f"[SYBIL-FAM-ERROR] Missing layout arrays for context '{context_label}'.")
        return None
    
    global_reporting_matrix = compute_sybil_family_id_distribution(df, context_label)
    
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating family metrics for specific service type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_sybil_family_id_distribution(sub_population_df, segmented_label)
                
    return global_reporting_matrix


# ANALYSIS 6: SYBIL PATTERN FOR DECLARED FAMILY LISTS - OPERATOR IDENTIFICATION (DAILY & WEEKLY VIEW)
def compute_sybil_family_list_distribution(df_slice, context_label):
    output_dir = "analysis-results/sybil_family_lists"
    os.makedirs(output_dir, exist_ok=True)
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if df_slice.empty or "declared_family" not in df_slice.columns or "fingerprint" not in df_slice.columns:
        print(f"[SYBIL-FAM-LIST-ERROR] Missing critical structural fields for label: {context_label}")
        return None
            
    df_unstable = df_slice[df_slice["status"] != "ACTIVE_IN_RING"].copy()
    if df_unstable.empty:
        print(f"[SYBIL-FAM-LIST-INFO] No entries found matching status != 'ACTIVE_IN_RING' for label: {context_label}")
        return None

    def generate_canonical_cluster_id(row):
        node_fp = row.get("fingerprint")
        family_list = row.get("declared_family")
        
        if not isinstance(family_list, (list, tuple, np.ndarray)):
            return "Unknown"
            
        combined_set = set()
        if isinstance(node_fp, str) and node_fp not in ["None", "Unknown", ""]:
            combined_set.add(node_fp.strip().upper())
            
        for item in family_list:
            if isinstance(item, str) and item not in ["None", "Unknown", ""]:
                combined_set.add(item.strip().upper())
                
        if not combined_set or (len(combined_set) == 1 and node_fp in combined_set):
            return "Unknown"
            
        return ",".join(sorted(list(combined_set)))

    df_unstable["canonical_family_group"] = df_unstable.apply(generate_canonical_cluster_id, axis=1)
    
    df_unstable = df_unstable[df_unstable["canonical_family_group"] != "Unknown"]
    if df_unstable.empty:
        print(f"[SYBIL-FAM-LIST-INFO] No active family relationships found for label: {context_label}")
        return None

    unique_clusters = df_unstable["canonical_family_group"].unique()
    operator_label_map = {}
    for idx, cluster in enumerate(unique_clusters):
        first_fp = cluster.split(',')[0]
        short_id = first_fp[:6] if len(first_fp) >= 6 else f"Grp{idx}"
        operator_label_map[cluster] = f"Operator_{short_id}"
        
    df_unstable["operator_identifier"] = df_unstable["canonical_family_group"].map(operator_label_map)

    unique_days = [d for d in df_unstable["date"].unique() if d not in ['', 'None', None]]
    unique_days_sorted = sorted(unique_days, key=get_day_integer)

    daily_records = []

    for obs_day in unique_days_sorted:
        day_data = df_unstable[df_unstable["date"] == obs_day]
        
        day_unique = day_data.drop_duplicates(subset=["fingerprint"])
        
        counts = day_unique["operator_identifier"].value_counts()
        for op_id, count in counts.items():
            daily_records.append({
                "Observation Date": obs_day,
                "Operator Identifier": op_id,
                "Count": count
            })

    df_daily_matrix = pd.DataFrame()

    for records, prefix_suffix in [(daily_records, "family_list")]:
        if not records:
            print(f"[SYBIL-FAM-LIST-INFO] No valid operator records generated under label: {context_label}")
            continue
            
        df_raw_counts = pd.DataFrame(records)
        
        df_pivot = df_raw_counts.pivot(index="Observation Date", columns="Operator Identifier", values="Count").fillna(0)
        df_pivot = df_pivot.reindex(unique_days_sorted).fillna(0)
        
        all_operators = list(df_pivot.columns)
        if not all_operators:
            continue
        
        df_daily_matrix = df_pivot.reset_index()
        daily_summary_baseline = {"Observation Date": "Overall Average"}
        for col in all_operators:
            daily_summary_baseline[col] = round(df_daily_matrix[col].mean(), 2)
        
        df_daily_export = pd.concat([df_daily_matrix, pd.DataFrame([daily_summary_baseline])], ignore_index=True)
        df_daily_export.to_csv(f"{output_dir}/{context_label}_daily_matrix_{prefix_suffix}.csv", index=False)
        
        plt.figure(figsize=(13, 7))
        has_plotted_daily = False
        for col in all_operators:
            plot_series = df_daily_matrix[col].apply(lambda x: x if x >= 5 else np.nan)
            
            if plot_series.notna().any():
                has_plotted_daily = True
                plt.plot(df_daily_matrix["Observation Date"], plot_series, marker='o', linewidth=2, label=col)
                for x_val, y_val in zip(df_daily_matrix["Observation Date"], df_daily_matrix[col]):
                    if y_val >= 5:
                        plt.annotate(f"{int(y_val)}", (x_val, y_val), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)
                        
        if has_plotted_daily:
            plt.title(f"Tor v3 Daily Co-located Unstable Directory Density (by Mutual Family List Mapping)\nDataset Segment: {context_label.replace('_', ' ').title()}")
            plt.xlabel("Observation Timeline")
            plt.ylabel("Unique Fingerprint Target Count (status != ACTIVE_IN_RING)")
            plt.xticks(rotation=45)
            plt.legend(title="Identified Entity Operators", loc="upper left", bbox_to_anchor=(1.02, 1))
            plt.tight_layout()
            plt.savefig(f"{output_dir}/{context_label}_daily_plot_{prefix_suffix}.png")
        plt.close()
        
        df_weekly_prep = df_daily_matrix.copy()
        df_weekly_prep["DayNum"] = df_weekly_prep["Observation Date"].apply(get_day_integer)
        df_weekly_prep["WeekIndex"] = df_weekly_prep["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")
        
        df_weekly_matrix = df_weekly_prep.groupby("WeekIndex")[all_operators].mean().reset_index()
        df_weekly_matrix.rename(columns={"WeekIndex": "Observation Week"}, inplace=True)
        
        for col in all_operators:
            df_weekly_matrix[col] = df_weekly_matrix[col].round(2)
            
        weekly_summary_baseline = {"Observation Week": "Overall Weekly Average"}
        for col in all_operators:
            weekly_summary_baseline[col] = round(df_weekly_matrix[col].mean(), 2)
            
        df_weekly_export = pd.concat([df_weekly_matrix, pd.DataFrame([weekly_summary_baseline])], ignore_index=True)
        df_weekly_export.to_csv(f"{output_dir}/{context_label}_weekly_matrix_{prefix_suffix}.csv", index=False)
        
        plt.figure(figsize=(11, 6))
        has_plotted_weekly = False
        for col in all_operators:
            plot_series_weekly = df_weekly_matrix[col].apply(lambda x: x if x >= 5 else np.nan)
            
            if plot_series_weekly.notna().any():
                has_plotted_weekly = True
                plt.plot(df_weekly_matrix["Observation Week"], plot_series_weekly, marker='s', linewidth=2, label=col)
                for x_val, y_val in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix[col]):
                    if y_val >= 5:
                        plt.annotate(f"{y_val}", (x_val, y_val), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)
                        
        if has_plotted_weekly:
            plt.title(f"Tor v3 Weekly Averaged Unstable Directory Density (by Mutual Family List Mapping)\nDataset Segment: {context_label.replace('_', ' ').title()}")
            plt.xlabel("Observation Timeline (Weeks)")
            plt.ylabel("Mean Unique Fingerprint Count")
            plt.legend(title="Identified Entity Operators", loc="upper left", bbox_to_anchor=(1.02, 1))
            plt.tight_layout()
            plt.savefig(f"{output_dir}/{context_label}_weekly_plot_{prefix_suffix}.png")
        plt.close()

    print(f"[SYBIL-FAM-LIST-INFO] Family List matrices saved successfully for context: '{context_label}'")
    return df_daily_matrix if not df_daily_matrix.empty else None


def calculate_sybil_family_list_distributions(df, context_label):
    print(f"PROCESSING {context_label.upper()} SYBIL PATTERN FAMILY LIST DISTRIBUTION ANALYSIS")
    if df is None or df.empty:
        print(f"[SYBIL-FAM-LIST-ERROR] Missing layout arrays for context '{context_label}'.")
        return None
    
    global_reporting_matrix = compute_sybil_family_list_distribution(df, context_label)
    
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating list-family metrics for specific service type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_sybil_family_list_distribution(sub_population_df, segmented_label)
                
    return global_reporting_matrix


# ANALYSIS 7: TRACKING CHRONOLOGICAL HSDIR SESSION UPTIME WINDOWS (DAILY & WEEKLY VIEW)
def compute_uptime_distribution(df_slice, context_label):
    os.makedirs("analysis-results/uptime_distributions", exist_ok=True)
    computed_metrics = []
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if "hour" not in df_slice.columns or "fingerprint" not in df_slice.columns:
        print(f"[UPTIME-ERROR] Missing critical structural fields ('hour' or 'fingerprint') for label: {context_label}")
        return None

    df_working = df_slice.copy()
    
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
    
    for observation_day in unique_days_sorted:
        day_data = df_working[df_working["date"] == observation_day]
        all_sessions_durations = []
        
        for fp in day_data["fingerprint"].unique():
            df_fp = day_data[day_data["fingerprint"] == fp].sort_values(by="hour_num")
            
            current_start = None
            current_last = None
            
            for _, row in df_fp.iterrows():
                is_active = row["status"] in ["ACTIVE_IN_RING", "None"]
                
                has_temporal_gap = (current_last is not None) and (row["hour_num"] - current_last > 2.5)
                
                if is_active and not has_temporal_gap:
                    if current_start is None:
                        current_start = row["hour_num"]
                    current_last = row["hour_num"]
                else:
                    if current_start is not None:
                        duration = max(1.0, current_last - current_start)
                        all_sessions_durations.append(duration)
                    
                    if is_active:
                        current_start = row["hour_num"]
                        current_last = row["hour_num"]
                    else:
                        current_start = None
                        current_last = None
            
            if current_start is not None:
                duration = max(1.0, current_last - current_start)
                all_sessions_durations.append(duration)
                
        total_unique_sessions = len(all_sessions_durations)
        if total_unique_sessions == 0:
            continue
            
        durations_arr = np.array(all_sessions_durations)
        
        mean_uptime   = durations_arr.mean()
        median_uptime = np.median(durations_arr)
        max_uptime    = durations_arr.max()
        min_uptime    = durations_arr.min()
        
        full_uptime_count  = np.sum(durations_arr >= 20)
        high_uptime_count  = np.sum((durations_arr >= 12) & (durations_arr < 20))
        mod_uptime_count   = np.sum((durations_arr >= 6) & (durations_arr < 12))
        low_uptime_count   = np.sum(durations_arr < 6)
        
        computed_metrics.append({
            "Observation Date": observation_day,
            "Total HSDir Sessions": total_unique_sessions,
            "Mean Session Uptime (hrs)": round(mean_uptime, 2),
            "Median Session Uptime (hrs)": round(median_uptime, 2),
            "Max Session Uptime (hrs)": int(max_uptime),
            "Min Session Uptime (hrs)": int(min_uptime),
            "Full Uptime (>=20h)": int(full_uptime_count),
            "High Uptime (12-19h)": int(high_uptime_count),
            "Moderate Uptime (6-11h)": int(mod_uptime_count),
            "Low Uptime (<6h)": int(low_uptime_count)
        })

    df_summary_matrix = pd.DataFrame(computed_metrics)
    if df_summary_matrix.empty:
        print(f"[UPTIME-WARNING] Summary matrix empty for context: {context_label}")
        return None

    plt.figure(figsize=(14, 6))
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Mean Session Uptime (hrs)"], marker='o', color='purple', linewidth=2, label='Mean Session Duration')
    plt.plot(df_summary_matrix["Observation Date"], df_summary_matrix["Median Session Uptime (hrs)"], marker='s', color='teal', linewidth=2, label='Median Session Duration')
    
    for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix["Mean Session Uptime (hrs)"]):
        plt.annotate(f"{y}h", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
    for x, y in zip(df_summary_matrix["Observation Date"], df_summary_matrix["Median Session Uptime (hrs)"]):
        plt.annotate(f"{y}h", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
        
    plt.title(f"Tor v3 HSDir Daily Active Session Lifespan Profile ({context_label.replace('_', ' ').title()})")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Session Duration Value (Hours)")
    plt.xticks(rotation=45)
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/uptime_distributions/{context_label}_hsdir_daily_uptime_distribution.png")
    plt.close()

    df_weekly_prep = df_summary_matrix.copy()
    df_weekly_prep["DayNum"] = df_weekly_prep["Observation Date"].apply(get_day_integer)
    df_weekly_prep["WeekIndex"] = df_weekly_prep["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")
    
    df_weekly_matrix = df_weekly_prep.groupby("WeekIndex").agg({
        "Total HSDir Sessions": "mean",
        "Mean Session Uptime (hrs)": "mean",
        "Median Session Uptime (hrs)": "mean",
        "Max Session Uptime (hrs)": "max",
        "Min Session Uptime (hrs)": "min",
        "Full Uptime (>=20h)": "mean",
        "High Uptime (12-19h)": "mean",
        "Moderate Uptime (6-11h)": "mean",
        "Low Uptime (<6h)": "mean"
    }).reset_index()
    
    for col in df_weekly_matrix.columns:
        if col != "WeekIndex":
            df_weekly_matrix[col] = df_weekly_matrix[col].round(2)
            
    df_weekly_matrix.rename(columns={"WeekIndex": "Observation Week"}, inplace=True)
    
    if not df_weekly_matrix.empty:
        plt.figure(figsize=(10, 6))
        plt.plot(df_weekly_matrix["Observation Week"], df_weekly_matrix["Mean Session Uptime (hrs)"], marker='^', color='indigo', linewidth=2, label='Weekly Mean Uptime Baseline')
        
        for x, y in zip(df_weekly_matrix["Observation Week"], df_weekly_matrix["Mean Session Uptime (hrs)"]):
            plt.annotate(f"{y}h", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
            
        plt.title(f"Tor v3 Weekly Averaged HSDir Active Session Profile ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline (Weeks)")
        plt.ylabel("Mean Hours Operational")
        plt.legend(loc="upper right")
        plt.tight_layout()
        plt.savefig(f"analysis-results/uptime_distributions/{context_label}_hsdir_weekly_uptime_distribution.png")
        plt.close()
        
        weekly_totals = {"Observation Week": "Overall Weekly Average"}
        for col in df_weekly_matrix.columns:
            if col != "Observation Week":
                weekly_totals[col] = round(df_weekly_matrix[col].mean(), 2)
        df_weekly_matrix_export = pd.concat([df_weekly_matrix, pd.DataFrame([weekly_totals])], ignore_index=True)
        df_weekly_matrix_export.to_csv(f"analysis-results/uptime_distributions/{context_label}_hsdir_weekly_uptime_matrix.csv", index=False)

    daily_totals = {"Observation Date": "Overall Average"}
    for col in df_summary_matrix.columns:
        if col != "Observation Date":
            daily_totals[col] = round(df_summary_matrix[col].mean(), 2)
            
    df_summary_matrix_export = pd.concat([df_summary_matrix, pd.DataFrame([daily_totals])], ignore_index=True)
    df_summary_matrix_export.to_csv(f"analysis-results/uptime_distributions/{context_label}_hsdir_daily_uptime_matrix.csv", index=False)
    
    print(f"[UPTIME-INFO] Complete operational active matrices saved under context label: '{context_label}'")
    return df_summary_matrix

def calculate_hsdir_uptime_distributions(df, context_label):

    print(f"PROCESSING {context_label.upper()} CONTINUOUS INSTANCE SESSION UPTIME DISCOVERY DISTRIBUTION ANALYSIS")
    if df is None or df.empty:
        print(f"[UPTIME-ERROR] Missing valid data slice matrix layouts for context '{context_label}'.")
        return None
        
    global_reporting_matrix = compute_uptime_distribution(df, context_label)

    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating continuous active session lifetimes for onion type: {service}")
            sub_population_df = df[df["service_type"] == service]
            if not sub_population_df.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_uptime_distribution(sub_population_df, segmented_label)
                
    return global_reporting_matrix


# ANALYSIS 8: TRACKING DAILY AND WEEKLY UNIQUE HSDIR RECONNECTION PATTERNS (CHURN)
def compute_churn_reconnect_distribution(df_slice, context_label):
    os.makedirs("analysis-results/str_behave_status", exist_ok=True)
    computed_daily_metrics = []
    
    def get_day_integer(day_str):
        if isinstance(day_str, str) and len(day_str.split()) > 1 and day_str.split()[1].isdigit():
            return int(day_str.split()[1])
        return 0

    if "hour" not in df_slice.columns or "fingerprint" not in df_slice.columns:
        print(f"[CHURN-ERROR] Missing critical fields ('hour' or 'fingerprint') for label: {context_label}")
        return None

    df_working = df_slice.copy()
    if df_working.empty:
        print(f"[CHURN-WARNING] Empty data slice received for label: {context_label}")
        return None
    
    if df_working["hour"].dtype == object or isinstance(df_working["hour"].iloc[0], str):
        if df_working["hour"].str.contains(":").any():
            hour_parts = df_working["hour"].str.split(":", expand=True)
            df_working["hour_num"] = hour_parts[0].astype(int) + hour_parts[1].astype(int) / 60.0
        else:
            df_working["hour_num"] = pd.to_numeric(df_working["hour"], errors='coerce').fillna(0)
    else:
        df_working["hour_num"] = df_working["hour"].astype(float)
        
    df_working["DayNum"] = df_working["date"].apply(get_day_integer)
    df_working["global_hour"] = (df_working["DayNum"] * 24) + df_working["hour_num"]
    df_working["WeekIndex"] = df_working["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")

    unique_days_sorted = sorted([d for d in df_working["date"].unique() if d not in ['', 'None']], key=get_day_integer)
    
    for observation_day in unique_days_sorted:
        day_data = df_working[df_working["date"] == observation_day]
        total_observed_fps = day_data["fingerprint"].nunique()
        total_daily_reconnections = 0
        
        for fp in day_data["fingerprint"].unique():
            df_fp = day_data[day_data["fingerprint"] == fp].sort_values(by="hour_num")
            
            reconnect_events = 0
            state = None
            
            for _, row in df_fp.iterrows():
                is_curr_active = row["status"] in ["ACTIVE_IN_RING", "None"] or pd.isna(row["status"])
                
                if state is None:
                    state = "ACTIVE" if is_curr_active else "OFFLINE"
                elif state == "ACTIVE" and not is_curr_active:
                    state = "OFFLINE"
                elif state == "OFFLINE" and is_curr_active:
                    reconnect_events = 1
                    state = "ACTIVE"
            
            total_daily_reconnections += reconnect_events
            
        reconnect_rate = (total_daily_reconnections / total_observed_fps * 100) if total_observed_fps > 0 else 0.0
        computed_daily_metrics.append({
            "Observation Date": observation_day,
            "Total Unique HSDirs": total_observed_fps,
            "Reconnecting Unique HSDirs": total_daily_reconnections,
            "Daily Reconnection Rate (%)": round(reconnect_rate, 2)
        })
        
    df_daily_churn = pd.DataFrame(computed_daily_metrics)

    if df_daily_churn.empty:
        print(f"[CHURN-ERROR] Summary matrix empty for context: {context_label}")
        return None

    plt.figure(figsize=(14, 6))
    plt.plot(df_daily_churn["Observation Date"], df_daily_churn["Reconnecting Unique HSDirs"], marker='o', color='purple', linewidth=2, label='Reconnecting HSDir Count')
    for x, y in zip(df_daily_churn["Observation Date"], df_daily_churn["Reconnecting Unique HSDirs"]):
        plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
        
    plt.title(f"Tor v3 Unique HSDir Daily Reconnection Counts ({context_label.replace('_', ' ').title()})")
    plt.xlabel("Observation Timeline")
    plt.ylabel("Relay Count (Total Reconnections)")
    plt.xticks(rotation=45)
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(f"analysis-results/str_behave_status/{context_label}_hsdir_daily_reconnect_count_churn.png")
    plt.close()

    weekly_unique_reconnects = {}
    unique_weeks_sorted = sorted([w for w in df_working["WeekIndex"].unique() if w not in ['', 'None']])
    
    for observation_week in unique_weeks_sorted:
        week_data = df_working[df_working["WeekIndex"] == observation_week]
        reconnecting_fps_count = 0
        
        for fp in week_data["fingerprint"].unique():
            df_fp = week_data[week_data["fingerprint"] == fp].sort_values(by="global_hour")
            
            reconnect_events = 0
            state = None
            
            for _, row in df_fp.iterrows():
                is_curr_active = row["status"] in ["ACTIVE_IN_RING", "None"] or pd.isna(row["status"])
                
                if state is None:
                    state = "ACTIVE" if is_curr_active else "OFFLINE"
                elif state == "ACTIVE" and not is_curr_active:
                    state = "OFFLINE"
                elif state == "OFFLINE" and is_curr_active:
                    reconnect_events = 1
                    state = "ACTIVE"

            if reconnect_events >= 1:
                reconnecting_fps_count += 1
                
        weekly_unique_reconnects[observation_week] = reconnecting_fps_count

    df_weekly_prep = df_daily_churn.copy()
    df_weekly_prep["DayNum"] = df_weekly_prep["Observation Date"].apply(get_day_integer)
    df_weekly_prep["WeekIndex"] = df_weekly_prep["DayNum"].apply(lambda d: f"Week {((d - 1) // 7) + 1}")
    
    df_weekly_churn = df_weekly_prep.groupby("WeekIndex").agg({
        "Total Unique HSDirs": "mean",
        "Reconnecting Unique HSDirs": "mean",
    }).reset_index()
    
    df_weekly_churn["Reconnecting Unique HSDirs Count"] = df_weekly_churn["WeekIndex"].map(weekly_unique_reconnects).fillna(0)
    df_weekly_churn["Total Unique HSDirs"] = df_weekly_churn["Total Unique HSDirs"].round(2)
    df_weekly_churn["Reconnecting Unique HSDirs"] = df_weekly_churn["Reconnecting Unique HSDirs"].round(2)
    df_weekly_churn["Reconnecting Unique HSDirs Count"] = df_weekly_churn["Reconnecting Unique HSDirs Count"].round(2)
    df_weekly_churn.rename(columns={"WeekIndex": "Observation Week"}, inplace=True)

    if not df_weekly_churn.empty:
        plt.figure(figsize=(10, 6))
        plt.plot(df_weekly_churn["Observation Week"], df_weekly_churn["Reconnecting Unique HSDirs Count"], marker='s', color='teal', linewidth=2, label='Weekly Reconnecting Count')
        for x, y in zip(df_weekly_churn["Observation Week"], df_weekly_churn["Reconnecting Unique HSDirs Count"]):
            plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
            
        plt.title(f"Tor v3 Unique HSDir Weekly Reconnection Counts ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline (Weeks)")
        plt.ylabel("Relay Count (Unique Fingerprints)")
        plt.legend(loc="upper right")
        plt.tight_layout()
        plt.savefig(f"analysis-results/str_behave_status/{context_label}_hsdir_weekly_reconnect_count_churn.png")
        plt.close()

        plt.figure(figsize=(10, 6))
        plt.plot(df_weekly_churn["Observation Week"], df_weekly_churn["Reconnecting Unique HSDirs"], marker='s', color='teal', linewidth=2, label='Weekly Reconnecting Count')
        for x, y in zip(df_weekly_churn["Observation Week"], df_weekly_churn["Reconnecting Unique HSDirs"]):
            plt.annotate(f"{int(y)}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9)
            
        plt.title(f"Tor v3 Unique HSDir Weekly Reconnection Avg ({context_label.replace('_', ' ').title()})")
        plt.xlabel("Observation Timeline (Weeks)")
        plt.ylabel("Relay Count (Unique Fingerprints)")
        plt.legend(loc="upper right")
        plt.tight_layout()
        plt.savefig(f"analysis-results/str_behave_status/{context_label}_hsdir_weekly_reconnect_progression_churn.png")
        plt.close()

    daily_totals = {"Observation Date": "Overall Average"}
    for col in df_daily_churn.columns:
        if col != "Observation Date":
            daily_totals[col] = round(df_daily_churn[col].mean(), 2)
    df_daily_export = pd.concat([df_daily_churn, pd.DataFrame([daily_totals])], ignore_index=True)
    df_daily_export.to_csv(f"analysis-results/str_behave_status/{context_label}_hsdir_daily_reconnect_matrix.csv", index=False)

    if not df_weekly_churn.empty:
        weekly_totals = {"Observation Week": "Overall Weekly Average"}
        for col in df_weekly_churn.columns:
            if col != "Observation Week":
                weekly_totals[col] = round(df_weekly_churn[col].mean(), 2)
        df_weekly_export = pd.concat([df_weekly_churn, pd.DataFrame([weekly_totals])], ignore_index=True)
        df_weekly_export.to_csv(f"analysis-results/str_behave_status/{context_label}_hsdir_weekly_reconnect_matrix.csv", index=False)

    print(f"[CHURN-INFO] Churn and Reconnection matrices saved under context label: '{context_label}'")
    return df_daily_churn


def calculate_hsdir_reconnect_churn_distributions(df, context_label):
    print(f"PROCESSING {context_label.upper()} CHURN-RECONNECT INTRA-WINDOW TRACKING ANALYSES")
    if df is None or df.empty:
        print(f"[CHURN-ERROR] Missing valid data slice matrix layouts for context '{context_label}'.")
        return None
        
    global_churn_matrix = compute_churn_reconnect_distribution(df, context_label)
    
    if context_label in ["tracked_targets", "rotated_tracked_targets"] and "service_type" in df.columns:
        unique_services = df["service_type"].dropna().unique()
        unique_services = [s for s in unique_services if s not in ['', 'nan', 'None']]
        
        for service in unique_services:
            print(f"    --> Isolating churn patterns for hidden service identifier: {service}")
            sub_pop = df[df["service_type"] == service]
            if not sub_pop.empty:
                segmented_label = f"{context_label}_{str(service).lower()}"
                compute_churn_reconnect_distribution(sub_pop, segmented_label)
                
    return global_churn_matrix


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
    for filename in target_files:
        if os.path.exists(filename):
            extract_telemetry(filename)
        else:
            print(f"[INGESTION-ERROR] Target telemetry log not found: {filename}")
            
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
    
    if df_net_consensus is not None:
        calculate_stability_matrices(df_net_consensus, context_label="network_wide")
        
    if df_track_consensus is not None:
        calculate_stability_matrices(df_track_consensus, context_label="tracked_targets")

    if df_rot_net is not None:
        calculate_stability_matrices(df_rot_net, context_label="rotated_network_wide")

    if df_rot_track is not None:
        calculate_stability_matrices(df_rot_track, context_label="rotated_tracked_targets")

    print("="*60)
    print("PROCESSING HRT ONION-TO-HSDIR SPATIAL DISTRIBUTION ANALYSIS")
    print("="*60)
    
    if df_track_consensus is not None:
        calculate_hrt_onion_to_hsdir_distribution(df_track_consensus, context_label="tracked_targets")
        
    if df_rot_track is not None:
        calculate_hrt_onion_to_hsdir_distribution(df_rot_track, context_label="rotated_tracked_targets")

    print("="*60)
    print("PROCESSING HRT INDEX TERTILE DISTRIBUTIONS")
    print("="*60)
    
    if df_net_consensus is not None:
        calculate_hrt_index_distributions(df_net_consensus, context_label="network_wide")
        
    if df_track_consensus is not None:
        calculate_hrt_index_distributions(df_track_consensus, context_label="tracked_targets")

    if df_rot_net is not None:
        calculate_hrt_index_distributions(df_rot_net, context_label="rotated_network_wide")

    if df_rot_track is not None:
        calculate_hrt_index_distributions(df_rot_track, context_label="rotated_tracked_targets")

    print("="*60)
    print("PROCESSING SYBIL IP PATTERN AND OPERATOR INFRASTRUCTURE ANALYSIS")
    print("="*60)
    
    if df_net_consensus is not None:
        calculate_sybil_pattern_ip_distributions(df_net_consensus, context_label="network_wide")
        
    if df_track_consensus is not None:
        calculate_sybil_pattern_ip_distributions(df_track_consensus, context_label="tracked_targets")

    if df_rot_net is not None:
        calculate_sybil_pattern_ip_distributions(df_rot_net, context_label="rotated_network_wide")

    if df_rot_track is not None:
        calculate_sybil_pattern_ip_distributions(df_rot_track, context_label="rotated_tracked_targets")

    print("="*60)
    print("PROCESSING SYBIL FAMILY ID PATTERN AND OPERATOR INFRASTRUCTURE ANALYSIS")
    print("="*60)
    
    if df_net_consensus is not None:
        calculate_sybil_family_id_distributions(df_net_consensus, context_label="network_wide")
        
    if df_track_consensus is not None:
        calculate_sybil_family_id_distributions(df_track_consensus, context_label="tracked_targets")

    if df_rot_net is not None:
        calculate_sybil_family_id_distributions(df_rot_net, context_label="rotated_network_wide")

    if df_rot_track is not None:
        calculate_sybil_family_id_distributions(df_rot_track, context_label="rotated_tracked_targets")

    print("="*60)
    print("PROCESSING SYBIL FAMILY LIST PATTERN AND OPERATOR INFRASTRUCTURE ANALYSIS")
    print("="*60)
    
    if df_net_consensus is not None:
        calculate_sybil_family_list_distributions(df_net_consensus, context_label="network_wide")
        
    if df_track_consensus is not None:
        calculate_sybil_family_list_distributions(df_track_consensus, context_label="tracked_targets")

    if df_rot_net is not None:
        calculate_sybil_family_list_distributions(df_rot_net, context_label="rotated_network_wide")

    if df_rot_track is not None:
        calculate_sybil_family_list_distributions(df_rot_track, context_label="rotated_tracked_targets")

    print("="*60)
    print("PROCESSING HSDIR UPTIME DISTRIBUTIONS ANALYSIS")
    print("="*60)
    
    if df_net_consensus is not None:
        calculate_hsdir_uptime_distributions(df_net_consensus, context_label="network_wide")
        
    if df_track_consensus is not None:
        calculate_hsdir_uptime_distributions(df_track_consensus, context_label="tracked_targets")

    print("="*60)
    print("PROCESSING CHURN RECONNECTION DISTRIBUTIONS ANALYSIS")
    print("="*60)
    
    if df_net_consensus is not None:
        calculate_hsdir_reconnect_churn_distributions(df_net_consensus, context_label="network_wide")
        
    if df_track_consensus is not None:
        calculate_hsdir_reconnect_churn_distributions(df_track_consensus, context_label="tracked_targets")

    print("="*60)
    print("ANALYSIS COMPLETED SUCCESSFULLY. ALL MATRIX ARTIFACTS EXPORTED.")
    print("="*60)
