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
        
    for col in df_processed.columns:
        if df_processed[col].dtype == 'object':
            df_processed[col] = df_processed[col].astype(str).replace(['nan', '0', '0.0','',' '], 'None')
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

# MAIN
if __name__ == "__main__":
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
    print("ANALYSIS COMPLETED SUCCESSFULLY. ALL MATRIX ARTIFACTS EXPORTED.")
    print("="*60)
