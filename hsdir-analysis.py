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
#

import os
import ast
import json
import ipaddress
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
from scipy.stats import kstest
from scipy.cluster.hierarchy import linkage, leaves_list
from scipy.spatial.distance import squareform

target_files = [
    "network_hsdir_consensus_snapshots.jsonl",
    "hsdir_ring_events.jsonl",
    "tracked_hsdir_consensus_snapshots.jsonl",
    "rotated_network_hsdir_consensus_snapshots.jsonl",
    "rotated_tracked_hsdir_consensus_snapshots.jsonl"
]

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

    if "declared_family_id" in df_processed.columns:
        df_processed["declared_family_id"] = df_processed["declared_family_id"].astype(str).replace(['nan', 'NaN', 'None', '0', '0.0', '', ' '], 'None')
        df_processed["declared_family_id"] = df_processed["declared_family_id"].fillna('None')
    else:
        df_processed["declared_family_id"] = 'None'

    # List-valued fields that must be stored/reloaded as real Python lists,
    # not stringified. declared_family and flags share identical handling:
    # parse to a list here, skip the generic astype(str) pass below, and get
    # reconstructed from CSV by the matching block in validate_sort_data.
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
            return [val_stripped]
        return []

    _LIST_VALUED_COLUMNS = ("declared_family", "flags")
    for _list_col in _LIST_VALUED_COLUMNS:
        if _list_col in df_processed.columns:
            df_processed[_list_col] = df_processed[_list_col].apply(clean_initial_family)
        elif _list_col == "declared_family":
            # declared_family is guaranteed downstream; flags is only created
            # if the source had it (consensus data), never fabricated empty.
            df_processed[_list_col] = [[] for _ in range(len(df_processed))]

    for col in df_processed.columns:
        if col in _LIST_VALUED_COLUMNS:
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

def validate_sort_data(csv_path):
    if not os.path.exists(csv_path):
        print(f"[VERIFY-ERROR] Target file unavailable: {csv_path}")
        return None
    
    df = pd.read_csv(csv_path, low_memory=False)
    
    for col in df.columns:
        if col in ['ip_address', 'fingerprint', 'status', 'onion_address', 'action', 'reason', 'declared_family_id', 'timestamp_dropped', 'service_type']:
            df[col] = df[col].fillna('None').astype(str)
            df[col] = df[col].replace(['nan', 'NaN', 'None', ''], 'None')
            
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

    # Reconstruct the same list-valued columns extract_telemetry parsed
    # (declared_family, flags) back into real lists after the CSV round-trip.
    for _list_col in ("declared_family", "flags"):
        if _list_col in df.columns:
            df[_list_col] = df[_list_col].apply(parse_csv_family_list)
        elif _list_col == "declared_family":
            df[_list_col] = [[] for _ in range(len(df))]

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


# ---------------------------------------------------------------------------
# Sybil distribution analysis (family-id and IP based)
# ---------------------------------------------------------------------------

def _chronological_days(df):
    """
    Return the distinct values of df['date'] ordered chronologically.
    Post-validate_sort_data(), 'date' holds sequential "Day N" labels, so we
    sort on the numeric N rather than lexicographically (which would put
    "Day 10" before "Day 2"). Any value that doesn't match "Day N" is kept
    but sorted to the end, so nothing is silently dropped.
    """
    days = list(pd.unique(df['date']))

    def day_key(d):
        try:
            return int(str(d).split(' ')[1])
        except (IndexError, ValueError):
            return float('inf')

    return sorted(days, key=day_key)


def _plot_sybil_series(df, group_col, top_group_ids, output_path, metric_label):
    """
    Save one figure with, per entry in `top_group_ids`, a solid line for its
    absolute per-day unique-fingerprint count (left axis) and a dashed line
    for its per-day network share (right, twin axis) -- both in the same
    color per group so the two series are easy to pair up visually.

    Network share for a given day is that group's unique-fingerprint count
    on that day divided by the unique-fingerprint count across the WHOLE
    `df` (all groups, including the excluded "None"/ungrouped bucket) on
    that day -- i.e. the daily counterpart of the overall network-share
    figure printed to the terminal, so both numbers describe "share of the
    total observed network" rather than "share among only the top groups".
    """
    days = _chronological_days(df)

    daily_group_counts = (
        df[df[group_col].isin(top_group_ids)]
        .groupby(['date', group_col])['fingerprint']
        .nunique()
    )
    daily_totals = df.groupby('date')['fingerprint'].nunique()

    fig, ax1 = plt.subplots(figsize=(11, 6))
    ax2 = ax1.twinx()
    colors = plt.cm.tab10.colors

    for i, group_id in enumerate(top_group_ids):
        color = colors[i % len(colors)]

        counts = [daily_group_counts.get((day, group_id), 0) for day in days]
        totals = [daily_totals.get(day, 0) for day in days]
        # Days with zero total nodes (shouldn't normally happen) or zero
        # nodes for this group both yield a share of 0, never a ZeroDivisionError.
        shares = [(c / t) if t else 0.0 for c, t in zip(counts, totals)]

        legend_id = str(group_id)
        if len(legend_id) > 18:
            legend_id = legend_id[:15] + "..."

        ax1.plot(days, counts, linestyle='-', marker='o', color=color,
                  label=f"{legend_id} (count)")
        ax2.plot(days, shares, linestyle='--', marker='s', color=color,
                  label=f"{legend_id} (share)")

    ax1.set_xlabel("Day")
    ax1.set_ylabel(f"Unique count, solid ({metric_label})")
    ax2.set_ylabel("Network share, dashed")
    ax1.tick_params(axis='x', rotation=45)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=8,
               loc='upper left', bbox_to_anchor=(1.1, 1))

    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _sybil_top5_report_and_plot(df, group_col, label, output_path, metric_label=None,
                                 exclude_none_bucket=True):
    """
    Shared logic for both deliverables: rank `group_col` values by unique
    -fingerprint count, print the top 5 (full id + absolute count + overall
    network share), and save the per-day count/share figure.

    The "None" bucket (declared_family_id == 'None' means no declared
    family -- i.e. a non-sybil singleton node; ip_address/subnet == 'None'
    means missing/unusable address data) is excluded from the ranking by
    default: for family IDs specifically, singleton nodes would otherwise
    dominate every ranking and defeat the point of a sybil-cluster view.
    Set exclude_none_bucket=False to include it if that's ever useful.
    """
    if metric_label is None:
        metric_label = group_col

    total_unique = df['fingerprint'].nunique()
    scoped = df[df[group_col] != 'None'] if exclude_none_bucket else df

    if scoped.empty:
        print(f"[SYBIL-INFO] No usable '{metric_label}' groups for {label}; skipping.")
        return

    group_counts = scoped.groupby(group_col)['fingerprint'].nunique().sort_values(ascending=False)
    top_groups = group_counts.head(5)

    print(f"\n[SYBIL] Top {metric_label} groups — {label}")
    for group_id, count in top_groups.items():
        share = (count / total_unique) if total_unique else 0.0
        print(f"    {group_id}: {count} unique nodes  (network share: {share:.4%})")

    _plot_sybil_series(
        df, group_col, list(top_groups.index), output_path,
        metric_label=metric_label,
    )


def family_id_sybil_distributions(df, context_label):
    """
    Rank declared_family_id groups by unique-fingerprint count for `df`
    (network-wide or tracked), print the top 5 with their network share, and
    save a per-day count/share figure to
    analysis-results/sybil/family-id/{context_label}_family_id_sybil.png.
    For the tracked context, the analysis is repeated once per service_type,
    saved as {service_type_lower}_family_id_sybil.png.
    """
    base_dir = os.path.join("analysis-results", "sybil", "family-id")

    _sybil_top5_report_and_plot(
        df, 'declared_family_id', context_label,
        os.path.join(base_dir, f"{context_label}_family_id_sybil.png"),
        metric_label="declared_family_id",
    )

    if context_label == "tracked" and 'service_type' in df.columns:
        service_types = sorted(t for t in df['service_type'].dropna().unique() if t not in ('None', ''))
        for service_type in service_types:
            subset = df[df['service_type'] == service_type]
            _sybil_top5_report_and_plot(
                subset, 'declared_family_id', f"tracked — {service_type}",
                os.path.join(base_dir, f"{service_type.lower()}_family_id_sybil.png"),
                metric_label="declared_family_id",
            )


def _to_24_subnet(ip_str):
    """
    Derive the /24 subnet string for an IPv4 address (e.g. '192.168.2.12'
    -> '192.168.2.0/24') using the standard ipaddress module. Missing,
    'None', or malformed/non-IPv4 address text is normalized to the
    literal string 'None' so it's excluded the same way a missing
    declared_family_id is elsewhere in this file.
    """
    if ip_str in (None, 'None', ''):
        return 'None'
    try:
        return str(ipaddress.ip_network(f"{ip_str}/24", strict=False))
    except ValueError:
        return 'None'


def ip_sybil_distributions(df, context_label):
    """
    Like family_id_sybil_distributions, but groups by IP identity in two
    variants: exact ip_address and derived /24 subnet. Saved under
    analysis-results/sybil/ip_sybil/ as
    {context_label}_ip_sybil.png / {context_label}_ip_24_subnet_sybil.png,
    with per-service_type variants for the tracked context.
    """
    base_dir = os.path.join("analysis-results", "sybil", "ip_sybil")

    working = df.copy()
    working['_ip_24_subnet'] = working['ip_address'].apply(_to_24_subnet)

    def run_both_variants(sub_df, label, filename_prefix):
        _sybil_top5_report_and_plot(
            sub_df, 'ip_address', label,
            os.path.join(base_dir, f"{filename_prefix}_ip_sybil.png"),
            metric_label="ip_address",
        )
        _sybil_top5_report_and_plot(
            sub_df, '_ip_24_subnet', label,
            os.path.join(base_dir, f"{filename_prefix}_ip_24_subnet_sybil.png"),
            metric_label="/24 subnet",
        )

    run_both_variants(working, context_label, context_label)

    if context_label == "tracked" and 'service_type' in working.columns:
        service_types = sorted(t for t in working['service_type'].dropna().unique() if t not in ('None', ''))
        for service_type in service_types:
            subset = working[working['service_type'] == service_type]
            run_both_variants(subset, f"tracked — {service_type}", service_type.lower())


def service_type_overlap_diagnostic(df, context_label):
    """
    Diagnostic explaining why per-service_type unique-fingerprint counts
    don't sum to the overall tracked count: each tracked row is a
    (relay, mapped_onion) pairing, so one relay's fingerprint can appear
    under several service_types and is counted once per subset it touches but
    once overall, making the subsets overlap.

    Groups by fingerprint, collects the set of service_types each was seen
    under, reports how many relays touch one vs 2+ types, and breaks that
    down by exact combination. Only meaningful where service_type exists
    (skipped for network-wide data). Prints the summary and saves the full
    breakdown to
    analysis-results/sybil/{context_label}_service_type_overlap.csv.
    Returns the breakdown DataFrame (None if service_type is absent).
    """

    df = df[df["consecutive_hourly_absences"].isna() | (df["consecutive_hourly_absences"] <= 1)]

    if 'service_type' not in df.columns:
        print(f"[OVERLAP-INFO] '{context_label}' has no service_type column; skipping diagnostic.")
        return None

    fp_service_sets = (
        df.groupby('fingerprint')['service_type']
        .apply(lambda s: frozenset(t for t in s.unique() if t not in ('None', '')))
    )
    fp_service_sets = fp_service_sets[fp_service_sets.apply(len) > 0]

    total_relays = len(fp_service_sets)
    if total_relays == 0:
        print(f"[OVERLAP-INFO] No relays with a usable service_type for {context_label}; skipping diagnostic.")
        return None

    overlap_mask = fp_service_sets.apply(len) > 1
    overlap_relays = int(overlap_mask.sum())

    print(f"\n[OVERLAP] service_type overlap — {context_label}")
    print(f"    total distinct relays (any service_type): {total_relays}")
    print(f"    relays touching 2+ service_types: {overlap_relays} "
          f"({overlap_relays / total_relays:.2%} of {total_relays})")

    combo_counts = fp_service_sets.value_counts()
    combo_rows = [
        {
            'service_type_combination': " + ".join(sorted(combo)),
            'num_service_types': len(combo),
            'relay_count': count,
        }
        for combo, count in combo_counts.items()
    ]
    combo_df = pd.DataFrame(combo_rows).sort_values(
        ['num_service_types', 'relay_count'], ascending=[False, False]
    ).reset_index(drop=True)

    print("    breakdown by exact combination:")
    for _, row in combo_df.iterrows():
        print(f"      {row['service_type_combination']}: {row['relay_count']} relays")

    out_dir = os.path.join("analysis-results", "sybil")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{context_label}_service_type_overlap.csv")
    combo_df.to_csv(out_path, index=False)
    print(f"    --> Saved diagnostic table: {out_path}")

    return combo_df


# ---------------------------------------------------------------------------
# Entity-drop distribution analysis (family-id and IP based)
# ---------------------------------------------------------------------------

def _chronological_snapshots(df):
    """
    Return the distinct (date, hour) pairs in `df`, in chronological order:
    by "Day N" number first, then by the literal "HH:MM" hour string within
    each day (which sorts correctly as-is since it's always zero-padded
    24h time). Snapshot cadence is whatever the data actually has -- e.g.
    every 1h in one file, every 2h in another -- so this makes no
    assumption of a fixed interval or a fixed 24-snapshot day.
    """
    pairs = df[['date', 'hour']].drop_duplicates()

    def day_key(d):
        try:
            return int(str(d).split(' ')[1])
        except (IndexError, ValueError):
            return float('inf')

    pairs = pairs.assign(_day_key=pairs['date'].map(day_key))
    pairs = pairs.sort_values(['_day_key', 'hour'])
    return list(zip(pairs['date'], pairs['hour']))


def _classify_drops(df, group_col, exclude_none_bucket=True):
    """
    For every (date, hour) snapshot, classify each `group_col` group (e.g.
    declared_family_id, ip_address, or a /24 subnet) present in that
    snapshot as a 'partial' drop (>=1 ACTIVE_IN_RING member AND >=1
    non-ACTIVE_IN_RING member that snapshot), a 'total' drop (0 active
    members, all present members inactive), or no drop at all (0 inactive
    members -- excluded entirely, since a family with no inactive members
    can't be experiencing any kind of drop).

    The "None"/ungrouped bucket is excluded by default for the same
    reason as in the sybil-distribution functions: a singleton node with
    no declared family (or an unusable IP) can't exhibit a partial-vs
    -total *operator* drop pattern. Set exclude_none_bucket=False to
    include it.

    Returns a long DataFrame, one row per (date, hour, group_col value)
    that has a drop, with columns ['date', 'hour', group_col,
    'total_members', 'drop_type'] -- 'total_members' is that group's
    unique-fingerprint count in that snapshot (active + inactive).
    """
    scoped = df[df[group_col] != 'None'] if exclude_none_bucket else df

    total_counts = (
        scoped.groupby(['date', 'hour', group_col])['fingerprint'].nunique()
    )
    active_counts = (
        scoped[scoped['status'] == 'ACTIVE_IN_RING']
        .groupby(['date', 'hour', group_col])['fingerprint'].nunique()
    )

    summary = total_counts.rename('total_members').to_frame()
    summary['active_members'] = active_counts.reindex(summary.index, fill_value=0)
    summary['inactive_members'] = summary['total_members'] - summary['active_members']

    summary = summary[summary['inactive_members'] > 0].copy()
    summary['drop_type'] = np.where(summary['active_members'] > 0, 'partial', 'total')

    return summary.reset_index()[['date', 'hour', group_col, 'total_members', 'drop_type']]


def _print_top5_drop_counts(drop_df, group_col, label, metric_label):
    """
    Item 4 of the entity-drops spec: across the whole (already
    service-type-scoped, if applicable) DataFrame, count how many distinct
    (date, hour) snapshots each group registered a partial drop in, and
    separately a total drop in, and print the top 5 for each -- full group
    id, drop type, absolute snapshot count.
    """
    for drop_type, drop_name in (('partial', 'partial-drop'), ('total', 'total-drop')):
        subset = drop_df[drop_df['drop_type'] == drop_type]
        if subset.empty:
            print(f"[DROPS-INFO] No {drop_name} snapshots found for {metric_label} — {label}.")
            continue

        snapshot_counts = subset.groupby(group_col).size().sort_values(ascending=False)
        top5 = snapshot_counts.head(5)

        print(f"\n[DROPS] Top {metric_label} groups by {drop_name} snapshot count — {label}")
        for group_id, count in top5.items():
            print(f"    {group_id}: {drop_name}, {count} snapshot(s)")


def _plot_entity_drops_series(df, drop_df, output_path):
    """
    Save one figure with 4 series over the chronological (date, hour)
    sequence: partial-drop count (solid blue), total-drop count (solid red),
    partial-drop network share (dashed blue), total-drop network share
    (dashed red).

    "Count" is the number of distinct groups (families / IPs / /24 subnets)
    registering that drop type in a snapshot -- a count of drop events, not a
    sum of affected nodes. "Share" divides the summed total_members of all
    groups with that drop type by the snapshot's unique-fingerprint count, so
    it tracks what fraction of the node population sits in a dropped group and
    can move independently of the event count.

    X-axis ticks show one "Day N" label per day (at that day's first
    snapshot), keeping hour-to-hour spikes visible without an unreadable wall
    of labels.
    """
    snapshots = _chronological_snapshots(df)
    snapshot_totals = df.groupby(['date', 'hour'])['fingerprint'].nunique()

    # Count of drop EVENTS (how many groups registered each drop type),
    # used for the solid "count" lines.
    partial_event_counts = (
        drop_df[drop_df['drop_type'] == 'partial']
        .groupby(['date', 'hour']).size()
    )
    total_event_counts = (
        drop_df[drop_df['drop_type'] == 'total']
        .groupby(['date', 'hour']).size()
    )
    # Sum of affected NODES (active + inactive members of dropped groups),
    # used only for the dashed "share" lines.
    partial_node_sums = (
        drop_df[drop_df['drop_type'] == 'partial']
        .groupby(['date', 'hour'])['total_members'].sum()
    )
    total_node_sums = (
        drop_df[drop_df['drop_type'] == 'total']
        .groupby(['date', 'hour'])['total_members'].sum()
    )

    x = list(range(len(snapshots)))
    partial_counts, total_counts, partial_shares, total_shares = [], [], [], []
    for snap in snapshots:
        denom = snapshot_totals.get(snap, 0)
        partial_counts.append(partial_event_counts.get(snap, 0))
        total_counts.append(total_event_counts.get(snap, 0))
        p_nodes = partial_node_sums.get(snap, 0)
        t_nodes = total_node_sums.get(snap, 0)
        # Zero total-network denominator (or zero drop that snapshot)
        # yields a share of 0, never a ZeroDivisionError.
        partial_shares.append((p_nodes / denom) if denom else 0.0)
        total_shares.append((t_nodes / denom) if denom else 0.0)

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()

    ax1.plot(x, partial_counts, linestyle='-', color='blue', marker='o', label='Partial drop (count)')
    ax1.plot(x, total_counts, linestyle='-', color='red', marker='o', label='Total drop (count)')
    ax2.plot(x, partial_shares, linestyle='--', color='blue', marker='s', label='Partial drop (share)')
    ax2.plot(x, total_shares, linestyle='--', color='red', marker='s', label='Total drop (share)')

    day_first_index = {}
    for i, (date, _hour) in enumerate(snapshots):
        if date not in day_first_index:
            day_first_index[date] = i
    ax1.set_xticks(list(day_first_index.values()))
    ax1.set_xticklabels(list(day_first_index.keys()), rotation=45)

    ax1.set_xlabel("Day")
    ax1.set_ylabel("Groups with a drop, solid")
    ax2.set_ylabel("Network share, dashed")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=8,
               loc='upper left', bbox_to_anchor=(1.1, 1))

    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _entity_drops_report_and_plot(df, group_col, label, output_path, metric_label=None,
                                   exclude_none_bucket=True):
    """
    Shared logic for both entity-drops deliverables: classify per-snapshot
    partial/total drops for `group_col`, print the top-5-by-snapshot-count
    for each drop type, and save the 4-series time plot.
    """
    if metric_label is None:
        metric_label = group_col

    drop_df = _classify_drops(df, group_col, exclude_none_bucket=exclude_none_bucket)
    if drop_df.empty:
        print(f"[DROPS-INFO] No drop events found for '{metric_label}' — {label}; skipping.")
        return

    _print_top5_drop_counts(drop_df, group_col, label, metric_label)

    _plot_entity_drops_series(
        df, drop_df, output_path,
    )


def family_id_entity_drops_distributions(df, context_label):
    """
    For `df` (network-wide or tracked, post-validate_sort_data
    shape), classifies each declared_family_id group's per-snapshot status
    as a partial or total drop, prints the top-5-by-snapshot-count for
    each drop type, and saves the 4-series time plot to
    analysis-results/sybil/family-id/{context_label}_family_id_entity_drops.png.

    When context_label == "tracked", additionally repeats the same
    analysis once per distinct service_type, saved to
    analysis-results/sybil/family-id/{service_type_lower}_family_id_entity_drops.png.
    """
    base_dir = os.path.join("analysis-results", "sybil", "family-id")

    df = df[df["consecutive_hourly_absences"].isna() | (df["consecutive_hourly_absences"] <= 1)]

    _entity_drops_report_and_plot(
        df, 'declared_family_id', context_label,
        os.path.join(base_dir, f"{context_label}_family_id_entity_drops.png"),
        metric_label="declared_family_id",
    )

    if context_label == "tracked" and 'service_type' in df.columns:
        service_types = sorted(t for t in df['service_type'].dropna().unique() if t not in ('None', ''))
        for service_type in service_types:
            subset = df[df['service_type'] == service_type]
            _entity_drops_report_and_plot(
                subset, 'declared_family_id', f"tracked — {service_type}",
                os.path.join(base_dir, f"{service_type.lower()}_family_id_entity_drops.png"),
                metric_label="declared_family_id",
            )


def ip_entity_drops_distributions(df, context_label):
    """
    IP-based equivalent, same structure as
    family_id_entity_drops_distributions, but groups by IP-based identity
    instead of declared_family_id, in two variants: exact ip_address, and
    the derived /24 subnet (via the same _to_24_subnet helper used by
    ip_sybil_distributions). Saved under analysis-results/sybil/ip_sybil/
    as {context_label}_ip_entity_drops.png /
    {context_label}_ip_24_subnet_entity_drops.png (and, for
    context_label == "tracked", additionally per service_type as
    {service_type_lower}_ip_entity_drops.png /
    {service_type_lower}_ip_24_subnet_entity_drops.png).
    """
    base_dir = os.path.join("analysis-results", "sybil", "ip_sybil")

    df = df[df["consecutive_hourly_absences"].isna() | (df["consecutive_hourly_absences"] <= 1)]

    working = df.copy()
    working['_ip_24_subnet'] = working['ip_address'].apply(_to_24_subnet)

    def run_both_variants(sub_df, label, filename_prefix):
        _entity_drops_report_and_plot(
            sub_df, 'ip_address', label,
            os.path.join(base_dir, f"{filename_prefix}_ip_entity_drops.png"),
            metric_label="ip_address",
        )
        _entity_drops_report_and_plot(
            sub_df, '_ip_24_subnet', label,
            os.path.join(base_dir, f"{filename_prefix}_ip_24_subnet_entity_drops.png"),
            metric_label="/24 subnet",
        )

    run_both_variants(working, context_label, context_label)

    if context_label == "tracked" and 'service_type' in working.columns:
        service_types = sorted(t for t in working['service_type'].dropna().unique() if t not in ('None', ''))
        for service_type in service_types:
            subset = working[working['service_type'] == service_type]
            run_both_variants(subset, f"tracked — {service_type}", service_type.lower())


# ---------------------------------------------------------------------------
# Rotated-node stability distribution analysis
# ---------------------------------------------------------------------------

def _daily_active_inactive_counts(df, dedupe_on_service_type=False):
    """
    Per-day unique active / inactive / total counts. When
    dedupe_on_service_type is True, dedupe on the (fingerprint,
    service_type) pair instead of fingerprint alone -- this is the
    rotated_tracked general-count overlap rule: a physical node that
    directory-hosts more than one tracked service that day is counted
    once per distinct service_type it served, not once overall.
    """
    working = df.copy()
    if dedupe_on_service_type:
        working['_dedupe_key'] = list(zip(working['fingerprint'], working['service_type']))
    else:
        working['_dedupe_key'] = working['fingerprint']

    is_active = working['status'] == 'ACTIVE_IN_RING'
    total_count = working.groupby('date')['_dedupe_key'].nunique()
    active_count = (
        working[is_active].groupby('date')['_dedupe_key'].nunique()
        .reindex(total_count.index, fill_value=0)
    )
    inactive_count = total_count - active_count
    return active_count, inactive_count, total_count


def _daily_fractions(active_count, inactive_count, total_count, days):
    """
    Convert per-day active/inactive/total count Series into aligned
    active_fraction / inactive_fraction lists over `days` (in
    chronological order). A day absent from the counts (e.g. a
    service_type with no rows that day) or with zero total nodes yields
    a fraction of 0 rather than a gap or a ZeroDivisionError.
    """
    active_fractions, inactive_fractions = [], []
    for day in days:
        total = total_count.get(day, 0)
        active = active_count.get(day, 0)
        inactive = inactive_count.get(day, 0)
        active_fractions.append((active / total) if total else 0.0)
        inactive_fractions.append((inactive / total) if total else 0.0)
    return active_fractions, inactive_fractions


def _mean_fraction(fractions):
    """
    Unweighted arithmetic mean of a list of per-day fractions -- i.e. the
    average of each day's percentage, not a count-weighted average across
    all nodes/days combined. E.g. 80%, 90%, 87%, 92%, 85% averages to
    86.8%, regardless of how many nodes were active each of those days.
    Returns 0.0 for an empty list rather than dividing by zero.
    """
    return (sum(fractions) / len(fractions)) if fractions else 0.0


def _plot_diverging_stability_bar(days, active_fractions, inactive_fractions, output_path):
    """
    Diverging bar chart: one blue bar (active_fraction, positive) and one
    red bar (inactive_fraction, plotted as its negative) per day, against
    a zero baseline. A bar chart rather than a line plot since this is
    one value pair per day, not a multi-point-per-day series.
    """
    x = list(range(len(days)))

    fig, ax = plt.subplots(figsize=(max(11, len(days) * 0.6), 6))
    ax.bar(x, active_fractions, color='blue', label='Active fraction')
    ax.bar(x, [-v for v in inactive_fractions], color='red', label='Inactive fraction')
    ax.axhline(0, color='black', linewidth=0.8)

    ax.set_xticks(x)
    ax.set_xticklabels(days, rotation=45)
    ax.set_xlabel("Day")
    ax.set_ylabel("Fraction of nodes (active / inactive)")
    ax.legend(fontsize=8, loc='upper left', bbox_to_anchor=(1.02, 1))

    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _plot_diverging_stability_grouped_bar(days, service_type_fractions, output_path):
    """
    Combined diverging bar chart for multiple service types on one figure:
    grouped/clustered bars per day, one blue/red bar pair per service
    type, each service type in a distinct shade (Blues colormap for
    active, Reds colormap for inactive), with a legend identifying which
    shade belongs to which service_type.

    `service_type_fractions`: dict {service_type: (active_fractions,
    inactive_fractions)}, each list already aligned to `days`.
    """
    service_types = list(service_type_fractions.keys())
    n = len(service_types)
    x = np.arange(len(days))
    bar_width = 0.8 / max(n, 1)
    # Sample mid-to-dark shades so bars stay visible against a white
    # background even for the first service type.
    shades = np.linspace(0.4, 0.9, n) if n > 1 else [0.7]
    blue_cmap = plt.cm.Blues
    red_cmap = plt.cm.Reds

    fig, ax = plt.subplots(figsize=(max(11, len(days) * 0.8), 6))

    for i, service_type in enumerate(service_types):
        active_fractions, inactive_fractions = service_type_fractions[service_type]
        offset = (i - (n - 1) / 2) * bar_width
        ax.bar(x + offset, active_fractions, width=bar_width,
               color=blue_cmap(shades[i]), label=f"{service_type} (active)")
        ax.bar(x + offset, [-v for v in inactive_fractions], width=bar_width,
               color=red_cmap(shades[i]), label=f"{service_type} (inactive)")

    ax.axhline(0, color='black', linewidth=0.8)
    ax.set_xticks(list(x))
    ax.set_xticklabels(days, rotation=45)
    ax.set_xlabel("Day")
    ax.set_ylabel("Fraction of nodes (active / inactive)")
    ax.legend(fontsize=7, loc='upper left', bbox_to_anchor=(1.02, 1))

    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def daily_rotated_nodes_stability_distributions(df, context_label):
    """
    For `df` (a rotated network-wide or rotated tracked DataFrame, in the
    shape produced by validate_sort_data()) and context_label
    ("rotated_network" or "rotated_tracked"): per "Day N", compute the
    unique active (status == ACTIVE_IN_RING) and inactive fraction of
    nodes, print them, and save a diverging bar chart (active fraction as
    a positive blue bar, inactive fraction as a negative red bar) to
    analysis-results/stability/rotated/{context_label}_daily_rotated_node_stability.png.

    context_label == "rotated_tracked" is handled specially in two ways:
    1. The general (whole-DataFrame) count above dedupes on the
       (fingerprint, service_type) pair rather than fingerprint alone, so
       a node tracked under N distinct service_types that day contributes
       N times, under the overlap rule for this general count.
       rotated_network has no such overlap (there's nothing to
       dedupe by), so it dedupes on fingerprint alone.
    2. Additionally repeats the per-day active/inactive fraction
       computation once per distinct service_type (each subset already
       single-service-type, so no overlap-dedupe needed there), and saves
       ONE combined grouped-bar figure with every service type's
       diverging fractions together, to
       analysis-results/stability/rotated/daily_rotated_service_type_node_stability.png.

    Branches strictly on context_label, not on service_type column
    presence -- rotated_network also carries a service_type column, just
    uniformly "None" after cleaning, so step 2 above only ever runs for
    "rotated_tracked".
    """
    base_dir = os.path.join("analysis-results", "stability", "rotated")
    df = df[df["consecutive_hourly_absences"].isna() | (df["consecutive_hourly_absences"] <= 1)]
    days = _chronological_days(df)

    dedupe_on_service_type = (context_label == "rotated_tracked")
    active_count, inactive_count, total_count = _daily_active_inactive_counts(
        df, dedupe_on_service_type=dedupe_on_service_type
    )
    active_fractions, inactive_fractions = _daily_fractions(active_count, inactive_count, total_count, days)

    print(f"\n[STABILITY] Daily rotated-node active/inactive fractions — {context_label}")
    # for day, active_frac, inactive_frac in zip(days, active_fractions, inactive_fractions):
    #     print(f"    {day}: active={active_frac:.4%}  inactive={inactive_frac:.4%}")

    mean_active = _mean_fraction(active_fractions)
    mean_inactive = _mean_fraction(inactive_fractions)
    print(f"    Average % of active HSDir nodes: {mean_active:.4%}")
    print(f"    Average % of inactive HSDir nodes: {mean_inactive:.4%}")

    _plot_diverging_stability_bar(
        days, active_fractions, inactive_fractions,
        os.path.join(base_dir, f"{context_label}_daily_rotated_node_stability.png"),
    )

    if context_label == "rotated_tracked" and 'service_type' in df.columns:
        service_types = sorted(t for t in df['service_type'].dropna().unique() if t not in ('None', ''))
        if service_types:
            service_type_fractions = {}
            for service_type in service_types:
                subset = df[df['service_type'] == service_type]
                st_active, st_inactive, st_total = _daily_active_inactive_counts(
                    subset, dedupe_on_service_type=False
                )
                st_active_fracs, st_inactive_fracs = _daily_fractions(st_active, st_inactive, st_total, days)
                service_type_fractions[service_type] = (st_active_fracs, st_inactive_fracs)

                print(f"\n[STABILITY] Daily rotated-node active/inactive fractions — {context_label} — {service_type}")
                # for day, active_frac, inactive_frac in zip(days, st_active_fracs, st_inactive_fracs):
                #     print(f"    {day}: active={active_frac:.4%}  inactive={inactive_frac:.4%}")

                st_mean_active = _mean_fraction(st_active_fracs)
                st_mean_inactive = _mean_fraction(st_inactive_fracs)
                print(f"    Average % of active HSDir nodes: {st_mean_active:.4%}")
                print(f"    Average % of inactive HSDir nodes: {st_mean_inactive:.4%}")

            _plot_diverging_stability_grouped_bar(
                days, service_type_fractions,
                os.path.join(base_dir, "daily_rotated_service_type_node_stability.png"),
            )


# ---------------------------------------------------------------------------
# Ring-position uniformity analysis (unified: onion->HSDir distance,
# HSDir ring index, onion ring index)
#
# Computes the same per-day uniformity/churn statistic family for three
# ring-position metrics, each scoped to the context_labels where its source
# column exists, via a vectorized groupby/pivot/diff approach.
#
# Conventions: only status == "ACTIVE_IN_RING" counts as active; output root
# is "analysis-results/". The EPHEMERAL_VARIABLE dispersion plot uses a
# per-day pooled mean +/- std across that day's ephemeral onions, since each
# ephemeral onion lives one day and a per-onion-across-days line would
# degenerate to a single point.
# ---------------------------------------------------------------------------

TWO_POW_256 = 2 ** 256

# Metric registry. Each entry describes one ring-position metric: which
# source column feeds it (with an optional fallback), how that column maps
# into normalized [0,1] space, which per-day entity it groups by, which
# context_labels it's valid for, and where its outputs go.
_UNIFORMITY_METRICS = {
    "onion_to_hsdir": {
        "value_col": "hsdir_to_onion_ring_distance",
        "fallback_col": "hsdir_to_onion_ring_position",
        "fallback_divisor": 100.0,
        "kind": "raw_over_2pow256",
        "entity": "mapped_onion",
        "valid_contexts": ("tracked",),
        "output_dir": os.path.join("analysis-results", "uniformity", "onion_to_hsdir"),
        "value_label": "Normalized Ring Distance [0, 1]",
        "metric_title": "Onion-to-HSDir Ring Distance",
    },
    "hsdir_index": {
        "value_col": "hsdir_index_hrt_hex",
        "fallback_col": "hsdir_index_hrt_100",
        "fallback_divisor": 100.0,
        "kind": "hex_over_2pow256",
        "entity": "fingerprint",
        "valid_contexts": ("tracked", "network"),
        "output_dir": os.path.join("analysis-results", "uniformity", "hsdir_index"),
        "value_label": "Normalized Ring Index [0, 1]",
        "metric_title": "HSDir Hash-Ring Index",
    },
    "onion_index": {
        "value_col": "mapped_onion_index_hex",
        "fallback_col": None,
        "fallback_divisor": None,
        "kind": "hex_over_2pow256",
        "entity": "mapped_onion",
        "valid_contexts": ("tracked",),
        "output_dir": os.path.join("analysis-results", "uniformity", "onion_index"),
        "value_label": "Normalized Onion Ring Index [0, 1]",
        "metric_title": "Onion Hash-Ring Index",
    },
}


def _parse_hex_to_unit(series):
    """
    Vectorized hex-string -> [0,1] conversion: int(hex, 16) / 2**256.
    Generalizes compute_hsdir_index_uniformity's parse_hex_index. Handles a
    leading 0x/0X and treats missing/"None"/"nan"/blank as NaN. Python ints
    are unbounded, so the 256-bit hex parses exactly; the division is done
    in float, which is fine for the [0,1] uniformity work here.
    """
    def conv(val):
        if val is None:
            return np.nan
        s = str(val).strip()
        if s in ('', 'None', 'nan', 'NaN'):
            return np.nan
        if s[:2] in ('0x', '0X'):
            s = s[2:]
        try:
            return float(int(s, 16)) / TWO_POW_256
        except (ValueError, TypeError):
            return np.nan
    return series.apply(conv)


def _hour_to_float(series):
    """
    Shared hour-parsing helper. "HH:MM" -> HH + MM/60; a bare number -> that
    number; anything unparseable -> 0.0. Vectorized and per-value safe.
    """
    s = series.astype(str)
    has_colon = s.str.contains(":", na=False)
    out = pd.Series(0.0, index=s.index, dtype=float)

    if has_colon.any():
        parts = s[has_colon].str.split(":", expand=True)
        hh = pd.to_numeric(parts[0], errors="coerce").fillna(0)
        mm = pd.to_numeric(parts[1], errors="coerce").fillna(0) if parts.shape[1] > 1 else 0
        out.loc[has_colon] = hh + mm / 60.0
    if (~has_colon).any():
        out.loc[~has_colon] = pd.to_numeric(s[~has_colon], errors="coerce").fillna(0.0)
    return out


def _prepare_uniformity_frame(df_slice, metric, dedupe_on_service_type=False):
    """
    Build the working frame for one metric: filter to valid entity+fingerprint
    rows, drop ghost nodes (consecutive_hourly_absences > 1), normalize status
    (STRICT: only ACTIVE_IN_RING is active), parse the hour, and derive the
    normalized [0,1] `value` column from the metric's source (or fallback)
    column. Returns None if the metric's source column is absent or nothing
    usable remains.

    `dedupe_on_service_type` only affects the entity key for the HSDir-index
    metric's tracked service-type overlap case: it appends service_type to
    the fingerprint entity so a relay serving N service types is counted once
    per service type (matching the rule used in
    daily_rotated_nodes_stability_distributions). It has no effect for the
    onion-indexed metrics (a mapped_onion belongs to exactly one service type).
    """
    entity = metric["entity"]
    value_col = metric["value_col"]
    fallback_col = metric["fallback_col"]

    if df_slice is None or df_slice.empty:
        return None
    if "fingerprint" not in df_slice.columns or entity not in df_slice.columns:
        return None
    if value_col not in df_slice.columns and (fallback_col is None or fallback_col not in df_slice.columns):
        return None

    df = df_slice.copy()

    # Valid entity + fingerprint only.
    df = df[(df[entity].notna()) & (df[entity] != 'None') &
            (df['fingerprint'].notna()) & (df['fingerprint'] != 'None')]

    # Ghost-node filter (consecutive_hourly_absences <= 1, NaN allowed).
    if "consecutive_hourly_absences" in df.columns:
        cha = pd.to_numeric(df["consecutive_hourly_absences"], errors="coerce")
        df = df[cha.isna() | (cha <= 1)]

    if df.empty:
        return None

    # STRICT active convention, consistent with the rest of this module.
    if "status" in df.columns:
        df["clean_status"] = df["status"].fillna("ACTIVE_IN_RING").astype(str)
    else:
        df["clean_status"] = "ACTIVE_IN_RING"
    df["_is_active"] = df["clean_status"] == "ACTIVE_IN_RING"

    df["hour_num"] = _hour_to_float(df["hour"]) if "hour" in df.columns else 0.0

    # Normalized [0,1] value.
    if metric["kind"] == "hex_over_2pow256":
        if value_col in df.columns:
            df["value"] = _parse_hex_to_unit(df[value_col])
        else:
            df["value"] = np.nan
        if df["value"].dropna().empty and fallback_col and fallback_col in df.columns:
            df["value"] = pd.to_numeric(df[fallback_col], errors="coerce") / metric["fallback_divisor"]
    else:  # raw_over_2pow256 (distance), with position fallback
        if value_col in df.columns:
            df["value"] = pd.to_numeric(df[value_col], errors="coerce") / TWO_POW_256
        else:
            df["value"] = np.nan
        if df["value"].dropna().empty and fallback_col and fallback_col in df.columns:
            df["value"] = pd.to_numeric(df[fallback_col], errors="coerce") / metric["fallback_divisor"]

    df = df.dropna(subset=["value"])
    if df.empty:
        return None

    # Entity key for churn/stats grouping.
    if dedupe_on_service_type and entity == "fingerprint" and "service_type" in df.columns:
        df["_entity_key"] = list(zip(df["fingerprint"], df["service_type"].astype(str)))
    else:
        df["_entity_key"] = df[entity]

    return df


def _vectorized_churn(df, entity_key_col="_entity_key"):
    """
    Vectorized per-(date, entity) churn with new/disappeared semantics:
      - "new"        = the first hour a fingerprint is seen active that day
                       for that entity (not recounted on later reappearance);
                       every active fp in the first observed hour is new.
      - "disappeared"= active in the previous hour, not active this hour
                       (counted each time it happens).
    Returns a DataFrame with columns [date, entity_key_col, New HSDirs,
    Disappeared HSDirs, Total Churn].
    """
    # (date, entity, fingerprint, hour) is active if ANY of its rows is active.
    active_cell = (
        df.groupby(["date", entity_key_col, "fingerprint", "hour_num"])["_is_active"]
        .any()
    )

    rows = []
    for (date, entity), sub in active_cell.groupby(level=[0, 1]):
        mat = (
            sub.reset_index(level=[0, 1], drop=True)
            .unstack("hour_num")
        )
        mat = mat.reindex(sorted(mat.columns), axis=1).fillna(False).astype(bool)
        arr = mat.values
        n_hours = arr.shape[1]
        if n_hours == 0:
            rows.append((date, entity, 0, 0, 0))
            continue

        # NEW: active this hour AND not active in any strictly-previous hour.
        seen_prev = np.zeros_like(arr, dtype=bool)
        if n_hours > 1:
            seen_prev[:, 1:] = np.maximum.accumulate(arr, axis=1)[:, :-1]
        tot_new = int((arr & ~seen_prev).sum())

        # DISAPPEARED: active previous hour, not active this hour.
        tot_disappeared = int((arr[:, :-1] & ~arr[:, 1:]).sum()) if n_hours > 1 else 0

        rows.append((date, entity, tot_new, tot_disappeared, tot_new + tot_disappeared))

    return pd.DataFrame(rows, columns=["date", entity_key_col,
                                       "New HSDirs", "Disappeared HSDirs", "Total Churn"])


def _ks_uniformity(values):
    """KS-test-based uniformity score against U(0,1): 1 - D. NaN if < 3 points."""
    v = np.asarray(values, dtype=float)
    if len(v) < 3:
        return np.nan
    ks_stat, _ = kstest(v, "uniform")
    return 1.0 - ks_stat


def _dispersion_points_table(df, entity_key_col="_entity_key", by_hour=False):
    """
    Per-onion mean/std of the normalized `value`, for the dispersion plot.

    by_hour=False: one (entity, date) mean/std -- the normal per-day
      trajectory used for long-lived onions (STATIC_CONTROL / THIRD_PARTY_PROBE),
      which persist across days and so form a line day-to-day.

    by_hour=True: one (entity, date, hour) mean/std, plus a "_t" ordinal that
      lays every (date, hour) snapshot on a single chronological axis. Used
      for EPHEMERAL_VARIABLE, where each onion lives a single day: aggregating
      per day gives one point per onion (no line), so we instead resolve each
      onion across the hourly snapshots of its day, giving it a real
      multi-point trajectory that renders like the other service types.
    """
    keys = ["date", "hour", entity_key_col] if by_hour else ["date", entity_key_col]
    grouped = df.groupby(keys)["value"]
    tbl = grouped.agg(
        mean_value="mean",
        std_value=lambda s: s.std(ddof=0),
    ).reset_index()
    tbl["std_value"] = tbl["std_value"].fillna(0.0)

    if by_hour:
        # Ordinal position for each distinct (date, hour) in chronological order.
        snaps = _chronological_snapshots(df)  # list of (date, hour)
        order = {snap: i for i, snap in enumerate(snaps)}
        tbl["_t"] = list(zip(tbl["date"], tbl["hour"]))
        tbl["_t"] = tbl["_t"].map(order)
        tbl = tbl.dropna(subset=["_t"]).sort_values("_t")
    return tbl


def _compute_uniformity_table(df, entity_key_col="_entity_key"):
    """
    Vectorized per-(date, entity) uniformity stats (mean/std/min/max/range +
    KS uniformity score) over the normalized `value`, merged with vectorized
    churn on (date, entity). Returns the combined per-(date, entity) table.
    """
    grouped = df.groupby(["date", entity_key_col])["value"]
    stats = grouped.agg(
        sample_count="count",
        mean_value="mean",
        std_value=lambda s: s.std(ddof=0),  # population std, matching np.std default
        min_value="min",
        max_value="max",
    ).reset_index()
    stats["range_value"] = stats["max_value"] - stats["min_value"]
    stats["std_value"] = stats["std_value"].fillna(0.0)

    ks = grouped.apply(_ks_uniformity).reset_index(name="uniformity_score")
    stats = stats.merge(ks, on=["date", entity_key_col])

    churn = _vectorized_churn(df, entity_key_col)
    table = stats.merge(churn, on=["date", entity_key_col], how="left")
    for c in ["New HSDirs", "Disappeared HSDirs", "Total Churn"]:
        table[c] = table[c].fillna(0).astype(int)
    return table


def _plot_ecdf(ax, values, label, color):
    """Draw one ECDF curve of `values` in [0,1] on `ax`."""
    vals = np.sort(np.asarray(values, dtype=float))
    if len(vals) == 0:
        return
    ecdf = np.arange(1, len(vals) + 1) / len(vals)
    ax.plot(vals, ecdf, color=color, linewidth=2.0, label=label)


def _save_ecdf_figure(dedup_values, output_path, value_label):
    """Single-series ECDF vs ideal U(0,1)."""
    fig, ax = plt.subplots(figsize=(10, 6))
    _plot_ecdf(ax, dedup_values, "Empirical CDF (ECDF)", "#2ca02c")
    ax.plot([0, 1], [0, 1], color="gray", linestyle="--", linewidth=1.5, label="Ideal Uniform U(0,1)")
    ax.set_xlabel(value_label)
    ax.set_ylabel("Cumulative Probability")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.legend(loc="lower right")
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _save_ecdf_overlay_figure(series_values, output_path, value_label):
    """Overlay one ECDF per key in `series_values` = {label: values}."""
    fig, ax = plt.subplots(figsize=(10, 6))
    for i, (lbl, vals) in enumerate(series_values.items()):
        _plot_ecdf(ax, vals, str(lbl), _series_color(lbl, i))
    ax.plot([0, 1], [0, 1], color="gray", linestyle="--", linewidth=1.5, label="Ideal Uniform U(0,1)")
    ax.set_xlabel(value_label)
    ax.set_ylabel("Cumulative Probability")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.legend(loc="lower right", fontsize=8)
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


# Fixed, high-contrast colors per service type so the same service type is
# always drawn in the same easily-distinguished color across every overlay
# (the previous tab10 blue/orange/green were too close in the scatter overlay).
_SERVICE_TYPE_COLORS = {
    "STATIC_CONTROL": "#e41a1c",      # strong red
    "EPHEMERAL_VARIABLE": "#377eb8",  # strong blue
    "THIRD_PARTY_PROBE": "#4daf4a",   # strong green
}
_FALLBACK_COLORS = ["#984ea3", "#ff7f00", "#a65628", "#f781bf", "#999999"]


def _series_color(label, index):
    """Contrasting color for a service-type (or other) series label."""
    return _SERVICE_TYPE_COLORS.get(str(label), _FALLBACK_COLORS[index % len(_FALLBACK_COLORS)])


def _save_churn_scatter_figure(tables_by_label, output_path):
    """
    Churn-vs-dispersion scatter. `tables_by_label` = {label: uniformity_table};
    each contributes its (Total Churn, std_value) points in one contrasting
    color, with a per-series linear trend line where there's enough spread.
    """
    fig, ax = plt.subplots(figsize=(12, 7))
    for i, (lbl, table) in enumerate(tables_by_label.items()):
        if table is None or table.empty:
            continue
        color = _series_color(lbl, i)
        x = table["Total Churn"].values
        y = table["std_value"].fillna(0).values
        ax.scatter(x, y, color=color, alpha=0.6, edgecolors="none", s=40, label=str(lbl))
        if len(table) > 1 and pd.Series(x).nunique() > 1:
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            xs = np.linspace(x.min(), x.max(), 100)
            ax.plot(xs, p(xs), color=color, linestyle="--", linewidth=1.8,
                    label=f"{lbl} trend (slope={z[0]:.4f})")
    ax.set_xlabel("Daily total churn")
    ax.set_ylabel(r"Value Std Dev ($\sigma$)")
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _save_dispersion_figure(df, entity_key_col, output_path, value_label,
                            by_hour=False):
    """
    Per-onion dispersion plot: for each onion, a mean line plus a shaded
    +/-1 std band (mean line in the Dark2 palette, band in YlOrRd), one line
    per onion.

    `df` is the prepared per-row frame (has date/hour/value/_entity_key). The
    x-axis is built explicitly from an ordered list of time slots and every
    onion is drawn against those same integer positions, so days sort
    chronologically ("Day 2" before "Day 10", not lexicographically) and all
    onions share one consistent axis.

    by_hour=False (default): x = chronological "Day N"; each onion contributes
      one point per day. Long-lived onions (STATIC_CONTROL / THIRD_PARTY_PROBE)
      persist across days, so they render as connected multi-day lines.

    by_hour=True: x = chronological (date, hour) snapshots; each onion
      contributes one point per hourly snapshot. Used for EPHEMERAL_VARIABLE,
      whose onions each live a single day -- resolving them by hour gives each
      a real multi-point line within its day instead of a single dot, so the
      plot looks like the other service types rather than empty vertical bands.
    """
    tbl = _dispersion_points_table(df, entity_key_col, by_hour=by_hour)

    if by_hour:
        slots = _chronological_snapshots(df)          # ordered (date, hour)
        pos = {snap: i for i, snap in enumerate(slots)}
        tbl = tbl.assign(_x=list(zip(tbl["date"], tbl["hour"])))
        tbl["_x"] = tbl["_x"].map(pos)
        # One tick per day, at that day's first snapshot.
        tick_pos, tick_lab, seen = [], [], set()
        for i, (d, _h) in enumerate(slots):
            if d not in seen:
                seen.add(d)
                tick_pos.append(i)
                tick_lab.append(d)
    else:
        slots = _chronological_days(df)               # ordered "Day N"
        pos = {d: i for i, d in enumerate(slots)}
        tbl = tbl.assign(_x=tbl["date"].map(pos))
        tick_pos, tick_lab = list(range(len(slots))), slots

    fig, ax = plt.subplots(figsize=(15, 8))
    entities = list(pd.unique(tbl[entity_key_col]))
    avg_colors = plt.cm.Dark2(np.linspace(0.0, 1.0, max(1, len(entities))))
    warm_colors = plt.cm.YlOrRd(np.linspace(0.5, 0.9, max(1, len(entities))))
    for idx, ent in enumerate(entities):
        sub = tbl[tbl[entity_key_col] == ent].sort_values("_x")
        x = sub["_x"].values
        m = sub["mean_value"].values
        s = sub["std_value"].values
        ax.plot(x, m, linestyle="-", color=avg_colors[idx % len(avg_colors)],
                alpha=0.8, linewidth=1.5)
        ax.fill_between(x, np.maximum(0, m - s), np.minimum(1, m + s),
                        color=warm_colors[idx % len(warm_colors)], alpha=0.15)

    legend_elements = [
        Line2D([0], [0], color="black", linestyle="-", label="Mean Relative Ring Distance"),
        Line2D([0], [0], color="orange", linestyle="--", label="±1 Std Dev Dispersion (Shaded)"),
    ]
    ax.legend(handles=legend_elements, loc="upper right")

    ax.set_xticks(tick_pos)
    ax.set_xticklabels(tick_lab, rotation=45)
    ax.set_xlabel("Observation Timeline")
    ax.set_ylabel(value_label)
    ax.set_ylim(-0.05, 1.05)
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _run_one_metric(df, metric_key, context_label):
    """Compute + plot one metric for one context. Returns the per-(date,entity) table or None."""
    metric = _UNIFORMITY_METRICS[metric_key]
    if context_label not in metric["valid_contexts"]:
        return None

    out_dir = metric["output_dir"]
    entity = metric["entity"]
    value_label = metric["value_label"]

    # --- General case (any valid context) ---
    prepared = _prepare_uniformity_frame(df, metric, dedupe_on_service_type=False)
    if prepared is None:
        print(f"[UNIFORMITY-INFO] No usable data for metric '{metric_key}' — {context_label}; skipping.")
        return None

    table = _compute_uniformity_table(prepared)
    if table.empty:
        print(f"[UNIFORMITY-INFO] No uniformity records for metric '{metric_key}' — {context_label}; skipping.")
        return None

    # ECDF over the deduped-per-entity value population (dedupe per day+entity
    # so repeated hourly rows of one entity don't over-weight the curve).
    dedup_vals = prepared.drop_duplicates(subset=["date", "_entity_key", "fingerprint"])["value"].values
    _save_ecdf_figure(
        dedup_vals,
        os.path.join(out_dir, f"{context_label}_distance_uniformity_ecdf.png"),
        value_label=value_label,
    )
    _save_churn_scatter_figure(
        {context_label: table},
        os.path.join(out_dir, f"{context_label}_churn_vs_distance_dispersion.png"),
    )
    # General per-onion dispersion plot:
    # one mean±std line per onion across days. Only meaningful for the two
    # onion-indexed metrics (entity == mapped_onion); the HSDir-ring-index
    # metric groups by fingerprint, for which a "per-onion" dispersion plot
    # is undefined, so it is skipped there.
    if entity == "mapped_onion":
        _save_dispersion_figure(
            prepared, "_entity_key",
            os.path.join(out_dir, f"{context_label}_onion_distance_uniformity.png"),
            value_label=value_label,
        )

    # --- Tracked-only service-type breakdowns ---
    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ('None', '')
        )

        # For the HSDir-index metric, the combined overlay dedupes on
        # (fingerprint, service_type) so a relay serving multiple service
        # types is counted once per service type.
        dedupe_st = (entity == "fingerprint")

        overlay_ecdf_series = {}
        overlay_scatter_tables = {}
        for st in service_types:
            sub = df[df["service_type"] == st]
            sub_prepared = _prepare_uniformity_frame(sub, metric, dedupe_on_service_type=dedupe_st)
            if sub_prepared is None:
                continue
            sub_table = _compute_uniformity_table(sub_prepared)
            if sub_table.empty:
                continue

            st_vals = sub_prepared.drop_duplicates(
                subset=["date", "_entity_key", "fingerprint"]
            )["value"].values
            overlay_ecdf_series[st] = st_vals
            overlay_scatter_tables[st] = sub_table

            st_lower = st.lower()

            # Per-service-type ECDF (its own real ECDF, matching the filename).
            _save_ecdf_figure(
                st_vals,
                os.path.join(out_dir, f"{st_lower}_distance_uniformity_ecdf.png"),
                value_label=value_label,
            )

            # Per-service-type churn-vs-dispersion scatter, for EVERY metric
            # (previously only produced for the onion-indexed metrics).
            _save_churn_scatter_figure(
                {st: sub_table},
                os.path.join(out_dir, f"{st_lower}_churn_vs_distance_dispersion.png"),
            )

            # Per-service-type per-onion dispersion breakdown (onion-indexed
            # metrics only — grouping fingerprints into a per-onion dispersion
            # plot isn't meaningful for the HSDir-ring-index metric).
            # EPHEMERAL_VARIABLE onions each live one day, so resolve them by
            # hourly snapshot (by_hour=True) to give each a real within-day
            # line; long-lived service types use the per-day trajectory.
            if entity == "mapped_onion":
                _save_dispersion_figure(
                    sub_prepared, "_entity_key",
                    os.path.join(out_dir, f"{st_lower}_onion_distance_uniformity.png"),
                    value_label=value_label,
                    by_hour=(st == "EPHEMERAL_VARIABLE"),
                )

        if overlay_ecdf_series:
            _save_ecdf_overlay_figure(
                overlay_ecdf_series,
                os.path.join(out_dir, "service_types_distance_uniformity_ecdf.png"),
                value_label=value_label,
            )
        if overlay_scatter_tables:
            _save_churn_scatter_figure(
                overlay_scatter_tables,
                os.path.join(out_dir, "service_types_churn_vs_distance_dispersion.png"),
            )

    return table


def hrt_uniformity_distributions(df, context_label):
    """
    Unified ring-position uniformity and churn analysis over every ring-position
    metric valid in `context_label`.

    Computes, per metric, the per-day uniformity statistic family
    (mean/std/min/max/range + KS-based uniformity score against U(0,1)) and
    daily HSDir churn (new/disappeared), then saves an ECDF plot and a
    churn-vs-dispersion scatter. For the "tracked" context it also saves
    service-type ECDF/scatter overlays and, for the onion-indexed metrics,
    per-service-type per-onion dispersion breakdowns (with the special
    EPHEMERAL_VARIABLE daily-pooled rendering).

    Metric -> (source column, per-day entity, valid contexts, output dir):
      * onion_to_hsdir : hsdir_to_onion_ring_distance (fallback ...position),
                         per mapped_onion, tracked only,
                         analysis-results/uniformity/onion_to_hsdir
      * hsdir_index    : hsdir_index_hrt_hex (fallback ..._100),
                         per fingerprint, tracked + network,
                         analysis-results/uniformity/hsdir_index
      * onion_index    : mapped_onion_index_hex, per mapped_onion, tracked only,
                         analysis-results/uniformity/onion_index

    Calling this with context_label == "network" simply skips the two
    onion-indexed metrics (their source columns don't exist there) and runs
    only the HSDir-index metric. Metrics/days/service types with too few
    points for a stable KS test (< 3) yield a NaN uniformity score rather
    than erroring.
    """

    df = df[df["consecutive_hourly_absences"].isna() | (df["consecutive_hourly_absences"] <= 1)]

    if df is None or df.empty:
        print(f"[UNIFORMITY-ERROR] Missing data slice for context '{context_label}'.")
        return

    print(f"\n[UNIFORMITY] Ring-position uniformity analysis — {context_label}")
    for metric_key in _UNIFORMITY_METRICS:
        _run_one_metric(df, metric_key, context_label)


# ---------------------------------------------------------------------------
# DHT ring-index coverage analysis (hsdir_index_hrt_hex / mapped_onion_index_hex)
#
# Companion to hrt_uniformity_distributions: instead of how *uniformly* the
# observed ring indexes are spread, this measures how much of the ring space
# is actually *occupied* by them -- overall, per day, and per service type.
#
# Coverage definition (resolved modeling choice, see HRT_COVERAGE_BUCKETS):
#   The literal "occupied fraction of the 2^256 ring" is degenerate at this
#   dataset's scale -- a few dozen-to-hundreds of exact points out of 2^256
#   is ~10^-62, far below float precision, so it always prints as 0.0 and
#   carries zero research signal. Instead we discretize the [0,1]-normalized
#   ring (the same _parse_hex_to_unit values the uniformity function uses)
#   into HRT_COVERAGE_BUCKETS equal-width buckets and define:
#       coverage % = (buckets containing >=1 observed index) / total buckets.
#   The bucket count is an explicit, tunable modeling parameter -- the
#   reported percentages are coverage *at this resolution*, NOT an absolute
#   ring-occupancy figure, and the plot/print labels say so.
# ---------------------------------------------------------------------------

# Number of equal-width buckets the normalized [0,1] ring is discretized into
# for coverage. Chosen as a fixed, moderate constant: with this dataset's
# scale (dozens to low hundreds of distinct indexes per day, ~1-2k across the
# month) 10,000 buckets keeps per-day coverage well away from both 0% (which
# a 2^256-scale exact count would always give) and 100% (which too few
# buckets would trivially give), so the number actually moves with the data.
# Tune here if a different resolution is wanted; every coverage % scales with it.
HRT_COVERAGE_BUCKETS = 10_000

# Metrics this coverage analysis applies to: only the two hex ring-INDEX
# metrics. onion_to_hsdir is a relative distance, not a ring position, so
# "DHT coverage" is undefined for it and it is excluded.
_COVERAGE_METRIC_KEYS = ("hsdir_index", "onion_index")


def _indexes_to_buckets(values, n_buckets=HRT_COVERAGE_BUCKETS):
    """
    Map normalized [0,1] ring values to integer bucket ids in [0, n_buckets-1].
    A value of exactly 1.0 would land in bucket n_buckets, so it's clamped
    into the last bucket. Returns a numpy int array.
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return np.empty(0, dtype=int)
    buckets = np.floor(arr * n_buckets).astype(int)
    np.clip(buckets, 0, n_buckets - 1, out=buckets)
    return buckets


def _day_covered_buckets(day_df, by_hour=False, n_buckets=HRT_COVERAGE_BUCKETS):
    """
    Set of covered bucket ids for a single day's prepared rows.

    by_hour=False (default, all non-ephemeral cases): the day's covered set is
      just the set of buckets over every observed index that day. Order and
      repetition are irrelevant -- a set can't double-count -- so this
      naturally realizes the "union over the day" semantics.

    by_hour=True (EPHEMERAL_VARIABLE): buckets are accumulated across the day's
      hourly snapshots in chronological order, adding an entity's index only
      the FIRST hour that entity is observed that day (mirroring
      _vectorized_churn's "new" = first-appearance rule). A later-hour
      reappearance of the same entity -- even if its index has since changed
      across an HRT rotation -- is not re-processed, so the day's set is the
      incremental union of first-seen indexes, not of all hourly values.
    """
    if not by_hour:
        return set(_indexes_to_buckets(day_df["value"].values, n_buckets).tolist())

    covered = set()
    seen_entities = set()
    # Chronological hour order within this day.
    hours = sorted(day_df["hour_num"].dropna().unique())
    for hr in hours:
        hr_slice = day_df[day_df["hour_num"] == hr]
        # First-appearance only: skip entities already counted earlier today.
        fresh = hr_slice[~hr_slice["_entity_key"].isin(seen_entities)]
        if not fresh.empty:
            covered.update(_indexes_to_buckets(fresh["value"].values, n_buckets).tolist())
            seen_entities.update(fresh["_entity_key"].tolist())
    return covered


def _daily_coverage_series(prepared, days, by_hour=False, n_buckets=HRT_COVERAGE_BUCKETS):
    """
    Per-day coverage fraction (covered buckets that day / n_buckets), aligned
    to `days`. A day absent from the data (or with zero observed indexes)
    yields 0.0 rather than a gap or a divide-by-zero.
    """
    fractions = []
    grouped = {d: sub for d, sub in prepared.groupby("date")}
    for d in days:
        sub = grouped.get(d)
        if sub is None or sub.empty:
            fractions.append(0.0)
            continue
        covered = _day_covered_buckets(sub, by_hour=by_hour, n_buckets=n_buckets)
        fractions.append(len(covered) / n_buckets)
    return fractions


def _overall_coverage(prepared, by_hour=False, n_buckets=HRT_COVERAGE_BUCKETS):
    """
    Whole-period coverage fraction: union of covered buckets across all days
    (each day built under the same by_hour rule) / n_buckets.
    """
    covered = set()
    for _d, sub in prepared.groupby("date"):
        covered |= _day_covered_buckets(sub, by_hour=by_hour, n_buckets=n_buckets)
    return len(covered) / n_buckets


def _dedup_prepared(prepared):
    """
    Dedup to one row per (day, entity, fingerprint), consistent with how
    hrt_uniformity_distributions dedups before bucketing/plotting, so repeated
    hourly rows of one entity don't distort coverage. Keeps date/hour/hour_num/
    value/_entity_key/fingerprint/service_type needed downstream.
    """
    return prepared.drop_duplicates(subset=["date", "_entity_key", "fingerprint", "hour"])


def _plot_coverage_diverging(days, coverage_fractions, output_path):
    """
    Single-series diverging coverage plot: coverage as a positive solid blue
    line, uncovered share (1 - coverage) as a negative solid red line, per day.
    """
    x = list(range(len(days)))
    uncovered = [-(1.0 - c) for c in coverage_fractions]

    fig, ax = plt.subplots(figsize=(max(11, len(days) * 0.6), 6))
    ax.plot(x, coverage_fractions, color="blue", linestyle="-", marker="o", label="Coverage")
    ax.plot(x, uncovered, color="red", linestyle="-", marker="o", label="Uncovered share")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(days, rotation=45)
    ax.set_xlabel("Day")
    ax.set_ylabel("Ring-bucket fraction (covered / uncovered)")
    ax.set_ylim(-1.05, 1.05)
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _plot_coverage_overlay(days, series_fractions, output_path, show_legend=True):
    """
    Combined diverging coverage plot for multiple labeled series (service types
    or onions). `series_fractions` = {label: coverage_fraction_list}. Each
    series' coverage is drawn in a clearer shade of its color and its uncovered
    share (negative) in a darker shade of the same hue.

    show_legend=False suppresses the per-series legend, used for the per-onion
    breakdown where dozens of onions would otherwise bury the plot under a
    legend larger than the axes (the aggregate spread pattern is the point
    there, not identifying each individual onion).
    """
    x = list(range(len(days)))
    fig, ax = plt.subplots(figsize=(max(12, len(days) * 0.7), 7))

    for i, (label, cov) in enumerate(series_fractions.items()):
        base = _series_color(label, i)
        rgb = np.array(mcolors.to_rgb(base))
        light = tuple(rgb + (1.0 - rgb) * 0.45)   # clearer/lighter shade -> coverage
        dark = tuple(rgb * 0.55)                   # darker shade of same hue -> uncovered
        uncovered = [-(1.0 - c) for c in cov]
        cov_label = f"{label} (coverage)" if show_legend else None
        unc_label = f"{label} (uncovered)" if show_legend else None
        ax.plot(x, cov, color=light, linestyle="-", marker="o", markersize=3, label=cov_label)
        ax.plot(x, uncovered, color=dark, linestyle="-", marker="o", markersize=3, label=unc_label)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(days, rotation=45)
    ax.set_xlabel("Day")
    ax.set_ylabel("Ring-bucket fraction (covered / uncovered)")
    ax.set_ylim(-1.05, 1.05)
    if show_legend:
        ax.legend(loc="upper right", fontsize=7, ncol=2)
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _run_one_coverage_metric(df, metric_key, context_label):
    """Compute + print + plot ring-index coverage for one metric in one context."""
    metric = _UNIFORMITY_METRICS[metric_key]
    if context_label not in metric["valid_contexts"]:
        return

    out_dir = metric["output_dir"]
    metric_title = metric["metric_title"]

    prepared = _prepare_uniformity_frame(df, metric, dedupe_on_service_type=False)
    if prepared is None:
        print(f"[COVERAGE-INFO] No usable data for metric '{metric_key}' — {context_label}; skipping.")
        return
    prepared = _dedup_prepared(prepared)
    days = _chronological_days(prepared)

    # --- General case (steps 1-3) ---
    overall = _overall_coverage(prepared)
    daily = _daily_coverage_series(prepared, days)
    daily_avg = (sum(daily) / len(daily)) if daily else 0.0

    print(f"\n[COVERAGE] {metric_title} ring coverage — {context_label} "
          f"(resolution: {HRT_COVERAGE_BUCKETS} buckets)")
    print(f"    Overall month-wide coverage (all days pooled): {overall:.4%}")
    print(f"    Daily-average coverage (mean of per-day coverage): {daily_avg:.4%}")

    _plot_coverage_diverging(
        days, daily,
        os.path.join(out_dir, f"{context_label}_daily_hrt_coverage.png"),
    )

    # --- Tracked-only: per-service-type (steps 5-6) ---
    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ('None', '')
        )
        # HSDir-index overlap rule: a relay serving multiple service types is
        # bucketed once per service type (dedupe_on_service_type=True); onion
        # index has no such overlap (an onion belongs to one service type).
        dedupe_st = (metric["entity"] == "fingerprint")

        overlay = {}
        for st in service_types:
            sub = df[df["service_type"] == st]
            sub_prepared = _prepare_uniformity_frame(sub, metric, dedupe_on_service_type=dedupe_st)
            if sub_prepared is None:
                continue
            sub_prepared = _dedup_prepared(sub_prepared)
            # EPHEMERAL_VARIABLE: accumulate a day's buckets across its hourly
            # snapshots, first-appearance only (see _day_covered_buckets).
            by_hour = (st == "EPHEMERAL_VARIABLE")

            st_overall = _overall_coverage(sub_prepared, by_hour=by_hour)
            st_daily = _daily_coverage_series(sub_prepared, days, by_hour=by_hour)
            st_avg = (sum(st_daily) / len(st_daily)) if st_daily else 0.0
            overlay[st] = st_daily

            print(f"    [{st}] overall: {st_overall:.4%}  |  daily-average: {st_avg:.4%}")

        # One combined overlay figure per metric (step 5).
        if overlay:
            _plot_coverage_overlay(
                days, overlay,
                os.path.join(out_dir, "service_types_daily_hrt_coverage.png"),
            )

        # Per-service-type, per-onion breakdown (step 6) — hsdir_index only.
        # For each service type, one figure with a coverage/uncoverage line
        # pair per mapped_onion: bucket the hsdir_index values of the HSDirs
        # responsible for that onion ("how spread on the ring are this onion's
        # HSDirs" — a sybil-clustering signal). Dropped for onion_index by
        # design (an onion's own index is ~one value per rotation, so per-onion
        # coverage would be near-empty).
        if metric_key == "hsdir_index":
            for st in service_types:
                sub = df[df["service_type"] == st]
                # Reuse the onion_to_hsdir metric spec to get a per-mapped_onion
                # frame, but bucket the HSDIR index, not the distance: prepare
                # with hsdir_index (fingerprint entity) yet keep mapped_onion.
                sub_prepared = _prepare_uniformity_frame(sub, metric, dedupe_on_service_type=False)
                if sub_prepared is None or "mapped_onion" not in sub_prepared.columns:
                    continue
                sub_prepared = _dedup_prepared(sub_prepared)
                by_hour = (st == "EPHEMERAL_VARIABLE")

                onion_series = {}
                for onion, onion_df in sub_prepared.groupby("mapped_onion"):
                    if onion in ('None', '') or onion_df.empty:
                        continue
                    onion_series[onion] = _daily_coverage_series(
                        onion_df, days, by_hour=by_hour
                    )
                if not onion_series:
                    continue
                _plot_coverage_overlay(
                    days, onion_series,
                    os.path.join(out_dir, f"{st.lower()}_onion_daily_hrt_coverage.png"),
                    show_legend=False,
                )


def hrt_index_coverage_distributions(df, context_label):
    """
    DHT ring-index coverage analysis for the two hex ring-index metrics
    (hsdir_index_hrt_hex for network+tracked, mapped_onion_index_hex for
    tracked only). Companion to hrt_uniformity_distributions.

    For each applicable metric it prints the overall month-wide coverage and
    the daily-average coverage (two distinct figures), saves a per-day
    diverging coverage/uncoverage plot, and -- under "tracked" -- per
    service-type coverage plus a combined service-type overlay, and (for
    hsdir_index only) a per-service-type per-onion coverage breakdown.

    "Coverage" is the fraction of HRT_COVERAGE_BUCKETS equal-width buckets of
    the normalized [0,1] ring that contain at least one observed index; it is
    a resolution-dependent modeling quantity, not an absolute 2^256-ring
    occupancy (which is ~0 at this scale). See HRT_COVERAGE_BUCKETS.
    """
    if df is None or df.empty:
        print(f"[COVERAGE-ERROR] Missing data slice for context '{context_label}'.")
        return

    df = df[df["consecutive_hourly_absences"].isna() | (df["consecutive_hourly_absences"] <= 1)]

    print(f"\n[COVERAGE] DHT ring-index coverage analysis — {context_label}")
    for metric_key in _COVERAGE_METRIC_KEYS:
        _run_one_coverage_metric(df, metric_key, context_label)


# ---------------------------------------------------------------------------
# HSDir new/disappeared/reconnected churn analysis
#
# Classifies each entity's per-(date, hour) transition against the
# immediately preceding chronological snapshot into exactly one of:
#   * new          - first-ever appearance in the whole observation window
#   * reconnected  - present now, absent last snapshot, but seen at some
#                    earlier point in the window (any earlier day or hour)
#   * disappeared  - present last snapshot, absent now
#
# Unlike every other function in this file, this one deliberately does NOT
# use `status` at all: activity is defined purely by row presence after a
# stricter ghost filter (consecutive_hourly_absences == 0, not <= 1), since
# the state machine needs a strict "was this entity actually in this
# consensus" signal rather than "not yet ghosted".
# ---------------------------------------------------------------------------

def _stripped_flag_counts(df, dedupe_on_service_type=False):
    """
    Per-(date, hour) count of distinct entities with
    status == "STRIPPED_HSDIR_FLAG" at consecutive_hourly_absences == 1 --
    a diagnostic breakdown of "just lost its HSDir flag", overlapping the
    disappeared churn category rather than being disjoint from it.

    Filter: (consecutive_hourly_absences.isna() |
    (consecutive_hourly_absences == 1)) AND status == "STRIPPED_HSDIR_FLAG".
    The status condition is required so a node with one absence for an
    unrelated reason isn't misread as a stripped-flag event. This "== 1"
    strictness differs from _churn_state_machine's "== 0" presence filter;
    both are needed at once (0 absences for who is present, 1 for who was
    just flagged).

    Entities are keyed as in _churn_state_machine: (fingerprint,
    service_type) under the tracked overlap rule when dedupe_on_service_type
    is set, else fingerprint alone.

    Returns [date, hour, stripped_count], one row per qualifying (date, hour);
    snapshots with none are absent and filled with 0 by the caller. Returns an
    empty frame if consecutive_hourly_absences, status, or fingerprint is
    missing.
    """
    empty = pd.DataFrame(columns=["date", "hour", "stripped_count"])
    if df is None or df.empty or "consecutive_hourly_absences" not in df.columns \
            or "status" not in df.columns or "fingerprint" not in df.columns:
        return empty

    cha = pd.to_numeric(df["consecutive_hourly_absences"], errors="coerce")
    mask = (cha.isna() | (cha == 1)) & (df["status"].astype(str) == "STRIPPED_HSDIR_FLAG")
    stripped = df[mask].copy()
    if stripped.empty:
        return empty

    if dedupe_on_service_type and "service_type" in stripped.columns:
        stripped["_entity_key"] = list(zip(stripped["fingerprint"], stripped["service_type"].astype(str)))
    else:
        stripped["_entity_key"] = stripped["fingerprint"]

    return (
        stripped.groupby(["date", "hour"])["_entity_key"]
        .nunique()
        .reset_index(name="stripped_count")
    )


def _churn_state_machine(df, dedupe_on_service_type=False):
    """
    Walk _chronological_snapshots(df) in order and classify every entity's
    per-snapshot transition, relative to the previous snapshot and never
    resetting at day boundaries.

    Two sets are carried across the whole walk: prev_entities (present in the
    previous snapshot) and ever_seen (every entity observed so far). Per
    snapshot, entities that just appeared split into "new" (never seen
    before) and "reconnected" (seen earlier); prev_entities not in the
    current snapshot are "disappeared". The first snapshot needs no special
    case, since both sets start empty.

    Rate denominators are type-specific: new/reconnected divide by the
    current snapshot's total, disappeared by the previous snapshot's total.
    Rates are returned as non-negative fractions; the negative-for-departures
    convention is applied only at plot time.

    A stripped_count/stripped_rate pair is merged on from
    _stripped_flag_counts by (date, hour), with missing snapshots treated as
    a real 0 and stripped_rate reusing prev_total. Stripped events are a
    diagnostic subset of disappeared, not a mutually exclusive category.

    `dedupe_on_service_type=True` keys entities on (fingerprint,
    service_type) so a node serving several service types is counted once per
    type; a no-op for single-service-type or network scopes.

    Returns one row per (date, hour) with columns: date, hour, new_count,
    disappeared_count, reconnected_count, stripped_count, new_rate,
    disappeared_rate, reconnected_rate, stripped_rate, current_total,
    prev_total.
    """
    empty_cols = ["date", "hour", "new_count", "disappeared_count", "reconnected_count",
                  "stripped_count", "new_rate", "disappeared_rate", "reconnected_rate",
                  "stripped_rate", "current_total", "prev_total"]
    if df is None or df.empty or "consecutive_hourly_absences" not in df.columns \
            or "fingerprint" not in df.columns:
        return pd.DataFrame(columns=empty_cols)

    snapshots = _chronological_snapshots(df)
    if not snapshots:
        return pd.DataFrame(columns=empty_cols)

    # Strict presence filter: only rows with zero consecutive
    # hourly absences count as "actually in this consensus", NOT the looser
    # <=1 ghost tolerance used elsewhere in this file. NaN (no data) is kept,
    # consistent with every other ghost filter's NaN handling.
    cha = pd.to_numeric(df["consecutive_hourly_absences"], errors="coerce")
    working = df[cha.isna() | (cha == 0)].copy()

    if dedupe_on_service_type and "service_type" in working.columns:
        working["_entity_key"] = list(zip(working["fingerprint"], working["service_type"].astype(str)))
    else:
        working["_entity_key"] = working["fingerprint"]

    entity_sets = (
        working.groupby(["date", "hour"])["_entity_key"].apply(set).to_dict()
        if not working.empty else {}
    )

    rows = []
    prev_entities = set()
    ever_seen = set()
    for snap in snapshots:
        current_entities = entity_sets.get(snap, set())

        appeared = current_entities - prev_entities
        new_entities = appeared - ever_seen
        reconnected_entities = appeared & ever_seen
        disappeared_entities = prev_entities - current_entities

        current_total = len(current_entities)
        prev_total = len(prev_entities)

        new_count = len(new_entities)
        reconnected_count = len(reconnected_entities)
        disappeared_count = len(disappeared_entities)

        rows.append({
            "date": snap[0], "hour": snap[1],
            "new_count": new_count,
            "disappeared_count": disappeared_count,
            "reconnected_count": reconnected_count,
            "new_rate": (new_count / current_total) if current_total else 0.0,
            "disappeared_rate": (disappeared_count / prev_total) if prev_total else 0.0,
            "reconnected_rate": (reconnected_count / current_total) if current_total else 0.0,
            "current_total": current_total,
            "prev_total": prev_total,
        })

        ever_seen |= current_entities
        prev_entities = current_entities

    df_states = pd.DataFrame(rows)

    # Merge the stripped-flag pass by (date, hour); snapshots with no
    # qualifying event get stripped_count = 0 (a real zero).
    stripped_df = _stripped_flag_counts(df, dedupe_on_service_type)
    df_states = df_states.merge(stripped_df, on=["date", "hour"], how="left")
    df_states["stripped_count"] = df_states["stripped_count"].fillna(0).astype(int)
    df_states["stripped_rate"] = np.where(
        df_states["prev_total"] > 0,
        df_states["stripped_count"] / df_states["prev_total"].replace(0, np.nan),
        0.0,
    )
    df_states["stripped_rate"] = df_states["stripped_rate"].fillna(0.0)

    return df_states


# Fixed colors for the three churn types, via the shared _series_color
# palette, so "new"/"disappeared"/"reconnected" are always the same color
# across the count and rate plots (and across every context/service type).
_CHURN_TYPE_COLORS = {
    "new": _series_color("new", 0),
    "disappeared": _series_color("disappeared", 1),
    "reconnected": _series_color("reconnected", 2),
    # Explicit purple, not the _series_color fallback (index 3 there would be
    # pink) -- also deliberately a different purple from "new"'s fallback
    # shade (#984ea3, itself an orchid/purple) so the two stay visually
    # distinct when plotted together.
    "stripped": "#4B0082",
}


def _churn_day_tick_positions(df_states, max_labels=15):
    """
    One x tick per day, at that day's first snapshot -- same pattern as
    _plot_entity_drops_series's x-axis -- but thinned to at most
    `max_labels` evenly-spaced day labels. Without this, a full month at
    hourly cadence would try to cram 30 rotated labels into a compact
    figure and they'd overlap into an unreadable smear.
    """
    day_first_index = {}
    for i, date in enumerate(df_states["date"]):
        if date not in day_first_index:
            day_first_index[date] = i
    days = list(day_first_index.keys())
    positions = list(day_first_index.values())
    if len(days) > max_labels:
        step = -(-len(days) // max_labels)  # ceil division, no import needed
        days = days[::step]
        positions = positions[::step]
    return positions, days


def _plot_churn_counts(df_states, output_path):
    """
    Four solid lines (new/disappeared/reconnected/stripped), all plotted as
    plain absolute counts (none negated -- unlike the rate plot, this is not
    a diverging chart). X-axis: every (date, hour) snapshot in chronological
    order, with day-only tick labels.

    stripped is a diagnostic breakdown of disappeared (nodes stripped of
    their HSDir flag specifically), not a mutually exclusive fourth
    category -- its line typically sits at or below the disappeared line at
    any given snapshot, which is the expected shape, not double-counting.

    Figure size is FIXED regardless of how many (date, hour) snapshots are
    plotted -- it does not grow with the data, the same compact look
    whether this is 11 days or a full month at hourly cadence (a plain
    thin line, no per-point markers, is legible at any density; a
    width that scales with point count is not).
    """
    x = list(range(len(df_states)))
    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(x, df_states["new_count"], linestyle="-", linewidth=1.0,
            color=_CHURN_TYPE_COLORS["new"], label="New")
    ax.plot(x, df_states["disappeared_count"], linestyle="-", linewidth=1.0,
            color=_CHURN_TYPE_COLORS["disappeared"], label="Disappeared")
    ax.plot(x, df_states["reconnected_count"], linestyle="-", linewidth=1.0,
            color=_CHURN_TYPE_COLORS["reconnected"], label="Reconnected")
    ax.plot(x, df_states["stripped_count"], linestyle="-", linewidth=1.0,
            color=_CHURN_TYPE_COLORS["stripped"], label="Stripped HSDir flag")

    tick_pos, tick_lab = _churn_day_tick_positions(df_states)
    ax.set_xticks(tick_pos)
    ax.set_xticklabels(tick_lab, rotation=45, ha="right", fontsize=8)
    ax.set_xlabel("Day")
    ax.set_ylabel("Entity count")
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _plot_churn_rates(df_states, output_path):
    """
    Same x-axis/line structure as the count plot, but diverging: new rate
    positive, disappeared/reconnected/stripped rates negated (plotted below
    zero). stripped is negated alongside disappeared because it is itself a
    leave-type event (a diagnostic breakdown of disappeared, not a distinct
    join/leave direction). The stored rate values themselves stay positive
    (see _churn_state_machine) -- negation happens only here, at plot time.

    Same fixed-figure-size, marker-free, thinned-tick-label treatment as
    _plot_churn_counts, for the same reason (compact regardless of a
    month's worth of hourly snapshots).
    """
    x = list(range(len(df_states)))
    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(x, df_states["new_rate"], linestyle="-", linewidth=1.0,
            color=_CHURN_TYPE_COLORS["new"], label="New rate")
    ax.plot(x, -df_states["disappeared_rate"], linestyle="-", linewidth=1.0,
            color=_CHURN_TYPE_COLORS["disappeared"], label="Disappeared rate")
    ax.plot(x, -df_states["reconnected_rate"], linestyle="-", linewidth=1.0,
            color=_CHURN_TYPE_COLORS["reconnected"], label="Reconnected rate")
    ax.plot(x, -df_states["stripped_rate"], linestyle="-", linewidth=1.0,
            color=_CHURN_TYPE_COLORS["stripped"], label="Stripped HSDir flag rate")
    ax.axhline(0, color="black", linewidth=0.8)

    tick_pos, tick_lab = _churn_day_tick_positions(df_states)
    ax.set_xticks(tick_pos)
    ax.set_xticklabels(tick_lab, rotation=45, ha="right", fontsize=8)
    ax.set_xlabel("Day")
    ax.set_ylabel("Churn rate per snapshot")
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _print_churn_summary(df_states, label):
    """
    Per type (new/disappeared/reconnected/stripped), print all four
    aggregate figures, clearly labeled so hourly and daily, and count and
    rate, are never confused with one another:
      * avg count/hour = sum of that type's count across every (date,hour)
        row / number of (date,hour) rows.
      * avg rate/hour  = same, using rate instead of count.
      * avg count/day  = first sum each day's hourly counts into one daily
        total (not a per-day average -- counts have no upper bound, so a
        "how many total that day" sum is meaningful), then average those
        daily totals across days.
      * avg rate/day   = first AVERAGE each day's hourly rates into one
        daily mean rate (not a sum -- rates are already bounded fractions
        of a population, so summing several together can run past 100%
        and stops meaning "a rate"), then average those daily means
        across days. Both steps are plain averages, so the final number
        stays a proper rate.

    stripped is a diagnostic subset of disappeared (see
    _churn_state_machine), so its printed figures are expected to run at or
    below disappeared's -- not evidence of an error.
    """
    n_snapshots = len(df_states)
    if n_snapshots == 0:
        print(f"[CHURN-INFO] No snapshots to summarize for {label}.")
        return

    daily_counts = df_states.groupby("date")[
        ["new_count", "disappeared_count", "reconnected_count", "stripped_count"]
    ].sum()
    daily_rates = df_states.groupby("date")[
        ["new_rate", "disappeared_rate", "reconnected_rate", "stripped_rate"]
    ].mean()
    n_days = len(daily_counts) if len(daily_counts) else 1

    print(f"\n[CHURN] {label} — per-snapshot summary ({n_snapshots} snapshots, {n_days} days)")
    for kind, count_col, rate_col in (
        ("new", "new_count", "new_rate"),
        ("disappeared", "disappeared_count", "disappeared_rate"),
        ("reconnected", "reconnected_count", "reconnected_rate"),
        ("stripped", "stripped_count", "stripped_rate"),
    ):
        avg_hour_count = df_states[count_col].sum() / n_snapshots
        avg_hour_rate = df_states[rate_col].sum() / n_snapshots
        avg_day_count = daily_counts[count_col].sum() / n_days
        avg_day_rate = daily_rates[rate_col].sum() / n_days
        print(f"    [{kind}] avg count/hour: {avg_hour_count:.4f}   avg rate/hour: {avg_hour_rate:.4%}   "
              f"avg count/day: {avg_day_count:.4f}   avg rate/day: {avg_day_rate:.4%}")


def hsdir_churn_distributions(df, context_label):
    """
    New/disappeared/reconnected/stripped-HSDir-flag churn analysis. For `df`
    (network-wide or tracked, post-validate_sort_data shape) and
    context_label ("network" or "tracked"): classifies every entity's
    transition at each (date, hour) snapshot (relative to the immediately
    preceding snapshot, never resetting at day boundaries), prints the four
    aggregate figures per type, and saves a counts plot and a rates plot to
    analysis-results/churn/.

    context_label == "tracked" additionally repeats the whole analysis once
    per distinct service_type (each already single-service-type, so no
    overlap-dedupe needed there), saving
    {service_type_lower}_churn_count_per_hour.png /
    {service_type_lower}_churn_rate_per_hour.png.

    Entity presence for new/disappeared/reconnected is decided purely by
    consecutive_hourly_absences == 0 (a stricter filter than the usual <=1
    ghost tolerance elsewhere in this file) and does not use status. The
    stripped-HSDir-flag signal is a separate, differently-filtered pass
    (consecutive_hourly_absences == 1 AND status == "STRIPPED_HSDIR_FLAG",
    see _stripped_flag_counts) merged onto the same table -- see
    _churn_state_machine for how the two filters coexist.
    """
    base_dir = os.path.join("analysis-results", "churn")

    if df is None or df.empty:
        print(f"[CHURN-ERROR] Missing data slice for context '{context_label}'.")
        return
    if "consecutive_hourly_absences" not in df.columns or "fingerprint" not in df.columns:
        print(f"[CHURN-INFO] Required columns missing for {context_label}; skipping.")
        return

    # Combined "tracked" (all service types) run applies the overlap-dedupe
    # rule; "network" has no service_type at all, so it's a no-op there.
    dedupe_st = (context_label == "tracked")
    df_states = _churn_state_machine(df, dedupe_on_service_type=dedupe_st)
    if df_states.empty:
        print(f"[CHURN-INFO] No snapshots to analyze for {context_label}; skipping.")
        return

    _print_churn_summary(df_states, context_label)
    _plot_churn_counts(
        df_states, os.path.join(base_dir, f"{context_label}_churn_count_per_hour.png"),
    )
    _plot_churn_rates(
        df_states, os.path.join(base_dir, f"{context_label}_churn_rate_per_hour.png"),
    )

    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ("None", "")
        )
        for st in service_types:
            sub = df[df["service_type"] == st]
            if sub.empty:
                print(f"[CHURN-INFO] No rows for service_type '{st}'; skipping.")
                continue
            sub_states = _churn_state_machine(sub, dedupe_on_service_type=False)
            if sub_states.empty:
                print(f"[CHURN-INFO] No snapshots to analyze for service_type '{st}'; skipping.")
                continue

            label = f"tracked — {st}"
            _print_churn_summary(sub_states, label)
            st_lower = st.lower()
            _plot_churn_counts(
                sub_states, os.path.join(base_dir, f"{st_lower}_churn_count_per_hour.png"),
            )
            _plot_churn_rates(
                sub_states, os.path.join(base_dir, f"{st_lower}_churn_rate_per_hour.png"),
            )


# ---------------------------------------------------------------------------
# Uptime-matrix and hourly-uptime-duration analysis
#
# Two complementary views of relay/HSDir online-offline behavior over the
# observation window:
#   * hsdir_uptime_distributions        -- entity x snapshot online/offline
#     bitmap, in two variants: a plain first-appearance-ordered matrix and a
#     similarity-clustered matrix that groups entities with correlated uptime
#     sequences and flags near-identical runs.
#   * hsdir_hourly_uptime_distributions -- per-day uptime-duration statistics
#     (average + 85/50/15 percentiles) in decimal hours.
# ---------------------------------------------------------------------------

def _uptime_entity_key(df, dedupe_on_service_type):
    """
    Entity key Series for the uptime analyses: the same overlap rule
    _prepare_uniformity_frame uses -- (fingerprint, service_type) tuples when
    dedupe_on_service_type is set and service_type exists (so a relay serving
    N service types is one column per service type), else fingerprint alone.
    """
    if dedupe_on_service_type and "service_type" in df.columns:
        return list(zip(df["fingerprint"], df["service_type"].astype(str)))
    return df["fingerprint"]


def _apply_ghost_filter(df):
    """Standard file-wide ghost filter: keep rows with consecutive_hourly_absences
    NaN or <= 1 (NOT the stricter == 0 used only by the churn analysis)."""
    if "consecutive_hourly_absences" not in df.columns:
        return df
    cha = pd.to_numeric(df["consecutive_hourly_absences"], errors="coerce")
    return df[cha.isna() | (cha <= 1)]


def _build_uptime_matrix(df, dedupe_on_service_type):
    """
    Build the snapshot x entity uptime matrix shared by both matrix variants,
    over the full entity universe (the union of every entity ever seen), with
    three cell states so "absent from a consensus" is never conflated with
    "present but offline":

        0 = OFFLINE : listed in that consensus but not ACTIVE_IN_RING
        1 = ACTIVE  : has an ACTIVE_IN_RING row in that consensus
        2 = ABSENT  : has no row at all (not yet born, already gone, or omitted)

    Keeping ABSENT distinct from OFFLINE lets the renderer colour it
    separately, so a relay that simply did not exist yet doesn't read as
    "offline" for every prior row.

    Returns (state_matrix, active_matrix, entities, snapshots):
      * state_matrix : int array (n_snapshots, n_entities) of 0/1/2, drives
                       rendering.
      * active_matrix: int array, 1 iff ACTIVE else 0 -- the binary uptime
                       sequence used for correlation clustering and
                       identical-run detection.
      * entities     : union entity keys in first-appearance order (column
                       order for the status-only variant).
      * snapshots    : (date, hour) pairs in chronological order (row order).
    """
    work = df.copy()
    work["_entity_key"] = _uptime_entity_key(work, dedupe_on_service_type)
    work["_is_active"] = (work["status"].astype(str) == "ACTIVE_IN_RING") \
        if "status" in work.columns else True

    snapshots = _chronological_snapshots(work)
    snap_index = {snap: i for i, snap in enumerate(snapshots)}
    work = work.assign(_snap_pos=list(zip(work["date"], work["hour"])))
    work["_snap_pos"] = work["_snap_pos"].map(snap_index)

    # Full union, first-appearance column order.
    work_sorted = work.sort_values("_snap_pos", kind="stable")
    entities = list(dict.fromkeys(work_sorted["_entity_key"].tolist()))
    entity_index = {e: j for j, e in enumerate(entities)}

    n_snap, n_ent = len(snapshots), len(entities)
    # Start everything ABSENT (2); mark presence, then activity.
    state = np.full((n_snap, n_ent), 2, dtype=int)

    rows_all = work["_snap_pos"].to_numpy()
    cols_all = work["_entity_key"].map(entity_index).to_numpy()
    valid_all = ~pd.isna(rows_all) & ~pd.isna(cols_all)
    ra = rows_all[valid_all].astype(int)
    ca = cols_all[valid_all].astype(int)
    # Present (row exists) -> at least OFFLINE (0).
    state[ra, ca] = 0

    act = work[work["_is_active"]]
    ract = act["_snap_pos"].to_numpy()
    cact = act["_entity_key"].map(entity_index).to_numpy()
    valid_act = ~pd.isna(ract) & ~pd.isna(cact)
    if valid_act.any():
        state[ract[valid_act].astype(int), cact[valid_act].astype(int)] = 1

    active_matrix = (state == 1).astype(int)
    return state, active_matrix, entities, snapshots


def _correlation_distance_order(matrix):
    """
    Column order from single-linkage hierarchical clustering on a
    (1 - Pearson r) distance between entities' uptime sequences.

    matrix is (n_snapshots, n_entities); correlation is between columns.
    numpy.corrcoef gives r for all non-constant columns; constant columns
    (zero variance -- always-on or always-off all month) yield NaN there and
    are patched per the resolved three-case split:
      * both constant, same value        -> r = 1  (identical behavior)  -> d 0
      * both constant, opposite values   -> r = -1 (exact complements)   -> d 2
      * one constant, one variable       -> r = 0  (no info)             -> d 1
    Returns a list of column indices (the dendrogram leaf order). For fewer
    than 2 entities, returns the trivial order unchanged.
    """
    n = matrix.shape[1]
    if n < 2:
        return list(range(n))

    col_var = matrix.var(axis=0)
    constant_mask = col_var == 0
    # Constant value per column (only meaningful where constant_mask): the
    # single value that column holds (0 or 1); use the first row.
    const_val = matrix[0, :] if matrix.shape[0] else np.zeros(n)

    with np.errstate(invalid="ignore", divide="ignore"):
        r = np.corrcoef(matrix, rowvar=False)
    r = np.atleast_2d(r)

    # Patch every pair involving at least one constant column.
    ci = np.where(constant_mask)[0]
    for i in ci:
        for j in range(n):
            if i == j:
                r[i, j] = 1.0
                continue
            if constant_mask[j]:
                # both constant: same value -> 1, opposite -> -1
                r[i, j] = 1.0 if const_val[i] == const_val[j] else -1.0
            else:
                # one constant, one variable: no information
                r[i, j] = 0.0
            r[j, i] = r[i, j]
    np.fill_diagonal(r, 1.0)
    # Any residual NaN (shouldn't remain) -> neutral.
    r = np.nan_to_num(r, nan=0.0)

    dist = 1.0 - r
    np.fill_diagonal(dist, 0.0)
    dist = np.clip(dist, 0.0, 2.0)
    # Enforce exact symmetry for squareform.
    dist = (dist + dist.T) / 2.0
    np.fill_diagonal(dist, 0.0)

    condensed = squareform(dist, checks=False)
    if condensed.size == 0:
        return list(range(n))
    Z = linkage(condensed, method="single")
    return list(leaves_list(Z))


def _identical_run_mask(matrix_ordered, min_run=5):
    """
    Boolean array over ordered columns: True where a column belongs to a run
    of `min_run` or more ADJACENT columns whose full binary sequence (all
    rows) is exactly identical. Used to recolor those columns solid red.
    """
    n = matrix_ordered.shape[1]
    flagged = np.zeros(n, dtype=bool)
    if n == 0:
        return flagged
    run_start = 0
    for j in range(1, n + 1):
        same = (j < n) and np.array_equal(matrix_ordered[:, j], matrix_ordered[:, run_start])
        if not same:
            if j - run_start >= min_run:
                flagged[run_start:j] = True
            run_start = j
    return flagged


def _render_uptime_matrix(matrix, output_path, red_mask=None):
    """
    Render a snapshot x entity uptime matrix as an RGB bitmap from the
    three-state values produced by _build_uptime_matrix:
        1 (ACTIVE)  -> black
        0 (OFFLINE) -> white   (present in the consensus but not active)
        2 (ABSENT)  -> purple  (not listed in that consensus at all -- a
                                distinct state, so a not-yet-born / already-gone
                                relay is never mistaken for a real offline one)
    Columns in red_mask (if given) are recolored solid red across all rows
    (identical-uptime blocks). Rows = snapshots (earliest at top).
    """
    n_snap, n_ent = matrix.shape
    ABSENT_COLOR = (0.55, 0.20, 0.75)  # high-contrast purple vs black/white/red

    rgb = np.ones((max(n_snap, 1), max(n_ent, 1), 3), dtype=float)  # default white = OFFLINE
    if n_snap and n_ent:
        rgb[matrix == 1] = (0.0, 0.0, 0.0)        # ACTIVE -> black
        rgb[matrix == 2] = ABSENT_COLOR           # ABSENT -> purple
        if red_mask is not None and red_mask.any():
            rgb[:, red_mask, :] = (1.0, 0.0, 0.0)  # identical-run -> red

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.imshow(rgb, aspect="auto", interpolation="nearest", origin="upper")
    ax.set_xlabel("Entities (columns)")
    ax.set_ylabel("Consensuses (rows, earliest at top)")
    ax.set_xticks([])
    ax.set_yticks([])

    legend_handles = [
        Patch(facecolor="black", label="Active (in ring)"),
        Patch(facecolor="white", edgecolor="gray", label="Offline (in consensus, not active)"),
        Patch(facecolor=ABSENT_COLOR, label="Absent (not in consensus)"),
    ]
    if red_mask is not None and red_mask.any():
        legend_handles.append(Patch(facecolor="red", label="Identical-uptime block (≥5)"))
    ax.legend(handles=legend_handles, loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8)

    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _uptime_matrix_for_slice(df, dedupe_on_service_type, out_dir, name_prefix, label):
    """Build + render both matrix variants (status-only and clustered) for one
    data slice (a whole context, or one service_type subset)."""
    if df is None or df.empty:
        print(f"[UPTIME-INFO] No data for {label}; skipping matrix.")
        return
    state_matrix, active_matrix, entities, snapshots = _build_uptime_matrix(df, dedupe_on_service_type)
    if state_matrix.shape[1] == 0 or state_matrix.shape[0] == 0:
        print(f"[UPTIME-INFO] Empty matrix for {label}; skipping.")
        return

    # 2a. Status-only, first-appearance column order (no clustering, no red).
    _render_uptime_matrix(
        state_matrix,
        os.path.join(out_dir, f"{name_prefix}_uptime_matrix.png"),
        red_mask=None,
    )

    # 2b. Clustering uses the binary ACTIVE sequence (absent/offline both 0),
    # then the SAME ordering is applied to the three-state matrix for render.
    order = _correlation_distance_order(active_matrix)
    state_ordered = state_matrix[:, order]
    active_ordered = active_matrix[:, order]
    red_mask = _identical_run_mask(active_ordered, min_run=5)
    _render_uptime_matrix(
        state_ordered,
        os.path.join(out_dir, f"{name_prefix}_uptime_matrix_clustered.png"),
        red_mask=red_mask,
    )


def hsdir_uptime_distributions(df, context_label):
    """
    Entity x snapshot uptime-matrix visualization. For `df` (network-wide or
    tracked, post-validate_sort_data shape) and context_label ("network" or
    "tracked"), produces two matrix figures under analysis-results/uptime/:

      * {context_label}_uptime_matrix.png           -- plain status-only bitmap
        (black = active, white = inactive), columns in first-appearance order,
        no statistical processing.
      * {context_label}_uptime_matrix_clustered.png -- columns reordered by
        single-linkage clustering on a (1 - Pearson r) uptime-sequence
        distance, with runs of >=5 adjacent identical-sequence columns
        recolored solid red.

    For context_label == "tracked", entities follow the (fingerprint,
    service_type) overlap rule, and both figures are additionally produced per
    service_type ({service_type_lower}_uptime_matrix[_clustered].png).
    """
    if df is None or df.empty:
        print(f"[UPTIME-ERROR] Missing data slice for context '{context_label}'.")
        return
    if "fingerprint" not in df.columns:
        print(f"[UPTIME-INFO] No fingerprint column for {context_label}; skipping.")
        return

    out_dir = os.path.join("analysis-results", "uptime")
    df = _apply_ghost_filter(df)
    dedupe_st = (context_label == "tracked")

    print(f"\n[UPTIME] Uptime-matrix analysis — {context_label}")
    _uptime_matrix_for_slice(df, dedupe_st, out_dir, context_label, context_label)

    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ("None", "")
        )
        for st in service_types:
            sub = df[df["service_type"] == st]
            # Within a single service type the (fingerprint, service_type) key
            # collapses to fingerprint anyway; keep dedupe on for consistency.
            _uptime_matrix_for_slice(sub, True, out_dir, st.lower(), f"tracked — {st}")


# ---------------------------------------------------------------------------
# Hourly uptime durations
# ---------------------------------------------------------------------------

def _entity_day_durations(day_df, day_hours=None):
    """
    All uptime-duration values (decimal hours) for one entity on one day.

    Walks that entity's consensus rows in chronological hour order. A duration
    opens at the first ACTIVE_IN_RING hour and closes at the first subsequent
    hour where status is no longer ACTIVE_IN_RING (duration = that offline
    hour - the open hour). A later reconnection opens a NEW duration. If the
    entity stays ACTIVE_IN_RING from its first appearance through the day's
    last row with no offline event, that stretch's duration is
    last_active_hour - first_active_hour. Day boundaries are hard resets:
    this only ever sees one day's rows, so nothing crosses midnight.

    Zero-span-stretch rule: whenever a stretch would close with zero span --
    i.e. a single ACTIVE_IN_RING consensus with no offline row after it, which
    happens both for a relay seen exactly once all day AND for a reconnection
    that opens on the day's last row -- that 0 understates a relay that was
    demonstrably up for (at least) one consensus interval. Instead its
    duration is (next consensus hour that day) - (open hour), where "next
    consensus" is the snapshot immediately after the open hour in that day's
    actual consensus timeline (`day_hours`, which may be spaced 1h, 2h, etc. --
    the real cadence, not an assumed step). If the open hour is the day's very
    last consensus there is no next consensus to measure to; the duration is
    then floored to 1.0 rather than 0.0 (minimum-risk: it was online once).

    `day_hours` is the sorted list of that day's distinct consensus decimal
    hours; when omitted, a zero-span stretch falls back to the 1.0 floor.
    """
    rows = day_df.sort_values("_hour_num", kind="stable")
    hours = rows["_hour_num"].to_numpy()
    active = (rows["status"].astype(str) == "ACTIVE_IN_RING").to_numpy() \
        if "status" in rows.columns else np.ones(len(rows), dtype=bool)

    durations = []
    open_hour = None
    last_active_hour = None
    for h, a in zip(hours, active):
        if a:
            if open_hour is None:
                open_hour = h
            last_active_hour = h
        else:
            if open_hour is not None:
                durations.append(h - open_hour)
                open_hour = None
    # Still open at end of the entity's rows.
    if open_hour is not None:
        span = last_active_hour - open_hour
        # Zero-span final stretch: the stretch is a single active consensus
        # with no offline row after it -- either the relay's only appearance
        # all day, or a reconnection that opened on the day's last row. Measure
        # to the next consensus in the day's timeline; if the open hour is the
        # day's LAST consensus there is no next consensus, so floor to 1.0
        # rather than 0.0, since the relay was demonstrably online for (at
        # least) that one consensus -- a deliberate minimum-risk floor for the
        # otherwise-unmeasurable case. (A stretch with span > 0 already spans
        # real consensuses and is left exactly as measured.)
        if span == 0:
            later = [hh for hh in day_hours if hh > open_hour] if day_hours else []
            span = (later[0] - open_hour) if later else 1.0
        durations.append(span)
    return durations


def _pooled_daily_durations(df, dedupe_on_service_type):
    """
    {day -> list of all uptime durations} pooling every entity's every
    duration that day (the flat pool used for the per-day average and
    percentiles). Entities keyed by the overlap rule.
    """
    work = df.copy()
    work["_entity_key"] = _uptime_entity_key(work, dedupe_on_service_type)
    work["_hour_num"] = _hour_to_float(work["hour"])

    out = {}
    for day, day_df in work.groupby("date"):
        day_hours = sorted(day_df["_hour_num"].dropna().unique().tolist())
        pooled = []
        for _ent, ent_df in day_df.groupby("_entity_key"):
            pooled.extend(_entity_day_durations(ent_df, day_hours))
        out[day] = pooled
    return out


def _plot_hourly_uptimes(days, avg, p85, p50, p15, output_path):
    """Four-series per-day uptime-duration plot with the specified styling."""
    x = list(range(len(days)))
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x, avg, linestyle="-", marker="o", color=_series_color("average", 0), label="Daily average")
    ax.plot(x, p85, linestyle=":", marker="s", color=_series_color("p85", 1), label="85th percentile")
    ax.plot(x, p50, linestyle="--", marker="^", color=_series_color("p50", 2), label="50th percentile (median)")
    ax.plot(x, p15, linestyle="-.", marker="d", color=_series_color("p15", 3), label="15th percentile")

    ax.set_xticks(x)
    ax.set_xticklabels(days, rotation=45, ha="right", fontsize=8)
    ax.set_xlabel("Day")
    ax.set_ylabel("Uptime duration (decimal hours)")
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _hourly_uptimes_for_slice(df, dedupe_on_service_type, out_dir, name_prefix, label):
    """Compute per-day avg + percentiles from a flat duration pool, plot, and
    print the across-day mean of daily averages. Returns that mean (or None)."""
    if df is None or df.empty:
        print(f"[UPTIME-INFO] No data for {label}; skipping hourly uptimes.")
        return None
    daily = _pooled_daily_durations(df, dedupe_on_service_type)
    days = _chronological_days(df)

    avg, p85, p50, p15 = [], [], [], []
    for d in days:
        vals = daily.get(d, [])
        if vals:
            arr = np.asarray(vals, dtype=float)
            avg.append(float(arr.mean()))
            p85.append(float(np.percentile(arr, 85)))
            p50.append(float(np.percentile(arr, 50)))
            p15.append(float(np.percentile(arr, 15)))
        else:
            avg.append(0.0); p85.append(0.0); p50.append(0.0); p15.append(0.0)

    _plot_hourly_uptimes(
        days, avg, p85, p50, p15,
        os.path.join(out_dir, f"{name_prefix}_hourly_uptimes.png"),
    )

    overall = float(np.mean(avg)) if avg else 0.0
    print(f"    [{label}] average uptime across all days: {overall:.4f} decimal hours")
    return overall


def _onion_nested_hourly_uptimes(df, out_dir, st_lower, label):
    """
    Per-service-type per-onion nested daily uptime: for each day, average
    per-fingerprint durations WITHIN each onion first, then average those
    per-onion averages across onions -> the day's value (so a many-HSDir
    onion doesn't dominate a flat pool). Plots the same four series and
    prints the across-day mean of that plotted daily value.
    """
    if df is None or df.empty:
        print(f"[UPTIME-INFO] No data for {label}; skipping onion-nested hourly uptimes.")
        return
    if "mapped_onion" not in df.columns:
        print(f"[UPTIME-INFO] No mapped_onion for {label}; skipping onion-nested version.")
        return

    work = df.copy()
    work["_hour_num"] = _hour_to_float(work["hour"])
    days = _chronological_days(work)

    # Per (day, onion): the onion's per-fingerprint duration averages, and the
    # onion's daily average. Then per day we take avg/percentiles over the set
    # of per-onion daily averages. "Next consensus" for the single-appearance
    # rule is a day-level timeline, so derive each day's consensus hours from
    # the whole day's rows, not the onion subset.
    day_hours_map = {
        day: sorted(day_df["_hour_num"].dropna().unique().tolist())
        for day, day_df in work.groupby("date")
    }
    day_to_onion_avgs = {d: [] for d in days}
    for (day, onion), grp in work.groupby(["date", "mapped_onion"]):
        if onion in ("None", ""):
            continue
        day_hours = day_hours_map.get(day, [])
        onion_durations = []
        for _fp, fp_df in grp.groupby("fingerprint"):
            onion_durations.extend(_entity_day_durations(fp_df, day_hours))
        if onion_durations:
            day_to_onion_avgs.setdefault(day, []).append(float(np.mean(onion_durations)))

    avg, p85, p50, p15 = [], [], [], []
    for d in days:
        vals = day_to_onion_avgs.get(d, [])
        if vals:
            arr = np.asarray(vals, dtype=float)
            avg.append(float(arr.mean()))
            p85.append(float(np.percentile(arr, 85)))
            p50.append(float(np.percentile(arr, 50)))
            p15.append(float(np.percentile(arr, 15)))
        else:
            avg.append(0.0); p85.append(0.0); p50.append(0.0); p15.append(0.0)

    _plot_hourly_uptimes(
        days, avg, p85, p50, p15,
        os.path.join(out_dir, f"{st_lower}_onion_hourly_uptimes.png"),
    )

    overall = float(np.mean(avg)) if avg else 0.0
    print(f"    [{label}] average of onion hour-uptime averages across all days: "
          f"{overall:.4f} decimal hours")


def hsdir_hourly_uptime_distributions(df, context_label):
    """
    Per-day uptime-duration statistics (decimal hours) for relays/HSDirs. For
    `df` and context_label ("network" or "tracked"): computes, per day, every
    entity's uptime durations (open at first ACTIVE_IN_RING hour, close at the
    next offline hour, reconnections start fresh durations, day boundaries are
    hard resets), pools them, and plots the daily average plus the 85/50/15
    percentiles to analysis-results/uptime/{context_label}_hourly_uptimes.png,
    printing the across-day mean of the daily averages.

    For context_label == "tracked": repeats per service_type (overlap-rule
    entities) as {service_type_lower}_hourly_uptimes.png, and additionally
    produces the per-onion nested version
    ({service_type_lower}_onion_hourly_uptimes.png) where each day's value is
    the average across onions of each onion's own per-fingerprint duration
    average -- so onions with many HSDirs don't dominate.
    """
    if df is None or df.empty:
        print(f"[UPTIME-ERROR] Missing data slice for context '{context_label}'.")
        return
    if "fingerprint" not in df.columns:
        print(f"[UPTIME-INFO] No fingerprint column for {context_label}; skipping.")
        return

    out_dir = os.path.join("analysis-results", "uptime")
    df = _apply_ghost_filter(df)
    dedupe_st = (context_label == "tracked")

    print(f"\n[UPTIME] Hourly uptime-duration analysis — {context_label}")
    _hourly_uptimes_for_slice(df, dedupe_st, out_dir, context_label, context_label)

    if context_label == "tracked" and "service_type" in df.columns:
        # General per-onion nested version across ALL service types combined,
        # saved as tracked_onion_hourly_uptimes.png (the tracked-wide
        # counterpart of the per-service-type onion-nested plots below).
        _onion_nested_hourly_uptimes(df, out_dir, context_label, context_label)

        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ("None", "")
        )
        for st in service_types:
            sub = df[df["service_type"] == st]
            st_lower = st.lower()
            _hourly_uptimes_for_slice(sub, True, out_dir, st_lower, f"tracked — {st}")
            _onion_nested_hourly_uptimes(sub, out_dir, st_lower, f"tracked — {st}")


# ---------------------------------------------------------------------------
# Nickname-based distinct-value-change analyses
#
# Three deliverables that share one shape: group relays by nickname (the
# operator-chosen, non-unique label), and for each nickname count how many
# DISTINCT values of some other field it has shown across the whole
# observation window -- fingerprint, declared_family_id, or IP address/
# /24 subnet. A nickname cycling through many distinct fingerprints (or
# family IDs, or addresses) is a signal worth surfacing on its own, distinct
# from the sybil-distribution functions elsewhere in this file (which rank
# groups by unique-fingerprint POPULATION, not by how many distinct values
# of a field one identity has cycled through).
# ---------------------------------------------------------------------------

# Cap the sorted-rank plot to the N highest-count entities (the original,
# smaller-dataset design), or leave it uncapped and plot every entity. None =
# uncapped (current default -- at full-month scale, entity counts land in
# the same thousands-of-relays range the uncapped view was designed for).
# To re-enable capping, set this to an integer, e.g. 1000.
NICKNAME_CHANGES_RANK_PLOT_CAP = None


def _nickname_entity_key(df, dedupe_on_service_type):
    """
    Entity key Series for the nickname-based "X changes" analyses: the same
    overlap rule _uptime_entity_key/_prepare_uniformity_frame use, generalized
    from fingerprint to nickname -- (nickname, service_type) tuples when
    dedupe_on_service_type is set and service_type exists (so a node serving
    multiple service types under the same nickname is counted once per
    service type it serves), else nickname alone.
    """
    if dedupe_on_service_type and "service_type" in df.columns:
        return list(zip(df["nickname"], df["service_type"].astype(str)))
    return df["nickname"]


def _strict_ghost_filter(df):
    """
    The stricter consecutive_hourly_absences == 0 filter (NaN kept) -- the
    same deliberate deviation already established in
    hsdir_churn_distributions for state-change-counting functions, NOT the
    file's usual <= 1 ghost tolerance used everywhere else.
    """
    if "consecutive_hourly_absences" not in df.columns:
        return df
    cha = pd.to_numeric(df["consecutive_hourly_absences"], errors="coerce")
    return df[cha.isna() | (cha == 0)]


def _format_entity_label(entity_id):
    """
    Pretty-print an entity id for a top-5 printout: a plain nickname string
    as-is, or a (nickname, service_type) overlap-rule tuple as
    'nickname (service_type)' rather than a raw Python tuple repr.
    """
    if isinstance(entity_id, tuple) and len(entity_id) == 2:
        return f"{entity_id[0]} ({entity_id[1]})"
    return str(entity_id)


def _sorted_rank_plot(counts, output_path, y_label):
    """
    Sorted-rank plot: every entity's count, sorted ascending, plotted at
    x = rank (1..N), y = count, linear y-axis. This is NOT a categorical
    per-entity bar chart -- with hundreds/thousands of entities individual
    labels wouldn't be legible, and a rank axis is what actually shows the
    right-skewed shape (most entities barely change, a small tail changes
    constantly) as a flat-then-sharp-upswing curve. A linear axis keeps the
    actual counts directly readable (a log axis compresses the tail in a way
    that can misrepresent how much larger the highest counts really are).

    NICKNAME_CHANGES_RANK_PLOT_CAP (module-level toggle): None plots every
    entity uncapped (current default); set to an integer N to instead select
    only the N highest-count entities -- still chosen from the full,
    uncapped computation -- and plot just those, sorted ascending.
    """
    values = sorted(counts)
    if not values:
        return
    if NICKNAME_CHANGES_RANK_PLOT_CAP is not None and len(values) > NICKNAME_CHANGES_RANK_PLOT_CAP:
        values = values[-NICKNAME_CHANGES_RANK_PLOT_CAP:]
    x = list(range(1, len(values) + 1))

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(x, values, linestyle="-", color="black", linewidth=1.2)
    ax.set_xlabel("Rank (ascending count)")
    ax.set_ylabel(y_label)
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _distinct_value_changes_report_and_plot(df, group_col, counted_col, label, output_path,
                                             metric_label=None):
    """
    Shared logic for all three "sybil relay X changes" deliverables (the
    counting analog of _sybil_top5_report_and_plot's compute -> print top 5
    -> plot shape, but ranking by distinct-VALUE count of `counted_col`
    rather than unique-fingerprint population): for each distinct
    `group_col` value, count the number of distinct `counted_col` values it
    has had across `df`. Prints the top 5 (full entity id + absolute count,
    descending), then saves the sorted-rank plot of every entity's count.

    Rows where `counted_col` is missing/'None' are excluded before counting,
    same convention as the rest of this file (missing data isn't a
    legitimate distinct value).
    """
    if metric_label is None:
        metric_label = counted_col

    scoped = df[
        df[counted_col].notna()
        & (df[counted_col].astype(str) != "None")
        & (df[counted_col].astype(str) != "")
    ]
    if scoped.empty:
        print(f"[CHANGES-INFO] No usable '{metric_label}' values for {label}; skipping.")
        return

    counts = scoped.groupby(group_col)[counted_col].nunique()
    if counts.empty:
        print(f"[CHANGES-INFO] No qualifying entities for '{metric_label}' — {label}; skipping.")
        return

    top5 = counts.sort_values(ascending=False).head(5)
    print(f"\n[CHANGES] Top nicknames by distinct {metric_label} count — {label}")
    for entity_id, count in top5.items():
        print(f"    {_format_entity_label(entity_id)}: {count} distinct {metric_label}")

    _sorted_rank_plot(
        counts.tolist(), output_path,
        y_label=f"Observed distinct {metric_label}",
    )


def _run_nickname_changes_distributions(df, context_label, out_dir, variants):
    """
    Shared driver for all three "sybil relay X changes" deliverables: apply
    the strict ghost filter, drop rows with a missing/blank/default nickname,
    build the tracked-overlap-rule entity key, run the general case, then
    (for context_label == "tracked") repeat once per service_type -- factored
    once here so it's written a single time regardless of how many fields
    are being counted.

    `variants` is a list of (counted_col, metric_label, filename_stub)
    tuples: one entry for the fingerprint/family-id deliverables, two
    entries (IP address and /24 subnet) for the IP-changes deliverable.
    """
    if df is None or df.empty:
        print(f"[CHANGES-ERROR] Missing data slice for context '{context_label}'.")
        return
    if "nickname" not in df.columns:
        print(f"[CHANGES-INFO] No nickname column for {context_label}; skipping.")
        return

    df = _strict_ghost_filter(df)
    if df.empty:
        print(f"[CHANGES-INFO] No rows survive the ghost filter for {context_label}; skipping.")
        return

    def run_scope(sub_df, label, filename_prefix, dedupe):
        # "Unnamed" is Tor's literal fallback nickname string when an
        # operator never configures one -- it is not a real, distinguishing
        # identity, and since any number of unrelated relays can share it,
        # grouping by it would attribute many different operators' unrelated
        # fingerprint/family/IP changes to one fictitious "nickname". There is
        # no way to distinguish that fallback from someone deliberately
        # naming a relay "Unnamed", so it is excluded here the same way a
        # missing/blank nickname already is -- both mean "no real nickname".
        sub_df = sub_df[
            sub_df["nickname"].notna()
            & (sub_df["nickname"].astype(str) != "None")
            & (sub_df["nickname"].astype(str) != "")
            & (sub_df["nickname"].astype(str) != "Unnamed")
        ]
        if sub_df.empty:
            print(f"[CHANGES-INFO] No usable nicknames for {label}; skipping.")
            return
        sub_df = sub_df.copy()
        sub_df["_entity_key"] = _nickname_entity_key(sub_df, dedupe)
        for counted_col, metric_label, filename_stub in variants:
            if counted_col not in sub_df.columns:
                print(f"[CHANGES-INFO] Column '{counted_col}' missing for {label}; skipping {metric_label}.")
                continue
            _distinct_value_changes_report_and_plot(
                sub_df, "_entity_key", counted_col, label,
                os.path.join(out_dir, f"{filename_prefix}_{filename_stub}.png"),
                metric_label=metric_label,
            )

    run_scope(df, context_label, context_label, False)

    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ("None", "")
        )
        for st in service_types:
            sub = df[df["service_type"] == st]
            if sub.empty:
                print(f"[CHANGES-INFO] No rows for service_type '{st}'; skipping.")
                continue
            run_scope(sub, f"tracked — {st}", st.lower(), False)


def sybil_relay_fingerprint_changes(df, context_label):
    """
    For `df` (network-wide or tracked, post-validate_sort_data
    shape) and context_label ("network" or "tracked"): groups by plain
    nickname (no service-type dedup -- the general case is always computed
    over the whole population regardless of context_label, identical in
    shape for "network" and "tracked"), counts each entity's number of
    distinct fingerprint values across the whole window, prints the top 5,
    and saves a sorted-rank plot to
    analysis-results/sybil/fp_changes/{context_label}_fingerprint_changes.png.

    For context_label == "tracked", additionally repeats per service_type,
    saved to
    analysis-results/sybil/fp_changes/{service_type_lower}_fingerprint_changes.png.
    """
    out_dir = os.path.join("analysis-results", "sybil", "fp_changes")
    _run_nickname_changes_distributions(
        df, context_label, out_dir,
        variants=[("fingerprint", "fingerprint", "fingerprint_changes")],
    )


def sybil_relay_family_id_changes(df, context_label):
    """
    Identical logic to sybil_relay_fingerprint_changes, with
    one substitution: counts distinct declared_family_id values per nickname
    instead of distinct fingerprint values. Saved to
    analysis-results/sybil/famID_changes/{context_label}_famID_changes.png
    (and per service_type under "tracked":
    {service_type_lower}_famID_changes.png).
    """
    out_dir = os.path.join("analysis-results", "sybil", "famID_changes")
    _run_nickname_changes_distributions(
        df, context_label, out_dir,
        variants=[("declared_family_id", "declared_family_id", "famID_changes")],
    )


def sybil_relay_ip_changes(df, context_label):
    """
    Same logic again, in two parallel variants per nickname:
    distinct ip_address count, and distinct /24-subnet count (subnet derived
    via _to_24_subnet). Saved to analysis-results/sybil/ip_changes/ as
    {context_label}_ip_changes.png and {context_label}_subnet_24_changes.png
    (and per service_type under "tracked":
    {service_type_lower}_ip_changes.png /
    {service_type_lower}_subnet_24_changes.png).
    """
    out_dir = os.path.join("analysis-results", "sybil", "ip_changes")

    working = df.copy() if df is not None else df
    if working is not None and "ip_address" in working.columns:
        working["_subnet_24"] = working["ip_address"].apply(_to_24_subnet)

    _run_nickname_changes_distributions(
        working, context_label, out_dir,
        variants=[
            ("ip_address", "ip_address", "ip_changes"),
            ("_subnet_24", "/24 subnet", "subnet_24_changes"),
        ],
    )


# ---------------------------------------------------------------------------
# BPK (blinded-public-key / descriptor) upload–fetch persistence analysis
#
# Operates on the HSDir ring-event log (df_ring_events), NOT a consensus
# snapshot table: each row is one observed descriptor event with an `action`
# (UPLOADED / RECEIVED / FAILED) and, for failures, a `reason`. There is no
# consecutive_hourly_absences column and therefore NO ghost filter here.
#
# The grouping entity throughout is the composite descriptor key
# (descriptor_id_b64, onion_address) -- referred to as a "descriptor" in
# labels. descriptor_id_b64 alone is NOT used because the raw data has bpk
# collisions (the same id logged for multiple onions, even simultaneously --
# an upstream defect, since a true V3 blinded key derives from one onion);
# pairing the id with onion_address restores one descriptor per onion, so
# each entity has a single service_type and per-type counts reconcile. Three
# deliverables share one per-descriptor scan:
#   * bpks_uf_persistance          -- per-descriptor TOTAL upload/fetch counts
#   * bpks_hour_uf_persistance     -- per-descriptor DISTINCT (date, hour)
#                                     bucket counts with an upload/fetch
#   * bpks_not_upload_fetch_distributions -- per-descriptor silent-failure cats
#
# context_label is effectively always "tracked" (ring events exist only for
# tracked onions); it is kept as a parameter for interface parity with the
# rest of the file, and the per-service-type breakdown runs under "tracked".
# ---------------------------------------------------------------------------

# Upload/fetch histograms are heavily right-skewed (a small number of bpks
# re-upload far more than the bulk -- the reference material itself notes
# outliers re-published thousands of times). Clip the histogram's upper bin
# edge to this value so a handful of extreme bpks don't flatten the whole
# distribution against the x-axis; counts at or above it fall in the last
# bin. Terminal output still reports the true unclipped extremes.
BPK_HIST_CLIP = 50


def _k_suffix_formatter():
    """
    Shared y-axis tick formatter rendering counts in thousands with a 'K'
    suffix (e.g. 40000 -> '40K', 500 -> '0.5K'), via matplotlib's
    FuncFormatter. One instance per axis (a formatter can't be shared across
    multiple axes), so this returns a fresh formatter each call.
    """
    return mticker.FuncFormatter(lambda v, _pos: f"{v/1000:g}K")


def _bpk_event_counts(df):
    """
    Single shared per-descriptor scan over the ring-event DataFrame, so it is
    grouped once for all the descriptor-hosting analyses rather than repeatedly.

    Grouping unit is the composite descriptor key (descriptor_id_b64,
    onion_address), not descriptor_id_b64 alone: in the real data one
    descriptor_id_b64 is logged under multiple onion_addresses (a bpk
    "collision", impossible for a true V3 blinded key and hence an upstream
    defect). Pairing the id with onion_address restores one descriptor per
    onion, so each composite belongs to one service_type and the
    per-service-type counts partition cleanly. Output labels call a
    (bpk, onion) pair a "descriptor".

    Returns a DataFrame indexed by the composite key (index name
    '_descriptor_key') with columns:
      * uploads             total UPLOADED rows
      * failed_uploads      total FAILED rows whose reason == UPLOAD_REJECTED
      * fetches             total RECEIVED rows
      * uploaded_hours      distinct (date, hour) buckets with >=1 UPLOADED
      * failed_upload_hours distinct (date, hour) buckets with >=1
                            FAILED+UPLOAD_REJECTED
      * fetched_hours       distinct (date, hour) buckets with >=1 RECEIVED
    Every descriptor appearing under any action gets a row (missing
    categories are 0).
    """
    work = df.copy()
    action = work["action"].astype(str)
    reason = work["reason"].astype(str) if "reason" in work.columns else pd.Series("None", index=work.index)

    is_upload = action == "UPLOADED"
    is_fetch = action == "RECEIVED"
    is_failed_upload = (action == "FAILED") & (reason == "UPLOAD_REJECTED")

    work["_is_upload"] = is_upload
    work["_is_fetch"] = is_fetch
    work["_is_failed_upload"] = is_failed_upload
    work["_bucket"] = list(zip(work["date"], work["hour"]))
    # Composite descriptor key: (descriptor_id_b64, onion_address).
    work["_descriptor_key"] = list(zip(work["descriptor_id_b64"], work["onion_address"]))

    all_keys = pd.Index(work["_descriptor_key"].unique(), name="_descriptor_key")

    def total_counts(mask, name):
        return work[mask].groupby("_descriptor_key").size().reindex(all_keys, fill_value=0).rename(name)

    def distinct_hour_counts(mask, name):
        return (
            work[mask].groupby("_descriptor_key")["_bucket"].nunique()
            .reindex(all_keys, fill_value=0).rename(name)
        )

    counts = pd.concat([
        total_counts(work["_is_upload"], "uploads"),
        total_counts(work["_is_failed_upload"], "failed_uploads"),
        total_counts(work["_is_fetch"], "fetches"),
        distinct_hour_counts(work["_is_upload"], "uploaded_hours"),
        distinct_hour_counts(work["_is_failed_upload"], "failed_upload_hours"),
        distinct_hour_counts(work["_is_fetch"], "fetched_hours"),
    ], axis=1).fillna(0).astype(int)

    return counts


def _overlaid_uf_histogram(upload_vals, fetch_vals, output_path, title, x_label,
                           failed_vals=None, failed_label="Failed uploads per descriptor"):
    """
    Overlaid histograms on one figure with shared bins and transparency:
    uploads/uploaded-hours in blue, fetches/fetched-hours in orange, and --
    when `failed_vals` is provided -- failed-uploads/failed-upload-hours in
    green as a third series. Right-skew is handled by clipping values into
    [0, BPK_HIST_CLIP] (values at or above the clip land in the final bin)
    so outliers don't dominate the x-axis; the y-axis uses the shared
    K-suffix formatter.
    """
    up = np.clip(np.asarray(upload_vals, dtype=float), 0, BPK_HIST_CLIP)
    fe = np.clip(np.asarray(fetch_vals, dtype=float), 0, BPK_HIST_CLIP)
    fa = np.clip(np.asarray(failed_vals, dtype=float), 0, BPK_HIST_CLIP) \
        if failed_vals is not None else None
    if up.size == 0 and fe.size == 0 and (fa is None or fa.size == 0):
        print(f"[BPK-INFO] No descriptor data for '{title}'; skipping histogram.")
        return

    hi = max(
        up.max() if up.size else 0,
        fe.max() if fe.size else 0,
        fa.max() if fa is not None and fa.size else 0,
        1,
    )
    bins = np.linspace(0, hi, min(int(hi) + 1, 51))

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.hist(up, bins=bins, color="blue", alpha=0.55, label="Uploads per descriptor")
    ax.hist(fe, bins=bins, color="orange", alpha=0.55, label="Fetches per descriptor")
    if fa is not None:
        ax.hist(fa, bins=bins, color="green", alpha=0.55, label=failed_label)
    ax.set_xlabel(x_label)
    ax.set_ylabel("Number of descriptors")
    ax.yaxis.set_major_formatter(_k_suffix_formatter())
    ax.legend(loc="upper right")
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _iter_ring_scopes(df, context_label):
    """
    Yield (scope_df, label, filename_prefix) for the general context and,
    when context_label == "tracked" and service_type exists, each non-empty
    service type. Shared by all three deliverables' scope loops.
    """
    yield df, context_label, context_label
    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ("None", "")
        )
        for st in service_types:
            sub = df[df["service_type"] == st]
            if sub.empty:
                print(f"[BPK-INFO] No ring events for service_type '{st}'; skipping.")
                continue
            yield sub, f"tracked — {st}", st.lower()


def bpks_uf_persistance(df, context_label):
    """
    Per-bpk TOTAL upload and fetch event counts, drawn as an
    overlaid pair of histograms (uploads-per-bpk blue, fetches-per-bpk
    orange) over all bpks -- i.e. how many bpks had N uploads vs. how many
    had N fetches, NOT one bar per bpk. Saved to
    analysis-results/bpks/uf_persistance/{context_label}_bpks_upload_fetch_persistance.png,
    and, under "tracked", once per service_type as
    {service_type_lower}_bpks_upload_fetch_persistance.png.

    failed_uploads is computed here (it feeds silent-failure detection) but is not a
    third plotted series -- the reference two-series design is uploads vs.
    fetches only, and a plotted failed-upload series was found to clutter
    the figure; a short failed-upload summary is printed instead.
    """
    if df is None or df.empty:
        print(f"[BPK-ERROR] Missing/empty ring-event data for '{context_label}'.")
        return
    out_dir = os.path.join("analysis-results", "bpks", "uf_persistance")

    print(f"\n[BPK] Upload/fetch persistence (per-descriptor totals) — {context_label}")
    for scope_df, label, prefix in _iter_ring_scopes(df, context_label):
        counts = _bpk_event_counts(scope_df)
        if counts.empty:
            print(f"[BPK-INFO] No descriptors for {label}; skipping.")
            continue

        total_failed = int(counts["failed_uploads"].sum())
        bpks_with_failed = int((counts["failed_uploads"] > 0).sum())
        print(f"    [{label}] {len(counts)} descriptors | failed uploads: {total_failed} events "
              f"across {bpks_with_failed} descriptors | max uploads/descriptor: {int(counts['uploads'].max())}, "
              f"max fetches/descriptor: {int(counts['fetches'].max())}")

        _overlaid_uf_histogram(
            counts["uploads"].to_numpy(), counts["fetches"].to_numpy(),
            os.path.join(out_dir, f"{prefix}_bpks_upload_fetch_persistance.png"),
            title=f"Per-descriptor upload/fetch counts — {label}",
            x_label="Number of uploads / fetches",
        )


def bpks_hour_uf_persistance(df, context_label):
    """
    Per-bpk DISTINCT (date, hour) bucket counts: for each bpk,
    how many distinct hourly consensus snapshots it had at least one upload
    in (uploaded_hours) vs. at least one fetch in (fetched_hours). "Hour
    bucket" = a distinct (date, hour) pair across the whole window, NOT one
    of 24 recurring hour-of-day slots -- a bpk uploaded at 14:00 on ten days
    counts as ten distinct buckets, not one. Same overlaid-histogram design
    as the total-counts variant. Saved to
    analysis-results/bpks/uf_persistance/{context_label}_bpks_upload_fetch_hour_persistance.png
    (and per service_type under "tracked").

    Shares the _bpk_event_counts scan with the total-counts variant so df_ring_events is
    grouped once; kept a separate public function because its metric (distinct
    hour buckets) genuinely differs from A's (total event counts).
    """
    if df is None or df.empty:
        print(f"[BPK-ERROR] Missing/empty ring-event data for '{context_label}'.")
        return
    out_dir = os.path.join("analysis-results", "bpks", "uf_persistance")

    print(f"\n[BPK] Upload/fetch persistence (distinct hour buckets) — {context_label}")
    for scope_df, label, prefix in _iter_ring_scopes(df, context_label):
        counts = _bpk_event_counts(scope_df)
        if counts.empty:
            print(f"[BPK-INFO] No descriptors for {label}; skipping.")
            continue

        print(f"    [{label}] {len(counts)} descriptors | max uploaded-hours/descriptor: "
              f"{int(counts['uploaded_hours'].max())}, max fetched-hours/descriptor: "
              f"{int(counts['fetched_hours'].max())}")

        _overlaid_uf_histogram(
            counts["uploaded_hours"].to_numpy(), counts["fetched_hours"].to_numpy(),
            os.path.join(out_dir, f"{prefix}_bpks_upload_fetch_hour_persistance.png"),
            title=f"Per-descriptor distinct upload/fetch hour buckets — {label}",
            x_label="Number of distinct hours uploaded / fetched",
        )


def _plot_silent_failure_bars(categories_by_label, output_path):
    """
    Grouped bar chart of the two silent-failure categories. For a single
    scope, `categories_by_label` = {label: (uploaded_never_fetched,
    fetched_never_uploaded)} with one entry; for the per-service-type
    combined figure it has one entry per service type, clustered.
    """
    labels = list(categories_by_label.keys())
    cat_names = ["Uploaded, never fetched", "Fetched, never uploaded"]
    x = np.arange(len(cat_names))
    n = len(labels)
    width = 0.8 / max(n, 1)

    fig, ax = plt.subplots(figsize=(9, 6))
    for i, label in enumerate(labels):
        vals = categories_by_label[label]
        offset = (i - (n - 1) / 2) * width
        ax.bar(x + offset, vals, width=width, color=_series_color(label, i), label=str(label))

    ax.set_xticks(list(x))
    ax.set_xticklabels(cat_names)
    ax.set_ylabel("Number of descriptors")
    if n > 1:
        ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _write_silent_failure_debug_csv(counts, output_path):
    """Per-descriptor diagnostic table written when a scope has no
    silent-failure signal, so an empty result can be checked for a join/field
    bug vs. a genuine absence. The composite index is split back into its
    descriptor_id_b64 and onion_address components for readability."""
    table = counts.reset_index().copy()
    # _descriptor_key is a (descriptor_id_b64, onion_address) tuple column.
    table["descriptor_id_b64"] = table["_descriptor_key"].map(lambda k: k[0])
    table["onion_address"] = table["_descriptor_key"].map(lambda k: k[1])
    table = table[
        ["descriptor_id_b64", "onion_address", "uploads", "failed_uploads", "fetches"]
    ]
    table["has_upload"] = table["uploads"] > 0
    table["has_fetch"] = table["fetches"] > 0
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    table.to_csv(output_path, index=False)
    print(f"    --> Wrote diagnostic CSV: {output_path}")


def bpks_not_upload_fetch_distributions(df, context_label):
    """
    Silent-failure detection over per-bpk upload/fetch presence. Two
    categories per bpk:
      * uploaded-never-fetched : has_upload and not has_fetch
      * fetched-never-uploaded : has_fetch and not has_upload

    A bpk (blinded public key) derives from one onion address, and each onion
    has one service_type, so has_upload/has_fetch is judged over all of a
    bpk's events and its service_type is fixed. The per-service-type
    breakdown attributes each globally-judged bpk to its own service_type
    rather than re-deciding presence from that type's rows alone, so per-type
    counts partition the bpks and sum to the tracked totals.

    Data-integrity check: any bpk observed under more than one service_type
    or onion_address is reported as a warning (it should be impossible for
    valid data); if it fires, per-type sums may not reconcile.

    Prints per-category count and percentage of unique bpks. When both
    categories are empty for a scope, skips the plot, prints a "no signal"
    notice, and writes a per-bpk diagnostic CSV. Under "tracked", repeats per
    service_type into one clustered grouped-bar figure.
    """
    if df is None or df.empty:
        print(f"[BPK-ERROR] Missing/empty ring-event data for '{context_label}'.")
        return
    out_dir = os.path.join("analysis-results", "bpks", "uf_failure")

    print(f"\n[BPK] Silent-failure detection — {context_label}")

    # --- bpk-collision note: a true V3 blinded key is derived from one onion,
    # so a bare descriptor_id_b64 appearing under multiple onions/service types
    # is an upstream data defect (collision). This analysis groups on the
    # composite (descriptor_id_b64, onion_address) key, which splits those
    # collisions back into per-onion descriptors -- so per-type counts DO
    # reconcile with the tracked totals regardless. The counts below are
    # informational (how much collision the raw data has), not a correctness
    # warning about this analysis. onion->service_type stays 1:1 (verified),
    # which is what the composite key relies on. ---
    if "service_type" in df.columns:
        st_per_bpk = df.groupby("descriptor_id_b64")["service_type"].nunique()
        multi_st = int((st_per_bpk > 1).sum())
        if multi_st:
            print(f"    [BPK-COLLISION NOTE] {multi_st} raw descriptor_id_b64 value(s) span "
                  f"more than one service_type upstream; the composite (bpk, onion) key "
                  f"resolves these into per-onion descriptors, so per-type sums still reconcile.")
    if "onion_address" in df.columns:
        onion_per_bpk = df.groupby("descriptor_id_b64")["onion_address"].nunique()
        multi_onion = int((onion_per_bpk > 1).sum())
        if multi_onion:
            print(f"    [BPK-COLLISION NOTE] {multi_onion} raw descriptor_id_b64 value(s) map "
                  f"to more than one onion_address upstream (bpk collision); split per-onion "
                  f"by the composite key.")
        # The composite key assumes onion -> service_type is 1:1; verify.
        if "service_type" in df.columns:
            st_per_onion = df.groupby("onion_address")["service_type"].nunique()
            bad_onions = int((st_per_onion > 1).sum())
            if bad_onions:
                print(f"    [DATA-INTEGRITY WARNING] {bad_onions} onion_address(es) span more "
                      f"than one service_type — breaks the composite-key assumption; per-type "
                      f"attribution may be ambiguous. Investigate upstream.")

    # --- Global per-descriptor counts (over ALL events), judged once. ---
    counts = _bpk_event_counts(df)
    total_bpks = len(counts)
    has_upload = counts["uploads"] > 0
    has_fetch = counts["fetches"] > 0
    uploaded_never_fetched_mask = has_upload & ~has_fetch
    fetched_never_uploaded_mask = has_fetch & ~has_upload

    unf = int(uploaded_never_fetched_mask.sum())
    fnu = int(fetched_never_uploaded_mask.sum())

    def pct(n, denom):
        return (n / denom) if denom else 0.0

    print(f"    [{context_label}] {total_bpks} unique descriptors")
    print(f"        uploaded, never fetched: {unf} ({pct(unf, total_bpks):.2%})")
    print(f"        fetched, never uploaded: {fnu} ({pct(fnu, total_bpks):.2%})")

    if unf == 0 and fnu == 0:
        print(f"    [{context_label}] no silent-failure signal found; writing diagnostic CSV.")
        _write_silent_failure_debug_csv(
            counts, os.path.join(out_dir, f"{context_label}_bpks_not_upload_fetch_debug.csv"))
    else:
        _plot_silent_failure_bars(
            {context_label: (unf, fnu)},
            os.path.join(out_dir, f"{context_label}_bpks_not_upload_fetch.png"),
        )

    # --- Per-service-type: attribute each globally-flagged bpk to its own
    # service_type (one map lookup), so the per-type counts sum to tracked. ---
    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ("None", "")
        )
        # Each composite descriptor key is (bpk, onion_address); its
        # service_type is that onion's service_type (every onion maps to
        # exactly one). Map onion -> service_type, then look up each key's
        # onion (the 2nd tuple element).
        onion_to_st = (
            df[["onion_address", "service_type"]]
            .drop_duplicates("onion_address")
            .set_index("onion_address")["service_type"]
        )
        st_of = counts.index.to_series().map(lambda k: onion_to_st.get(k[1]))

        combined = {}
        for st in service_types:
            in_st = (st_of == st)
            st_total = int(in_st.sum())
            if st_total == 0:
                print(f"[BPK-INFO] No descriptors for service_type '{st}'; skipping.")
                continue
            st_unf = int((uploaded_never_fetched_mask & in_st).sum())
            st_fnu = int((fetched_never_uploaded_mask & in_st).sum())
            print(f"    [tracked — {st}] {st_total} unique descriptors | "
                  f"uploaded-never-fetched: {st_unf} ({pct(st_unf, st_total):.2%}) | "
                  f"fetched-never-uploaded: {st_fnu} ({pct(st_fnu, st_total):.2%})")

            if st_unf == 0 and st_fnu == 0:
                print(f"    [tracked — {st}] no silent-failure signal; writing diagnostic CSV.")
                _write_silent_failure_debug_csv(
                    counts[in_st.values],
                    os.path.join(out_dir, f"{st.lower()}_bpks_not_upload_fetch_debug.csv"))
                continue
            combined[st] = (st_unf, st_fnu)

        if combined:
            _plot_silent_failure_bars(
                combined,
                os.path.join(out_dir, "service_types_bpks_not_upload_fetch.png"),
            )


# ---------------------------------------------------------------------------
# Undeclared-family mutual-graph analysis
#
# Builds a MUTUAL (bidirectional) family graph from the list-valued
# `declared_family` field and classifies focus nodes by the declared-family-ID
# status of their confirmed mutual neighbors. Two deliverables share one graph
# per scope:
#   * No-ID-focused:   focus = nodes with declared_family_id "None"
#   * With-ID-focused: focus = nodes with a declared_family_id F,
#                                      excluding same-F neighbors first
# Each produces three mutually-exclusive categories (mixed / only-with-ID /
# only-without-ID). Per-day plots + per-day averages, plus a month-wide
# whole-scope printout (no plot).
#
# Ghost filter is the stricter consecutive_hourly_absences == 0 (NaN kept),
# matching the churn / change-count precedent, NOT the file's usual <= 1.
# ---------------------------------------------------------------------------

_UNDECLARED_CATEGORIES = ("mixed", "only_with_id", "only_without_id")
_UNDECLARED_CATEGORY_LABELS = {
    "mixed": "Mixed",
    "only_with_id": "Only-with-ID",
    "only_without_id": "Only-without-ID",
}


def _undeclared_ghost_filter(df):
    """Stricter == 0 presence filter (NaN kept)."""
    if "consecutive_hourly_absences" not in df.columns:
        return df
    cha = pd.to_numeric(df["consecutive_hourly_absences"], errors="coerce")
    return df[cha.isna() | (cha == 0)]


def _build_mutual_family_graph(scope_df):
    """
    Build the mutual (bidirectional) family graph for one scope's rows (a day,
    or the whole month). `scope_df` is already ghost-filtered.

    Node population: every distinct fingerprint present as its own row in
    scope_df. A fingerprint's declared_family is the UNION of every list
    observed for it across all rows in the scope (confirmed
    union resolution), self-references dropped.

    A mutual edge A<->B exists iff B is in A's family union AND A is in B's
    family union AND both A and B are present nodes. A one-sided listing, or a
    listing toward a fingerprint that never appears as its own row, is
    automatically not mutual (nothing to confirm it against).

    Returns (adjacency, present) where:
      * adjacency: dict fingerprint -> set of mutual-neighbor fingerprints
        (only present nodes appear as keys; a node with no mutual edge maps
        to an empty set).
      * present:  set of all present fingerprints (the node population).
    """
    present = set(scope_df["fingerprint"].unique())

    # Union of declared_family per fingerprint (drop self-references).
    fam_union = {}
    for fp, fam in zip(scope_df["fingerprint"], scope_df["declared_family"]):
        if not isinstance(fam, list):
            continue
        acc = fam_union.setdefault(fp, set())
        for other in fam:
            if other != fp:
                acc.add(other)

    adjacency = {fp: set() for fp in present}
    for a, a_fam in fam_union.items():
        if a not in present:
            continue
        for b in a_fam:
            # Mutual check: b present, b lists a back. a<b guard avoids
            # doing the pair twice, then we add both directions.
            if b in present and b in fam_union and a in fam_union[b]:
                adjacency[a].add(b)
                adjacency[b].add(a)
    return adjacency, present


def _scope_family_id_map(scope_df, most_recent=False):
    """
    fingerprint -> its declared_family_id (as a plain str, "None" when unset).

    most_recent=False (per-day): a day's rows are effectively one status per
    node; take the first non-null seen (they don't vary meaningfully within a
    day). most_recent=True (month-wide): a node's ID can change across the
    month, so use the most-recently-observed value -- scope_df is already in
    chronological order post validate_sort_data, so the last row wins.
    """
    id_map = {}
    if most_recent:
        for fp, fid in zip(scope_df["fingerprint"], scope_df["declared_family_id"]):
            id_map[fp] = str(fid)  # last write wins -> most recent
    else:
        for fp, fid in zip(scope_df["fingerprint"], scope_df["declared_family_id"]):
            if fp not in id_map or id_map[fp] == "None":
                id_map[fp] = str(fid)
    return id_map


def _classify_node_no_id(node, adjacency, id_map):
    """
    No-ID-focused classification for a focus node (declared_family_id "None").
    Returns one of _UNDECLARED_CATEGORIES, or None if the node has zero mutual
    neighbors (excluded from all metrics).
    """
    neighbors = adjacency.get(node, set())
    if not neighbors:
        return None
    any_with = any(id_map.get(n, "None") != "None" for n in neighbors)
    any_without = any(id_map.get(n, "None") == "None" for n in neighbors)
    if any_with and any_without:
        return "mixed"
    if any_with:
        return "only_with_id"
    return "only_without_id"


def _classify_node_with_id(node, adjacency, id_map, focus_id):
    """
    With-ID-focused classification for a focus node whose own family ID is
    focus_id (!= "None"). Same-family-ID mutual neighbors are excluded first
    (an intra-family mutual link is expected, not an undeclared signal).
    Returns one of _UNDECLARED_CATEGORIES, or None if zero remaining neighbors.
    """
    neighbors = [n for n in adjacency.get(node, set())
                 if id_map.get(n, "None") != focus_id]
    if not neighbors:
        return None
    any_with = any(id_map.get(n, "None") != "None" for n in neighbors)
    any_without = any(id_map.get(n, "None") == "None" for n in neighbors)
    if any_with and any_without:
        return "mixed"
    if any_with:
        return "only_with_id"
    return "only_without_id"


def _service_type_multiplicity(scope_df, restrict_fps=None):
    """
    fingerprint -> number of distinct service types it served in scope_df
    (the overlap-rule weight, applied at tally time). If restrict_fps is
    given, only those fingerprints are counted (weight 0/absent otherwise).
    When service_type is absent (network context), every fingerprint weighs 1.
    """
    if "service_type" not in scope_df.columns:
        fps = restrict_fps if restrict_fps is not None else set(scope_df["fingerprint"].unique())
        return {fp: 1 for fp in fps}
    sub = scope_df
    if restrict_fps is not None:
        sub = sub[sub["fingerprint"].isin(restrict_fps)]
    return sub.groupby("fingerprint")["service_type"].nunique().to_dict()


def _tally_scope(scope_df, id_map, adjacency, present, focus_fps, classifier,
                 weight_map):
    """
    Tally one scope (day or month) for one deliverable into category counts.

    focus_fps: the set of fingerprints in the focus population for this
      deliverable (already filtered to the classified node set for a
      per-service-type breakdown).
    classifier: callable(node) -> category or None.
    weight_map: fingerprint -> overlap-rule weight (contribution to the
      count). For the tracked combined case this is the service-type
      multiplicity; for network / per-service-type it's 1 per node.

    Returns {category: count}.
    """
    counts = {c: 0 for c in _UNDECLARED_CATEGORIES}
    for fp in focus_fps:
        cat = classifier(fp)
        if cat is None:
            continue
        counts[cat] += weight_map.get(fp, 1)
    return counts


def _undeclared_counts_for_scope(scope_df, weight_restrict_fps=None,
                                 most_recent_id=False):
    """
    Build the graph + id map once for a scope and return both deliverables'
    category counts: (counts_no_id, counts_with_id).

    weight_restrict_fps: for a per-service-type breakdown, the set of
      fingerprints that served that service type in the scope -- only these
      are classified/counted, but the graph is the FULL scope graph (
      scoping resolution). None -> classify all present nodes with the
      overlap-rule weight (tracked combined) or weight 1 (network).
    """
    adjacency, present = _build_mutual_family_graph(scope_df)
    id_map = _scope_family_id_map(scope_df, most_recent=most_recent_id)

    # Focus populations (restricted to the countable node set if given).
    countable = present if weight_restrict_fps is None else (present & weight_restrict_fps)
    no_id_focus = {fp for fp in countable if id_map.get(fp, "None") == "None"}
    with_id_focus = {fp for fp in countable if id_map.get(fp, "None") != "None"}

    # Weights: per-service-type breakdown counts each node once (weight 1);
    # otherwise use service-type multiplicity (network -> all 1s).
    if weight_restrict_fps is None:
        weight_map = _service_type_multiplicity(scope_df)
    else:
        weight_map = {fp: 1 for fp in countable}

    counts_no_id = _tally_scope(
        scope_df, id_map, adjacency, present, no_id_focus,
        lambda fp: _classify_node_no_id(fp, adjacency, id_map), weight_map)
    counts_with_id = _tally_scope(
        scope_df, id_map, adjacency, present, with_id_focus,
        lambda fp: _classify_node_with_id(fp, adjacency, id_map, id_map.get(fp, "None")),
        weight_map)
    return counts_no_id, counts_with_id


def _plot_undeclared(days, per_day_counts, output_path):
    """
    One solid line per category (mixed / only-with-ID / only-without-ID) over
    days, absolute counts. per_day_counts: list aligned with `days`, each a
    {category: count} dict.
    """
    x = list(range(len(days)))
    fig, ax = plt.subplots(figsize=(max(11, len(days) * 0.6), 6))
    for i, cat in enumerate(_UNDECLARED_CATEGORIES):
        y = [per_day_counts[d].get(cat, 0) for d in range(len(days))]
        ax.plot(x, y, linestyle="-", linewidth=1.2,
                color=_series_color(cat, i), label=_UNDECLARED_CATEGORY_LABELS[cat])
    ax.set_xticks(x)
    ax.set_xticklabels(days, rotation=45, ha="right", fontsize=8)
    ax.set_xlabel("Day")
    ax.set_ylabel("Node count")
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
    fig.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"    --> Saved plot: {output_path}")


def _print_undeclared_averages(per_day_counts, n_days, label, deliverable):
    """Per-metric average per day (sum over days / number of days)."""
    print(f"    [{label}] {deliverable} — average per day:")
    for cat in _UNDECLARED_CATEGORIES:
        total = sum(per_day_counts[d].get(cat, 0) for d in range(len(per_day_counts)))
        avg = (total / n_days) if n_days else 0.0
        print(f"        {_UNDECLARED_CATEGORY_LABELS[cat]}: {avg:.4f}")


def _print_undeclared_monthwide(counts_no_id, counts_with_id, label):
    """Six month-wide absolute counts, clearly labeled as whole-scope totals."""
    print(f"    [{label}] [Month-wide, all consensuses] no-ID-focused — "
          + ", ".join(f"{_UNDECLARED_CATEGORY_LABELS[c]}: {counts_no_id[c]}"
                      for c in _UNDECLARED_CATEGORIES))
    print(f"    [{label}] [Month-wide, all consensuses] with-ID-focused — "
          + ", ".join(f"{_UNDECLARED_CATEGORY_LABELS[c]}: {counts_with_id[c]}"
                      for c in _UNDECLARED_CATEGORIES))


def _run_undeclared_scope(df, label, filename_prefix, out_dir,
                          restrict_service_type=None):
    """
    Run both deliverables for one scope label (a context, or one service
    type). Produces the two per-day plots, per-day averages, and the
    month-wide printout.

    restrict_service_type: None for the general/combined case; a service-type
      string for a per-service-type breakdown (classify only nodes that
      served it, using the full graph, weight 1 each).
    """
    days = _chronological_days(df)
    if not days:
        print(f"[UNDECLARED-INFO] No days for {label}; skipping.")
        return

    per_day_no_id, per_day_with_id = [], []
    for day in days:
        day_df = df[df["date"] == day]
        if restrict_service_type is not None:
            restrict_fps = set(day_df[day_df["service_type"] == restrict_service_type]["fingerprint"].unique())
            if not restrict_fps:
                per_day_no_id.append({c: 0 for c in _UNDECLARED_CATEGORIES})
                per_day_with_id.append({c: 0 for c in _UNDECLARED_CATEGORIES})
                continue
            c_no, c_with = _undeclared_counts_for_scope(day_df, weight_restrict_fps=restrict_fps)
        else:
            c_no, c_with = _undeclared_counts_for_scope(day_df)
        per_day_no_id.append(c_no)
        per_day_with_id.append(c_with)

    _plot_undeclared(
        days, per_day_no_id,
        os.path.join(out_dir, f"{filename_prefix}_undeclared_families_no_ID.png"))
    _plot_undeclared(
        days, per_day_with_id,
        os.path.join(out_dir, f"{filename_prefix}_undeclared_families_with_ID.png"))

    n_days = len(days)
    print(f"\n[UNDECLARED] {label} — per-day averages ({n_days} days)")
    _print_undeclared_averages(per_day_no_id, n_days, label, "no-ID-focused")
    _print_undeclared_averages(per_day_with_id, n_days, label, "with-ID-focused")

    # Month-wide: one combined graph over the whole scope df.
    if restrict_service_type is not None:
        restrict_fps = set(df[df["service_type"] == restrict_service_type]["fingerprint"].unique())
        mc_no, mc_with = _undeclared_counts_for_scope(
            df, weight_restrict_fps=restrict_fps, most_recent_id=True)
    else:
        mc_no, mc_with = _undeclared_counts_for_scope(df, most_recent_id=True)
    _print_undeclared_monthwide(mc_no, mc_with, label)


def undeclared_families_distribution(df, context_label):
    """
    Undeclared-family mutual-graph analysis. Builds a per-day mutual
    (bidirectional) family graph from `declared_family`, classifies focus
    nodes by their mutual neighbors' declared_family_id status into three
    categories, and produces:
      * No-ID-focused: focus = declared_family_id == "None".
      * With-ID-focused: focus = declared_family_id != "None",
        same-family-ID neighbors excluded first.
    Per-day line plots (one per deliverable) + per-day averages, plus a
    month-wide whole-scope six-count printout (no plot).

    General case runs for both "network" and "tracked". For "tracked",
    repeats per service_type using the FULL day's/month's graph but
    classifying only nodes that served that service type; the
    combined "tracked" run applies the overlap rule at tally time (a node
    serving N service types contributes N to its category).

    Ghost filter is the stricter consecutive_hourly_absences == 0 (NaN kept).
    """
    out_dir = os.path.join("analysis-results", "undeclared-families")

    if df is None or df.empty:
        print(f"[UNDECLARED-ERROR] Missing data slice for context '{context_label}'.")
        return
    required = {"fingerprint", "declared_family", "declared_family_id", "date"}
    if not required.issubset(df.columns):
        print(f"[UNDECLARED-INFO] Required columns missing for {context_label}; skipping.")
        return

    df = _undeclared_ghost_filter(df)
    if df.empty:
        print(f"[UNDECLARED-INFO] No rows survive the ghost filter for {context_label}; skipping.")
        return

    print(f"\n[UNDECLARED] Undeclared-family analysis — {context_label}")
    _run_undeclared_scope(df, context_label, context_label, out_dir)

    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ("None", "")
        )
        for st in service_types:
            if df[df["service_type"] == st].empty:
                print(f"[UNDECLARED-INFO] No rows for service_type '{st}'; skipping.")
                continue
            _run_undeclared_scope(df, f"tracked — {st}", st.lower(), out_dir,
                                  restrict_service_type=st)


# ---------------------------------------------------------------------------
# HSDir action-rate distribution and stripped-flag churn
#
# ---------------------------------------------------------------------------

def _action_overlap_key(df, id_col, dedupe_on_service_type):
    """
    Entity-key Series for the action-rate analysis: (id_col, service_type)
    tuples under the overlap rule when dedupe_on_service_type is set and
    service_type exists, else id_col alone. Generalizes the established
    (fingerprint, service_type) pattern to id_col == "hsdir_fingerprint"
    (ring-event schema's relay identifier).
    """
    if dedupe_on_service_type and "service_type" in df.columns:
        return list(zip(df[id_col], df["service_type"].astype(str)))
    return df[id_col]


def _aggregate_action_rates(scope_df, id_col, time_col, dedupe_on_service_type):
    """
    Point-1 aggregate: per (entity, time_bucket) counts first (so the overlap
    dedup is correct -- an event is attributed to a specific entity before
    entities are deduplicated), then summed across all entities to one
    network-wide rate per (action, time_bucket).

    rate(action, t) = (events of that action at t, summed across entities)
                      / (all events at t, summed across entities).

    Because summing per-entity counts across entities is arithmetically the
    same as counting events with each event attributed once per distinct
    entity key, and every ring-event row maps to exactly one entity key, the
    per-entity-first framing here reduces to grouping rows by
    (entity_key, time, action). Returns {action: {time_bucket: rate_fraction}}.
    """
    work = scope_df.copy()
    work["_entity_key"] = _action_overlap_key(work, id_col, dedupe_on_service_type)

    # events per (time, action) -- each row is one event, attributed to its
    # entity key (the dedup unit); counts summed across entities == row counts
    # grouped by (time, action), since one row == one entity-attributed event.
    per_time_action = work.groupby([time_col, "action"]).size()
    per_time_total = work.groupby(time_col).size()

    actions = sorted(work["action"].astype(str).unique())
    rates = {a: {} for a in actions}
    for (t, action), cnt in per_time_action.items():
        total = per_time_total.get(t, 0)
        rates[str(action)][t] = (cnt / total) if total else 0.0
    return rates


def _onion_averaged_action_rates(scope_df, id_col, time_col, dedupe_on_service_type):
    """
    Point-2 onion-averaged view: compute each onion's OWN aggregate rate
    (same per-entity-then-aggregate method as _aggregate_action_rates, scoped
    to that onion's events), then average those per-onion rates UNWEIGHTED
    across onions -- one action/time at a time. An onion with many events
    does not get more influence than one with few (deliberately different
    from point 1's event-count-weighted network aggregate).

    Returns {action: {time_bucket: mean_of_per_onion_rates}}.
    """
    if "onion_address" not in scope_df.columns:
        return {}
    onions = [o for o in scope_df["onion_address"].astype(str).unique() if o not in ("None", "")]
    if not onions:
        return {}

    # The full action vocabulary across this scope. Every onion contributes a
    # rate for every action ONLY within the time buckets where it was actually
    # active (had >=1 event): there it gets its computed rate, or 0.0 for an
    # action it produced none of. An onion is NOT forced to contribute 0.0 to
    # a time bucket it had no events in at all -- it simply has no rate there,
    # so it is excluded from that bucket's average (which would otherwise be
    # dragged down by onions that were merely absent, not underperforming).
    all_actions = sorted(scope_df["action"].astype(str).unique())

    # accum[action][t] = list of per-onion rates (one per onion active at t).
    accum = {a: {} for a in all_actions}
    for onion in onions:
        onion_df = scope_df[scope_df["onion_address"].astype(str) == onion]
        if onion_df.empty:
            continue
        onion_rates = _aggregate_action_rates(onion_df, id_col, time_col, dedupe_on_service_type)
        active_buckets = set(onion_df[time_col].astype(str).unique())
        for t in active_buckets:
            for action in all_actions:
                r = onion_rates.get(action, {}).get(t, 0.0)
                accum[action].setdefault(t, []).append(r)

    averaged = {}
    for action, tmap in accum.items():
        averaged[action] = {t: (sum(vals) / len(vals) if vals else 0.0)
                            for t, vals in tmap.items()}
    return averaged


def _print_action_rates(label, rates_hour, rates_day, view_name):
    """Print each action's hour-rate and day-rate as percentages, averaged
    across the time buckets for a compact single-number-per-action summary."""
    actions = sorted(set(rates_hour) | set(rates_day))
    if not actions:
        print(f"    [{label}] {view_name}: no qualifying events.")
        return
    print(f"    [{label}] {view_name}:")
    for action in actions:
        hvals = list(rates_hour.get(action, {}).values())
        dvals = list(rates_day.get(action, {}).values())
        h = (sum(hvals) / len(hvals)) if hvals else 0.0
        d = (sum(dvals) / len(dvals)) if dvals else 0.0
        print(f"        {action}: hour-rate {h:.2%}, day-rate {d:.2%}")


def _run_action_rate_scope(scope_df, label, dedupe_on_service_type):
    """Run both views (fingerprint aggregate + onion-averaged) for one scope."""
    if scope_df is None or scope_df.empty:
        print(f"    [{label}] no qualifying events; skipping.")
        return
    # Fingerprint-based network aggregate.
    agg_hour = _aggregate_action_rates(scope_df, "hsdir_fingerprint", "hour", dedupe_on_service_type)
    agg_day = _aggregate_action_rates(scope_df, "hsdir_fingerprint", "date", dedupe_on_service_type)
    _print_action_rates(label, agg_hour, agg_day, "fingerprint-aggregate")

    # Onion-averaged view (never dedupes on service type -- an onion
    # belongs to one service type; the average is over onions, not entities).
    onion_hour = _onion_averaged_action_rates(scope_df, "hsdir_fingerprint", "hour", False)
    onion_day = _onion_averaged_action_rates(scope_df, "hsdir_fingerprint", "date", False)
    _print_action_rates(label, onion_hour, onion_day, "onion-averaged")


def hsdir_action_rate_distribution(df, context_label):
    """
    Distribution of ring-event `action` types as rates, over
    df_ring_events. context_label is "tracked" in practice (ring events exist
    only for tracked targets); the parameter is kept for interface parity but
    there is no "network" branch.

    Two views, both printed (terminal only, no plot):
      * fingerprint-aggregate (point 1): per (hsdir_fingerprint [, service_type
        under the overlap rule], time-bucket) counts summed across entities to
        one network-wide rate per (action, hour) and (action, day) --
        "what fraction of all events network-wide were of this action type."
      * onion-averaged (point 2): each onion's own aggregate rate, then an
        UNWEIGHTED average across onions -- "on average, what a typical onion's
        rate looks like" (an onion with many events gets no extra weight).

    For "tracked", both views repeat per service_type: the fingerprint
    aggregate applies the (hsdir_fingerprint, service_type) overlap rule, and
    the onion-average is taken over only that service type's onions.
    """
    if df is None or df.empty:
        print(f"[ACTION-RATE-ERROR] Missing/empty ring-event data for '{context_label}'.")
        return
    required = {"hsdir_fingerprint", "action", "onion_address", "date", "hour"}
    if not required.issubset(df.columns):
        print(f"[ACTION-RATE-INFO] Required columns missing for {context_label}; skipping.")
        return

    print(f"\n[ACTION-RATE] HSDir action-rate distribution — {context_label}")

    # General case: combined overlap-rule aggregate + all-onion average.
    _run_action_rate_scope(df, context_label, dedupe_on_service_type=(context_label == "tracked"))

    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in df["service_type"].dropna().unique() if t not in ("None", "")
        )
        for st in service_types:
            sub = df[df["service_type"] == st]
            if sub.empty:
                print(f"[ACTION-RATE-INFO] No events for service_type '{st}'; skipping.")
                continue
            # Within one service type the overlap key collapses to fingerprint
            # alone; keep dedupe on for consistency (no-op within a type).
            _run_action_rate_scope(sub, f"tracked — {st}", dedupe_on_service_type=True)


# ---------------------------------------------------------------------------
# Stripped-flag churn
# ---------------------------------------------------------------------------

# The six non-HSDir flags whose presence at the moment of stripping is
# characterized (HSDir itself is by definition absent in a STRIPPED_HSDIR_FLAG
# row, so it is not among them).
_STRIPPED_CHURN_FLAGS = ("Fast", "Guard", "Running", "Stable", "V2Dir", "Valid")


def _stripped_overlap_key(df, dedupe_on_service_type):
    """(fingerprint, service_type) under the overlap rule, else fingerprint."""
    if dedupe_on_service_type and "service_type" in df.columns:
        return list(zip(df["fingerprint"], df["service_type"].astype(str)))
    return df["fingerprint"]


def _flag_presence_per_event(stripped_df):
    """
    Single-snapshot presence: for each STRIPPED_HSDIR_FLAG row, whether each
    of the six flags is present in THAT row's own flags list. Rows with a
    malformed/non-list flags value are skipped (not errored).

    Returns a DataFrame with one bool column per flag (index-aligned to the
    kept rows), plus a "_kept_index" list of the original indices kept.
    """
    records = []
    kept_index = []
    for idx, flags in zip(stripped_df.index, stripped_df["flags"]):
        if not isinstance(flags, list):
            continue  # malformed -> skip this row for flag-presence
        flag_set = set(flags)
        records.append({flag: (flag in flag_set) for flag in _STRIPPED_CHURN_FLAGS})
        kept_index.append(idx)
    presence = pd.DataFrame(records, index=kept_index)
    return presence, kept_index


def _run_stripped_flag_scope(stripped_df, label):
    """Compute + print month-wide and per-day flag-presence % (terminal only;
    no plot -- the aggregate percentages are fully conveyed by the printout,
    so a bar chart would only restate them)."""
    presence, kept = _flag_presence_per_event(stripped_df)
    n = len(presence)
    if n == 0:
        print(f"    [{label}] no stripped-events with parseable flags; skipping.")
        return

    # Month-wide presence % per flag.
    pct_by_flag = {f: float(presence[f].mean()) for f in _STRIPPED_CHURN_FLAGS}
    print(f"    [{label}] {n} stripped-events — month-wide flag presence at strip time:")
    for f in _STRIPPED_CHURN_FLAGS:
        print(f"        {f}: {pct_by_flag[f]:.2%}")

    # Per-day presence % (cheap add-on).
    kept_df = stripped_df.loc[kept]
    presence_with_day = presence.copy()
    presence_with_day["date"] = kept_df["date"].values
    days = _chronological_days(kept_df)
    # if days:
    #     print(f"    [{label}] per-day flag presence at strip time:")
    #     for day in days:
    #         day_mask = presence_with_day["date"] == day
    #         d_n = int(day_mask.sum())
    #         if d_n == 0:
    #             continue
    #         parts = ", ".join(
    #             f"{f}: {presence_with_day.loc[day_mask, f].mean():.0%}"
    #             for f in _STRIPPED_CHURN_FLAGS)
    #         print(f"        {day} (n={d_n}): {parts}")


def hsdir_stripped_flag_churn_distribution(df, context_label):
    """
    Characterizes what other flags a node still carries at the moment its
    HSDir flag is stripped -- distinct from the stripped-churn series (which
    counts how often stripping happens); this asks what else is going on
    flag-wise when it does.

    For each STRIPPED_HSDIR_FLAG row, checks whether each of the six non-HSDir
    flags (Fast, Guard, Running, Stable, V2Dir, Valid) is still present in
    that row and reports the percentage of stripped events where each is. Read
    from the single strip-moment snapshot, which distinguishes an
    HSDir-specific loss (other flags present) from a broader simultaneous
    flag-loss, without depending on a prior observation that may not exist.

    Filter: status == "STRIPPED_HSDIR_FLAG". Prints month-wide and per-day
    presence % (no plot). For "tracked", repeats per service_type (overlap
    rule on (fingerprint, service_type)).
    """
    if df is None or df.empty:
        print(f"[FLAG-CHURN-ERROR] Missing/empty data for '{context_label}'.")
        return
    required = {"status", "flags", "fingerprint", "date"}
    if not required.issubset(df.columns):
        print(f"[FLAG-CHURN-INFO] Required columns missing for {context_label}; skipping.")
        return

    stripped = df[df["status"].astype(str) == "STRIPPED_HSDIR_FLAG"]

    print(f"\n[FLAG-CHURN] Stripped-flag churn — {context_label}")
    if stripped.empty:
        print(f"    [{context_label}] no STRIPPED_HSDIR_FLAG rows; skipping.")
        return

    _run_stripped_flag_scope(stripped, context_label)

    if context_label == "tracked" and "service_type" in df.columns:
        service_types = sorted(
            t for t in stripped["service_type"].dropna().unique() if t not in ("None", "")
        )
        for st in service_types:
            sub = stripped[stripped["service_type"] == st]
            if sub.empty:
                print(f"[FLAG-CHURN-INFO] No stripped-events for service_type '{st}'; skipping.")
                continue
            _run_stripped_flag_scope(sub, f"tracked — {st}")


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

    # Sybil-distribution analysis only needs the network-wide and tracked
    # consensus DataFrames (ring events / rotated snapshots aren't inputs
    # to either function below, and may not be present for every run).
    if df_net_consensus is not None and df_track_consensus is not None:
        family_id_sybil_distributions(df_net_consensus, "network")
        family_id_sybil_distributions(df_track_consensus, "tracked")
        ip_sybil_distributions(df_net_consensus, "network")
        ip_sybil_distributions(df_track_consensus, "tracked")
        service_type_overlap_diagnostic(df_track_consensus, "tracked")
        service_type_overlap_diagnostic(df_rot_track, "rotated_tracked")
        family_id_entity_drops_distributions(df_net_consensus, "network")
        family_id_entity_drops_distributions(df_track_consensus, "tracked")
        ip_entity_drops_distributions(df_net_consensus, "network")
        ip_entity_drops_distributions(df_track_consensus, "tracked")
        # hrt_uniformity_distributions does its own ghost-node filtering
        # internally, so it takes the unfiltered consensus DataFrames.
        hrt_uniformity_distributions(df_net_consensus, "network")
        hrt_uniformity_distributions(df_track_consensus, "tracked")
        # hrt_index_coverage_distributions likewise does its own ghost-node
        # filtering internally; it covers only the two hex ring-index metrics.
        hrt_index_coverage_distributions(df_net_consensus, "network")
        hrt_index_coverage_distributions(df_track_consensus, "tracked")
        # hsdir_churn_distributions applies its own stricter ghost filter
        # (consecutive_hourly_absences == 0) internally, so it also takes
        # the unfiltered consensus DataFrames.
        hsdir_churn_distributions(df_net_consensus, "network")
        hsdir_churn_distributions(df_track_consensus, "tracked")
        # Undeclared-family mutual-graph analysis; applies its own stricter
        # == 0 ghost filter internally, so it takes the raw DataFrames.
        undeclared_families_distribution(df_net_consensus, "network")
        undeclared_families_distribution(df_track_consensus, "tracked")
        # Action-rate distribution on ring events (tracked-only).
        if df_ring_events is not None:
            hsdir_action_rate_distribution(df_ring_events, "tracked")
        else:
            print("[ANALYSIS-WARNING] ring-event DataFrame unavailable; "
                  "skipping action-rate distribution.")
        # Stripped-flag churn on the consensus DataFrames.
        hsdir_stripped_flag_churn_distribution(df_net_consensus, "network")
        hsdir_stripped_flag_churn_distribution(df_track_consensus, "tracked")
        # Uptime-matrix and hourly-uptime-duration analyses; both apply the
        # standard ghost filter internally, so they take the raw DataFrames.
        hsdir_uptime_distributions(df_net_consensus, "network")
        hsdir_uptime_distributions(df_track_consensus, "tracked")
        hsdir_hourly_uptime_distributions(df_net_consensus, "network")
        hsdir_hourly_uptime_distributions(df_track_consensus, "tracked")
        # sybil_relay_*_changes apply their own stricter ghost filter
        # (consecutive_hourly_absences == 0) internally, so they also take
        # the unfiltered consensus DataFrames.
        sybil_relay_fingerprint_changes(df_net_consensus, "network")
        sybil_relay_fingerprint_changes(df_track_consensus, "tracked")
        sybil_relay_family_id_changes(df_net_consensus, "network")
        sybil_relay_family_id_changes(df_track_consensus, "tracked")
        sybil_relay_ip_changes(df_net_consensus, "network")
        sybil_relay_ip_changes(df_track_consensus, "tracked")
    else:
        print("[ANALYSIS-WARNING] network and/or tracked consensus DataFrame "
              "unavailable; skipping sybil distribution analysis.")

    # Rotated-node stability analysis only needs the rotated DataFrames.
    if df_rot_net is not None and df_rot_track is not None:
        daily_rotated_nodes_stability_distributions(df_rot_net, "rotated_network")
        daily_rotated_nodes_stability_distributions(df_rot_track, "rotated_tracked")
    else:
        print("[ANALYSIS-WARNING] rotated network and/or tracked DataFrame "
              "unavailable; skipping rotated-node stability analysis.")

    # BPK upload/fetch persistence analyses only need the ring-event log.
    # context_label is fixed to "tracked" (ring events exist only for
    # tracked onions).
    if df_ring_events is not None:
        bpks_uf_persistance(df_ring_events, "tracked")
        bpks_hour_uf_persistance(df_ring_events, "tracked")
        bpks_not_upload_fetch_distributions(df_ring_events, "tracked")
    else:
        print("[ANALYSIS-WARNING] ring-event DataFrame unavailable; "
              "skipping bpk upload/fetch persistence analysis.")

    print("="*60)
    print("ANALYSIS COMPLETED SUCCESSFULLY. ALL MATRIX ARTIFACTS EXPORTED.")
    print("="*60)