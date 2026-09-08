#!/usr/bin/env python3
"""
trim_hsdir_snapshots.py

Removes trailing entries from tracked_hsdir_consensus_snapshots.jsonl and
network_hsdir_consensus_snapshots.jsonl whose "timestamp" field is later
than a cutoff (default: 2026-07-24T23:59).

Each line in these files is a large JSON object such as:
    {"timestamp": "2026-06-23T01:05", "total_active_hsdirs": 5030, "relays": [...]}
with the "relays" array making individual lines many megabytes long. Since
hsdir-fetcher.py appends one snapshot per line in chronological order, any
entries past the cutoff can only be at the END of the file.

This script exploits that: it scans backward from the end of the file,
reading only the first ~200 bytes of each line (enough to see the
"timestamp" field) rather than parsing the full multi-MB JSON object, and
truncates the file right after the last entry at-or-before the cutoff. This
avoids ever fully parsing or rewriting the large, untouched, "kept" portion
of the file.

Removed lines are NOT discarded -- they are saved to a sibling file
"<name>.removed_after_cutoff.jsonl" before the original is truncated, so no
data is lost.

Usage:
    python trim_hsdir_snapshots.py FILE [FILE ...] [--cutoff TIMESTAMP] [--dry-run]

Example:
    python trim_hsdir_snapshots.py \\
        tracked_hsdir_consensus_snapshots.jsonl \\
        network_hsdir_consensus_snapshots.jsonl
"""

import argparse
import os
import re
import sys

DEFAULT_CUTOFF = "2026-07-24T23:59"
TIMESTAMP_RE = re.compile(rb'"timestamp"\s*:\s*"([^"]+)"')
PREFIX_BYTES = 256          # more than enough to contain the timestamp field
INITIAL_CHUNK = 1024 * 1024  # 1 MB; doubles if a line has no newline within it


def rfind_prev_newline(f, before_pos):
    """Return the offset of the last b'\\n' at index < before_pos, or -1 if none exists."""
    chunk_size = INITIAL_CHUNK
    end = before_pos
    while end > 0:
        start = max(0, end - chunk_size)
        f.seek(start)
        chunk = f.read(end - start)
        idx = chunk.rfind(b"\n")
        if idx != -1:
            return start + idx
        end = start
        chunk_size *= 2  # line is bigger than the current window; widen and retry
    return -1


def iter_lines_backward(f, file_size):
    """Yield (line_start, line_end) byte offsets from the last line to the first.
    line_end excludes the trailing newline."""
    if file_size == 0:
        return
    f.seek(file_size - 1)
    cur_end = file_size - 1 if f.read(1) == b"\n" else file_size
    while cur_end > 0:
        nl_pos = rfind_prev_newline(f, cur_end)
        line_start = nl_pos + 1
        yield line_start, cur_end
        if nl_pos == -1:
            break
        cur_end = nl_pos


def extract_timestamp(f, line_start, line_end):
    f.seek(line_start)
    prefix = f.read(min(PREFIX_BYTES, line_end - line_start))
    m = TIMESTAMP_RE.search(prefix)
    if not m:
        raise ValueError(f'No "timestamp" field found near byte offset {line_start}')
    return m.group(1).decode("utf-8")


def trim_file(path, cutoff, dry_run=False):
    file_size = os.path.getsize(path)
    if file_size == 0:
        print(f"[{path}] empty file, nothing to do")
        return

    with open(path, "r+b") as f:
        cutoff_line_end = None  # byte offset to truncate at (end of last kept line, incl. newline)
        removed_start = None    # byte offset where the first removed line begins
        removed_count = 0
        first_removed_ts = None

        for line_start, line_end in iter_lines_backward(f, file_size):
            ts = extract_timestamp(f, line_start, line_end)
            if ts <= cutoff:
                # This line is kept; everything after it (already scanned) is removed.
                cutoff_line_end = line_end  # will add back the trailing newline below
                break
            else:
                removed_count += 1
                first_removed_ts = ts
                removed_start = line_start
        else:
            # Loop completed without break: every single entry is past the cutoff.
            cutoff_line_end = 0
            removed_start = 0

        if removed_count == 0:
            print(f"[{path}] no entries after {cutoff}; nothing removed")
            return

        # truncation point = right after the last kept line's newline,
        # or 0 if nothing is kept
        truncate_at = 0 if cutoff_line_end == 0 else cutoff_line_end + 1

        print(
            f"[{path}] {removed_count} entr{'y' if removed_count == 1 else 'ies'} "
            f"after {cutoff} (earliest removed: {first_removed_ts}); "
            f"{file_size - truncate_at} bytes"
        )

        if dry_run:
            print(f"[{path}] --dry-run: no changes written")
            return

        # Save the removed tail before truncating, so nothing is lost.
        backup_path = f"{path}.removed_after_cutoff.jsonl"
        f.seek(truncate_at)
        with open(backup_path, "wb") as backup:
            while True:
                chunk = f.read(INITIAL_CHUNK)
                if not chunk:
                    break
                backup.write(chunk)
        print(f"[{path}] removed entries saved to {backup_path}")

        f.truncate(truncate_at)
        print(f"[{path}] truncated in place")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("files", nargs="+", help="JSONL snapshot files to trim")
    parser.add_argument(
        "--cutoff",
        default=DEFAULT_CUTOFF,
        help=f'entries with "timestamp" strictly greater than this are removed (default: {DEFAULT_CUTOFF})',
    )
    parser.add_argument("--dry-run", action="store_true", help="report what would be removed without modifying files")
    args = parser.parse_args()

    for path in args.files:
        if not os.path.isfile(path):
            print(f"[{path}] not found, skipping", file=sys.stderr)
            continue
        trim_file(path, args.cutoff, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
