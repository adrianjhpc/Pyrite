import struct
import json
import sys
import os
import gzip
import zlib
import math
from collections import defaultdict, Counter

# -----------------------------------------------------------------------------
# MPI Message Type Mapping
# -----------------------------------------------------------------------------

MESSAGE_TYPES = {
    13: "MPI_SEND",
    14: "MPI_RECV",
    15: "MPI_BSEND",
    16: "MPI_SSEND",
    17: "MPI_RSEND",
    18: "MPI_ISEND",
    19: "MPI_IBSEND",
    20: "MPI_ISSEND",
    21: "MPI_IRSEND",
    22: "MPI_IRECV",
    23: "MPI_SENDRECV",
    24: "MPI_WAIT",
    25: "MPI_WAITALL",
    26: "MPI_BARRIER",
    27: "MPI_BCAST",
    28: "MPI_REDUCE",
    29: "MPI_ALLREDUCE",
    30: "MPI_GATHER",
    31: "MPI_SCATTER",
    32: "MPI_ALLGATHER",
    33: "MPI_WAITANY",
    34: "MPI_WAITSOME",
    35: "MPI_TEST",
    36: "MPI_TESTANY",
    37: "MPI_TESTALL",
    38: "MPI_TESTSOME",
}

MESSAGE_TYPE_ORDER = {name: code for code, name in MESSAGE_TYPES.items()}

SMALL_LABEL_TEXT = "P2P Small Type Messages"
LARGE_LABEL_TEXT = "P2P Large Type Messages"
SMALL_LABEL_BYTES = SMALL_LABEL_TEXT.encode("utf-8")
LARGE_LABEL_BYTES = LARGE_LABEL_TEXT.encode("utf-8")

BINS = [
    "< 128B",
    "128B - 1KB",
    "1KB - 64KB",
    "64KB - 1MB",
    "1MB - 16MB",
    "> 16MB",
]

SEND_LIKE_CALLS = {
    "MPI_SEND", "MPI_BSEND", "MPI_SSEND", "MPI_RSEND",
    "MPI_ISEND", "MPI_IBSEND", "MPI_ISSEND", "MPI_IRSEND",
}

RECV_LIKE_CALLS = {
    "MPI_RECV", "MPI_IRECV",
}

COMPLETION_CALLS = {
    "MPI_WAIT", "MPI_WAITALL", "MPI_WAITANY", "MPI_WAITSOME",
    "MPI_TEST", "MPI_TESTANY", "MPI_TESTALL", "MPI_TESTSOME",
}

SYNC_CALLS = {
    "MPI_BARRIER",
}

ROOTED_COLLECTIVES = {
    "MPI_BCAST", "MPI_REDUCE", "MPI_GATHER", "MPI_SCATTER",
}

GLOBAL_COLLECTIVES = {
    "MPI_ALLREDUCE", "MPI_ALLGATHER",
}


# -----------------------------------------------------------------------------
# Basic helpers
# -----------------------------------------------------------------------------

def _make_bins_template():
    return {
        "< 128B": 0,
        "128B - 1KB": 0,
        "1KB - 64KB": 0,
        "64KB - 1MB": 0,
        "1MB - 16MB": 0,
        "> 16MB": 0,
    }


def _cstr(raw_bytes):
    return raw_bytes.split(b"\x00", 1)[0].decode("utf-8", errors="ignore")


def _open_maybe_gzip(filepath):
    with open(filepath, "rb") as probe:
        magic = probe.read(2)

    if filepath.endswith(".gz") or magic == b"\x1f\x8b":
        return gzip.open(filepath, "rb")
    return open(filepath, "rb")


def _read_exact(buffer, offset, size, context):
    end = offset + size
    if end > len(buffer):
        raise ValueError(
            "Unexpected end of file while reading {}: need {} bytes at offset {}, only {} remain".format(
                context, size, offset, len(buffer) - offset
            )
        )
    return buffer[offset:end], end


def _safe_div(num, den):
    if den == 0:
        return 0.0
    return float(num) / float(den)


def _mean(values):
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


def _pstdev(values):
    if not values:
        return 0.0
    m = _mean(values)
    return math.sqrt(sum((x - m) * (x - m) for x in values) / float(len(values)))


def _cv(values):
    m = _mean(values)
    if m == 0.0:
        return 0.0
    return _pstdev(values) / m


def _categorise_small_call(call_name):
    if call_name in COMPLETION_CALLS:
        return "completion"
    if call_name in SYNC_CALLS:
        return "synchronisation"
    if call_name in ("MPI_BCAST", "MPI_REDUCE", "MPI_ALLREDUCE"):
        return "collective"
    return "point-to-point"


def _categorise_large_part(call_name, part_index):
    if call_name == "MPI_SENDRECV":
        return "sendrecv_part_{}".format(part_index)
    return "collective_part_{}".format(part_index)


def _bin_for_bytes(num_bytes):
    if num_bytes < 0:
        return None
    if num_bytes < 128:
        return "< 128B"
    if num_bytes < 1024:
        return "128B - 1KB"
    if num_bytes < 65536:
        return "1KB - 64KB"
    if num_bytes < 1048576:
        return "64KB - 1MB"
    if num_bytes < 16777216:
        return "1MB - 16MB"
    return "> 16MB"


def _ensure_stats_entry(stats, call_name):
    if call_name not in stats:
        stats[call_name] = _make_bins_template()


def _update_stats(stats, call_name, bytes_vol):
    _ensure_stats_entry(stats, call_name)
    bucket = _bin_for_bytes(bytes_vol)
    if bucket is not None:
        stats[call_name][bucket] += 1


def _severity(score):
    if score >= 0.85:
        return "critical"
    if score >= 0.6:
        return "warning"
    return "info"


def _top_dict_items(dct, n, reverse=True):
    return sorted(dct.items(), key=lambda kv: kv[1], reverse=reverse)[:n]


def print_progress(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='#'):
    """
    Call in a loop to create terminal progress bar.
    """
    if total == 0:
        return
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    sys.stdout.write('\r{} |{}| {}% {}'.format(prefix, bar, percent, suffix))
    sys.stdout.flush()
    # Print New Line on Complete
    if iteration == total:
        print()

# -----------------------------------------------------------------------------
# Timeline recording helpers
# -----------------------------------------------------------------------------

def _record_small_event(data, rank_id, time_val, msg_id, mtype, sender, receiver, count, bytes_vol):
    call_name = MESSAGE_TYPES.get(mtype, "UNKNOWN_{}".format(mtype))

    data["timeline"].append({
        "time": time_val,
        "event_id": msg_id,
        "rank_recording": rank_id,
        "call": call_name,
        "sender": sender,
        "receiver": receiver,
        "count": count,
        "bytes": bytes_vol,
        "category": _categorise_small_call(call_name),
    })

    _update_stats(data["statistics"], call_name, bytes_vol)


def _record_large_event(data, rank_id, time_val, msg_id, mtype, s1, r1, c1, b1, s2, r2, c2, b2):
    call_name = MESSAGE_TYPES.get(mtype, "UNKNOWN_{}".format(mtype))
    _ensure_stats_entry(data["statistics"], call_name)

    parts = [
        (1, s1, r1, c1, b1),
        (2, s2, r2, c2, b2),
    ]

    for part_index, sender, receiver, count, bytes_vol in parts:
        is_empty_placeholder = (count <= 0 and bytes_vol <= 0)

        data["timeline"].append({
            "time": time_val,
            "event_id": msg_id,
            "rank_recording": rank_id,
            "call": call_name,
            "sender": sender,
            "receiver": receiver,
            "count": count,
            "bytes": bytes_vol,
            "category": _categorise_large_part(call_name, part_index),
            "synthetic_empty": bool(is_empty_placeholder),
        })

        # Keep placeholder parts in the timeline but exclude them from summary stats.
        if not is_empty_placeholder:
            _update_stats(data["statistics"], call_name, bytes_vol)


# -----------------------------------------------------------------------------
# Hardware map
# -----------------------------------------------------------------------------

def load_hardware_map(filepath):
    if not filepath or not os.path.exists(filepath):
        return {}

    with open(filepath, "r") as f:
        hw = json.load(f)

    lookup = {}

    for cab in hw.get("cabinets", []):
        cab_id = cab.get("id")
        cab_x = cab.get("x", 0)
        cab_z = cab.get("z", 0)

        for rack in cab.get("racks", []):
            rack_id = rack.get("id")
            rack_x_off = rack.get("x_offset", 0)
            rack_z_off = rack.get("z_offset", 0)

            if "blades" in rack:
                for blade in rack.get("blades", []):
                    blade_id = blade.get("id")
                    blade_y_off = blade.get("y_offset", 0)

                    for node in blade.get("nodes", []):
                        hostname = node.get("hostname")
                        if not hostname:
                            continue

                        lookup[hostname] = {
                            "cab_id": cab_id,
                            "rack_id": rack_id,
                            "blade_id": blade_id,
                            "x": cab_x + rack_x_off + node.get("x_offset", node.get("slot", 0) * 12),
                            "y": blade_y_off,
                            "z": cab_z + rack_z_off,
                        }
            else:
                for node in rack.get("nodes", []):
                    hostname = node.get("hostname")
                    if not hostname:
                        continue

                    lookup[hostname] = {
                        "cab_id": cab_id,
                        "rack_id": rack_id,
                        "x": cab_x + rack_x_off,
                        "y": node.get("slot", 0) * 15,
                        "z": cab_z + rack_z_off,
                    }

    return lookup


# -----------------------------------------------------------------------------
# Human-readable summaries
# -----------------------------------------------------------------------------

def print_summary_table(stats):
    print("\n" + "=" * 95)
    print(" MPI COMMUNICATION SUMMARY")
    print("=" * 95)

    if not stats:
        print(" No communication events found.")
        print("=" * 95 + "\n")
        return

    ordered_calls = sorted(
        stats.keys(),
        key=lambda name: MESSAGE_TYPE_ORDER.get(name, 9999)
    )

    header = " {:<13} | ".format("MPI Call") + " | ".join("{:<10}".format(b) for b in BINS) + " | {:<8}".format("Total")
    print(header)
    print("-" * len(header))

    for call in ordered_calls:
        bin_data = stats[call]
        short_call = call.replace("MPI_", "")
        row = " {:<13} | ".format(short_call)

        total = 0
        for bucket in BINS:
            count = bin_data.get(bucket, 0)
            total += count
            row += "{:<10} | ".format(count)

        row += "{:<8}".format(total)
        print(row)

    print("=" * 95 + "\n")


def print_analysis_summary(analysis):
    print("\n" + "=" * 95)
    print(" TRACE ANALYSIS SUMMARY")
    print("=" * 95)

    summary = analysis.get("summary", {})
    print(" Total events:             {}".format(summary.get("total_events", 0)))
    print(" Canonical transfers:      {}".format(summary.get("canonical_transfer_events", 0)))
    print(" Canonical transfer bytes: {}".format(summary.get("canonical_transfer_bytes", 0)))
    print(" Completion events:        {}".format(summary.get("completion_events", 0)))
    print(" Barrier events:           {}".format(summary.get("barrier_events", 0)))
    print(" Estimated runtime:        {:.6f}s".format(summary.get("estimated_runtime", 0.0)))

    patterns = analysis.get("patterns", [])
    issues = analysis.get("issues", [])

    if patterns:
        print("\n Detected Patterns:")
        for pat in patterns:
            print("  - [{}] {}".format(pat.get("type", "pattern"), pat.get("description", "")))

    if issues:
        print("\n Potential Performance Issues:")
        for issue in issues:
            print("  - [{}] {}".format(issue.get("severity", "info").upper(), issue.get("description", "")))

    print("=" * 95 + "\n")


# -----------------------------------------------------------------------------
# Parsing backends
# -----------------------------------------------------------------------------

def _parse_sections_strict(raw_data, total_ranks, data, small_fmt, large_fmt):
    offset = 0
    small_size = struct.calcsize(small_fmt)
    large_size = struct.calcsize(large_fmt)

    for section_index in range(total_ranks):
        chunk, offset = _read_exact(raw_data, offset, 4, "rank id")
        rank_id = struct.unpack("=i", chunk)[0]

        chunk, offset = _read_exact(raw_data, offset, 24, "small section label")
        small_label = _cstr(chunk)
        if SMALL_LABEL_TEXT not in small_label:
            raise ValueError(
                "Bad small-section label for rank {} at section {}: {!r}".format(
                    rank_id, section_index, small_label
                )
            )

        chunk, offset = _read_exact(raw_data, offset, 4, "small section count")
        num_small = struct.unpack("=i", chunk)[0]
        if num_small < 0:
            raise ValueError("Negative small record count {} for rank {}".format(num_small, rank_id))

        for _ in range(num_small):
            chunk, offset = _read_exact(raw_data, offset, small_size, "small record")
            time_val, msg_id, mtype, sender, receiver, count, bytes_vol = struct.unpack(small_fmt, chunk)
            _record_small_event(data, rank_id, time_val, msg_id, mtype, sender, receiver, count, bytes_vol)

        chunk, offset = _read_exact(raw_data, offset, 24, "large section label")
        large_label = _cstr(chunk)
        if LARGE_LABEL_TEXT not in large_label:
            raise ValueError(
                "Bad large-section label for rank {} at section {}: {!r}".format(
                    rank_id, section_index, large_label
                )
            )

        chunk, offset = _read_exact(raw_data, offset, 4, "large section count")
        num_large = struct.unpack("=i", chunk)[0]
        if num_large < 0:
            raise ValueError("Negative large record count {} for rank {}".format(num_large, rank_id))

        for _ in range(num_large):
            chunk, offset = _read_exact(raw_data, offset, large_size, "large record")
            unpacked = struct.unpack(large_fmt, chunk)
            _record_large_event(data, rank_id, *unpacked)

        print_progress(section_index + 1, total_ranks, prefix='Parsing Ranks:', suffix='Complete', length=40)

    trailing = raw_data[offset:]
    if trailing not in (b"",):
        raise ValueError("Strict parse finished with {} unexpected trailing bytes".format(len(trailing)))


def _parse_sections_salvage(raw_data, total_len, data, small_fmt, large_fmt):
    offset = 0
    small_size = struct.calcsize(small_fmt)
    large_size = struct.calcsize(large_fmt)

    while offset < total_len:
        if offset + 4 > total_len:
            break

        rank_id = struct.unpack("=i", raw_data[offset:offset + 4])[0]
        offset += 4

        if offset + 24 > total_len:
            break
        small_str = raw_data[offset:offset + 24]
        offset += 24

        if SMALL_LABEL_BYTES not in small_str:
            next_idx = raw_data.find(SMALL_LABEL_BYTES, offset)
            if next_idx == -1:
                break
            offset = max(0, next_idx - 4)
            continue

        if offset + 4 > total_len:
            break
        offset += 4

        next_large_idx = raw_data.find(LARGE_LABEL_BYTES, offset)
        if next_large_idx == -1:
            small_bytes_len = total_len - offset
            next_large_idx = total_len
        else:
            small_bytes_len = next_large_idx - offset

        valid_small_len = small_bytes_len - (small_bytes_len % small_size)
        small_buffer = raw_data[offset:offset + valid_small_len]

        for unpacked in struct.iter_unpack(small_fmt, small_buffer):
            _record_small_event(data, rank_id, *unpacked)

        offset = next_large_idx

        if offset + 24 > total_len:
            break
        offset += 24

        if offset + 4 > total_len:
            break
        offset += 4

        data_start_large = offset
        next_small_idx = raw_data.find(SMALL_LABEL_BYTES, offset)

        if next_small_idx == -1:
            large_bytes_len = total_len - data_start_large
            next_rank_offset = total_len
        else:
            next_rank_offset = max(0, next_small_idx - 4)
            large_bytes_len = next_rank_offset - data_start_large

        valid_large_len = large_bytes_len - (large_bytes_len % large_size)
        large_buffer = raw_data[data_start_large:data_start_large + valid_large_len]

        for unpacked in struct.iter_unpack(large_fmt, large_buffer):
            _record_large_event(data, rank_id, *unpacked)

        offset = next_rank_offset


# -----------------------------------------------------------------------------
# Analysis helpers
# -----------------------------------------------------------------------------

def _is_real_payload_event(event):
    return (event.get("bytes", 0) > 0) and (not event.get("synthetic_empty", False))


def _is_canonical_transfer_event(event):
    """
    Select a canonical subset of events that is useful for pair/link analysis
    without double-counting the common send+recv case.

    Rules:
      - send-like point-to-point calls count
      - SENDRECV part_1 counts
      - BCAST / REDUCE count as directional rooted traffic
      - GATHER part_1 counts
      - SCATTER part_2 counts
      - ALLREDUCE / ALLGATHER are analysed separately, not as pair links
      - receive-side point-to-point records are ignored for pair aggregation
    """
    if not _is_real_payload_event(event):
        return False

    call = event["call"]
    category = event.get("category", "")

    if call in SEND_LIKE_CALLS:
        return True

    if call == "MPI_SENDRECV" and category == "sendrecv_part_1":
        return True

    if call == "MPI_BCAST" and event["sender"] != event["receiver"]:
        return True

    if call == "MPI_REDUCE" and event["sender"] != event["receiver"]:
        return True

    if call == "MPI_GATHER" and category == "collective_part_1" and event["sender"] != event["receiver"]:
        return True

    if call == "MPI_SCATTER" and category == "collective_part_2" and event["sender"] != event["receiver"]:
        return True

    return False


def _guess_root_for_rooted_collective(event):
    call = event["call"]
    if call in ("MPI_BCAST", "MPI_SCATTER"):
        return event["sender"]
    if call in ("MPI_REDUCE", "MPI_GATHER"):
        return event["receiver"]
    return None


def _build_time_windows(timeline, window_count):
    if not timeline:
        return []

    start_t = timeline[0]["time"]
    end_t = timeline[-1]["time"]
    runtime = end_t - start_t

    if runtime <= 0.0 or window_count <= 1:
        return [{
            "t_start": start_t,
            "t_end": end_t,
            "events": len(timeline),
            "canonical_transfer_events": sum(1 for e in timeline if _is_canonical_transfer_event(e)),
            "canonical_transfer_bytes": sum(e.get("bytes", 0) for e in timeline if _is_canonical_transfer_event(e)),
            "completion_events": sum(1 for e in timeline if e["call"] in COMPLETION_CALLS),
            "barrier_events": sum(1 for e in timeline if e["call"] == "MPI_BARRIER"),
            "collective_events": sum(1 for e in timeline if e["call"] in ROOTED_COLLECTIVES or e["call"] in GLOBAL_COLLECTIVES),
        }]

    windows = []
    for i in range(window_count):
        t0 = start_t + (runtime * i) / float(window_count)
        t1 = start_t + (runtime * (i + 1)) / float(window_count)
        windows.append({
            "t_start": t0,
            "t_end": t1,
            "events": 0,
            "canonical_transfer_events": 0,
            "canonical_transfer_bytes": 0,
            "completion_events": 0,
            "barrier_events": 0,
            "collective_events": 0,
        })

    for event in timeline:
        idx = int(((event["time"] - start_t) / runtime) * window_count)
        if idx >= window_count:
            idx = window_count - 1
        if idx < 0:
            idx = 0

        win = windows[idx]
        win["events"] += 1

        if _is_canonical_transfer_event(event):
            win["canonical_transfer_events"] += 1
            win["canonical_transfer_bytes"] += event.get("bytes", 0)

        if event["call"] in COMPLETION_CALLS:
            win["completion_events"] += 1

        if event["call"] == "MPI_BARRIER":
            win["barrier_events"] += 1

        if event["call"] in ROOTED_COLLECTIVES or event["call"] in GLOBAL_COLLECTIVES:
            win["collective_events"] += 1

    return windows


def analyse_trace(data):
    """
    Build analysis structures intended for visualisation and lightweight
    performance diagnosis.
    """
    timeline = data["timeline"]
    total_ranks = data["metadata"].get("total_ranks", 0)

    per_rank = {}
    for r in range(total_ranks):
        per_rank[r] = {
            "rank": r,
            "events": 0,
            "completion_events": 0,
            "barrier_events": 0,
            "collective_events": 0,
            "global_collective_events": 0,
            "rooted_collective_events": 0,
            "canonical_bytes_out": 0,
            "canonical_bytes_in": 0,
            "canonical_messages_out": 0,
            "canonical_messages_in": 0,
            "distinct_out_peers": set(),
            "distinct_in_peers": set(),
            "completion_request_count": 0,
        }

    pair_stats = defaultdict(lambda: {
        "messages": 0,
        "bytes": 0,
        "calls": Counter(),
        "first_time": None,
        "last_time": None,
    })

    rooted_collective_roots = defaultdict(lambda: {
        "events": 0,
        "bytes": 0,
        "calls": Counter(),
    })

    barrier_times = defaultdict(list)

    canonical_total_events = 0
    canonical_total_bytes = 0
    completion_total_events = 0
    barrier_total_events = 0
    collective_total_events = 0
    global_collective_total_events = 0

    small_by_call = Counter()
    small128_by_call = Counter()
    small1k_by_call = Counter()

    sorted_timeline = timeline
    if sorted_timeline:
        start_t = sorted_timeline[0]["time"]
        end_t = sorted_timeline[-1]["time"]
        runtime = max(0.0, end_t - start_t)
    else:
        start_t = 0.0
        end_t = 0.0
        runtime = 0.0

    total_events = len(sorted_timeline)

    # ---------------------------------------------------------
    # Pass 1: basic counts over all recorded events.
    # ---------------------------------------------------------
    print("\nAnalyzing Trace - Pass 1 (Basic Counts)...")
    print_progress(0, total_events, prefix='Pass 1: ', suffix='Complete', length=40, fill='#')
    
    for idx, event in enumerate(sorted_timeline):
        rr = event["rank_recording"]
        call = event["call"]
        count = event.get("count", 0)

        if rr in per_rank:
            per_rank[rr]["events"] += 1

            if call in COMPLETION_CALLS:
                per_rank[rr]["completion_events"] += 1
                per_rank[rr]["completion_request_count"] += max(0, count)

            if call == "MPI_BARRIER":
                per_rank[rr]["barrier_events"] += 1
                barrier_times[rr].append(event["time"])

            if call in ROOTED_COLLECTIVES or call in GLOBAL_COLLECTIVES:
                per_rank[rr]["collective_events"] += 1
                if call in ROOTED_COLLECTIVES:
                    per_rank[rr]["rooted_collective_events"] += 1
                if call in GLOBAL_COLLECTIVES:
                    per_rank[rr]["global_collective_events"] += 1

        if call in COMPLETION_CALLS:
            completion_total_events += 1

        if call == "MPI_BARRIER":
            barrier_total_events += 1

        if call in ROOTED_COLLECTIVES or call in GLOBAL_COLLECTIVES:
            collective_total_events += 1

        if call in GLOBAL_COLLECTIVES:
            global_collective_total_events += 1

        # Throttle progress bar updates to save I/O time
        if idx % 10000 == 0:
            print_progress(idx, total_events, prefix='Pass 1: ', suffix='Complete', length=40, fill='#')
            
    # Ensure it hits 100% at the end
    print_progress(total_events, total_events, prefix='Pass 1: ', suffix='Complete', length=40, fill='#')


    # ---------------------------------------------------------
    # Pass 2: canonical pair/link traffic.
    # ---------------------------------------------------------
    print("Analyzing Trace - Pass 2 (Canonical Traffic)...")
    print_progress(0, total_events, prefix='Pass 2: ', suffix='Complete', length=40, fill='#')
    
    for idx, event in enumerate(sorted_timeline):
        if not _is_canonical_transfer_event(event):
            continue

        call = event["call"]
        sender = event["sender"]
        receiver = event["receiver"]
        num_bytes = event.get("bytes", 0)
        t = event.get("time", 0.0)

        canonical_total_events += 1
        canonical_total_bytes += num_bytes

        if 0 <= sender < total_ranks:
            per_rank[sender]["canonical_bytes_out"] += num_bytes
            per_rank[sender]["canonical_messages_out"] += 1
            per_rank[sender]["distinct_out_peers"].add(receiver)

        if 0 <= receiver < total_ranks:
            per_rank[receiver]["canonical_bytes_in"] += num_bytes
            per_rank[receiver]["canonical_messages_in"] += 1
            per_rank[receiver]["distinct_in_peers"].add(sender)

        pair_key = (sender, receiver)
        pair_stats[pair_key]["messages"] += 1
        pair_stats[pair_key]["bytes"] += num_bytes
        pair_stats[pair_key]["calls"][call] += 1
        if pair_stats[pair_key]["first_time"] is None or t < pair_stats[pair_key]["first_time"]:
            pair_stats[pair_key]["first_time"] = t
        if pair_stats[pair_key]["last_time"] is None or t > pair_stats[pair_key]["last_time"]:
            pair_stats[pair_key]["last_time"] = t

        small_by_call[call] += 1
        if num_bytes < 128:
            small128_by_call[call] += 1
        if num_bytes < 1024:
            small1k_by_call[call] += 1

        if call in ROOTED_COLLECTIVES:
            root = _guess_root_for_rooted_collective(event)
            if root is not None:
                rooted_collective_roots[root]["events"] += 1
                rooted_collective_roots[root]["bytes"] += num_bytes
                rooted_collective_roots[root]["calls"][call] += 1

        # Throttle progress bar updates to save I/O time
        if idx % 10000 == 0:
            print_progress(idx, total_events, prefix='Pass 2: ', suffix='Complete', length=40, fill='#')
            
    # Ensure it hits 100% at the end
    print_progress(total_events, total_events, prefix='Pass 2: ', suffix='Complete', length=40, fill='#')


    print("Finalising rank sets...")
    # Finalise per-rank sets into counts / serialisable data.
    per_rank_list = []
    total_touch_bytes = []
    out_peer_counts = []
    in_peer_counts = []

    for rank in range(total_ranks):
        entry = per_rank[rank]
        entry["distinct_out_peers"] = len(entry["distinct_out_peers"])
        entry["distinct_in_peers"] = len(entry["distinct_in_peers"])
        entry["touch_bytes"] = entry["canonical_bytes_out"] + entry["canonical_bytes_in"]
        per_rank_list.append(entry)
        total_touch_bytes.append(entry["touch_bytes"])
        out_peer_counts.append(entry["distinct_out_peers"])
        in_peer_counts.append(entry["distinct_in_peers"])

    print("Calculatng top links...")

    # Top links.
    top_links = []
    for (sender, receiver), stats in sorted(pair_stats.items(), key=lambda kv: kv[1]["bytes"], reverse=True)[:20]:
        top_links.append({
            "sender": sender,
            "receiver": receiver,
            "messages": stats["messages"],
            "bytes": stats["bytes"],
            "calls": dict(sorted(stats["calls"].items())),
            "first_time": stats["first_time"],
            "last_time": stats["last_time"],
        })

    print("Calculating top ranks...")

    # Top ranks.
    top_ranks_by_out = [
        {"rank": r["rank"], "bytes": r["canonical_bytes_out"], "messages": r["canonical_messages_out"]}
        for r in sorted(per_rank_list, key=lambda x: x["canonical_bytes_out"], reverse=True)[:10]
    ]
    top_ranks_by_in = [
        {"rank": r["rank"], "bytes": r["canonical_bytes_in"], "messages": r["canonical_messages_in"]}
        for r in sorted(per_rank_list, key=lambda x: x["canonical_bytes_in"], reverse=True)[:10]
    ]
    top_ranks_by_touch = [
        {"rank": r["rank"], "bytes": r["touch_bytes"]}
        for r in sorted(per_rank_list, key=lambda x: x["touch_bytes"], reverse=True)[:10]
    ]

    print("Analysing collectives...")

    # Rooted collective roots.
    rooted_collective_summary = []
    for root, stats in sorted(rooted_collective_roots.items(), key=lambda kv: kv[1]["bytes"], reverse=True):
        rooted_collective_summary.append({
            "root": root,
            "events": stats["events"],
            "bytes": stats["bytes"],
            "calls": dict(sorted(stats["calls"].items())),
        })

    print("Analysing barriers...")
    # Barrier skew analysis.
    barrier_spreads = []
    if total_ranks > 0:
        barrier_counts = [len(barrier_times[r]) for r in range(total_ranks)]
        common_barriers = min(barrier_counts) if barrier_counts else 0

        for idx in range(common_barriers):
            times = [barrier_times[r][idx] for r in range(total_ranks)]
            spread = max(times) - min(times)
            barrier_spreads.append({
                "barrier_index": idx,
                "t_min": min(times),
                "t_max": max(times),
                "spread": spread,
            })
    else:
        common_barriers = 0

    max_barrier_spread = max((b["spread"] for b in barrier_spreads), default=0.0)
    avg_barrier_spread = _mean([b["spread"] for b in barrier_spreads]) if barrier_spreads else 0.0

    print("Looking for patterns")

    # Patterns.
    patterns = []

    avg_out_peers = _mean(out_peer_counts)
    avg_in_peers = _mean(in_peer_counts)
    pair_density = _safe_div(len(pair_stats), total_ranks * max(0, total_ranks - 1))

    # Pattern: star/master-worker
    if per_rank_list and canonical_total_bytes > 0:
        top_touch_rank = max(per_rank_list, key=lambda x: x["touch_bytes"])
        top_touch_fraction = _safe_div(top_touch_rank["touch_bytes"], 2 * canonical_total_bytes)
        top_degree = max(top_touch_rank["distinct_out_peers"], top_touch_rank["distinct_in_peers"])

        if total_ranks > 2 and top_touch_fraction >= 0.35 and top_degree >= max(2, int(0.5 * (total_ranks - 1))):
            patterns.append({
                "type": "master_worker",
                "strength": top_touch_fraction,
                "ranks": [top_touch_rank["rank"]],
                "description": "Rank {} is communication-central, touching {:.1%} of observed canonical traffic.".format(
                    top_touch_rank["rank"], top_touch_fraction
                ),
                "metrics": {
                    "touch_fraction": top_touch_fraction,
                    "degree": top_degree,
                },
            })

    # Pattern: rooted collectives concentrated on one root.
    if rooted_collective_summary:
        total_rooted_bytes = sum(x["bytes"] for x in rooted_collective_summary)
        top_root = rooted_collective_summary[0]
        top_root_frac = _safe_div(top_root["bytes"], total_rooted_bytes)

        if top_root_frac >= 0.5 and top_root["events"] > 0:
            patterns.append({
                "type": "rooted_collectives",
                "strength": top_root_frac,
                "ranks": [top_root["root"]],
                "description": "Rooted collective traffic is concentrated on rank {}, which handles {:.1%} of rooted collective bytes.".format(
                    top_root["root"], top_root_frac
                ),
                "metrics": {
                    "root": top_root["root"],
                    "rooted_collective_fraction": top_root_frac,
                    "rooted_collective_bytes": top_root["bytes"],
                    "rooted_collective_events": top_root["events"],
                },
            })

    # Pattern: ring / nearest-neighbour
    if total_ranks > 2 and pair_stats:
        offset_counter = Counter()
        reciprocal_pair_count = 0
        seen_pairs = set()

        for (s, r), stats in pair_stats.items():
            if s == r:
                continue
            offset = (r - s) % total_ranks
            canonical_offset = min(offset, total_ranks - offset) if total_ranks > 0 else offset
            offset_counter[canonical_offset] += stats["messages"]

            if (r, s) in pair_stats and ((r, s), (s, r)) not in seen_pairs:
                reciprocal_pair_count += 1
                seen_pairs.add(((s, r), (r, s)))
                seen_pairs.add(((r, s), (s, r)))

        total_offset_msgs = sum(offset_counter.values())
        one_hop_frac = _safe_div(offset_counter.get(1, 0), total_offset_msgs)
        top_offsets = offset_counter.most_common(4)
        top_offsets_frac = _safe_div(sum(v for _, v in top_offsets), total_offset_msgs)
        reciprocal_pair_frac = _safe_div(reciprocal_pair_count, max(1, len(pair_stats)))

        if avg_out_peers <= 2.5 and one_hop_frac >= 0.5:
            patterns.append({
                "type": "ring_nearest_neighbor",
                "strength": one_hop_frac,
                "description": "Communication is dominated by nearest-neighbour rank offsets (1-hop fraction {:.1%}).".format(
                    one_hop_frac
                ),
                "metrics": {
                    "one_hop_fraction": one_hop_frac,
                    "avg_out_peers": avg_out_peers,
                    "reciprocal_pair_fraction": reciprocal_pair_frac,
                },
            })
        elif avg_out_peers <= 6.0 and top_offsets_frac >= 0.75 and reciprocal_pair_frac >= 0.2:
            patterns.append({
                "type": "neighborhood_exchange",
                "strength": top_offsets_frac,
                "description": "Communication is concentrated on a small set of rank offsets, consistent with a neighbourhood/halo exchange.".format(),
                "metrics": {
                    "top_offset_fraction": top_offsets_frac,
                    "avg_out_peers": avg_out_peers,
                    "reciprocal_pair_fraction": reciprocal_pair_frac,
                },
            })

    # Pattern: all-to-all-ish
    if total_ranks > 2 and avg_out_peers >= 0.6 * (total_ranks - 1) and pair_density >= 0.5:
        patterns.append({
            "type": "all_to_all_like",
            "strength": min(1.0, max(pair_density, _safe_div(avg_out_peers, total_ranks - 1))),
            "description": "The pair graph is dense, suggesting an all-to-all-like communication pattern.",
            "metrics": {
                "pair_density": pair_density,
                "avg_out_peers": avg_out_peers,
                "avg_in_peers": avg_in_peers,
            },
        })

    # Pattern: ping-pong pairs
    ping_pong_candidates = []
    for (s, r), stats_sr in pair_stats.items():
        if s >= r:
            continue
        stats_rs = pair_stats.get((r, s))
        if not stats_rs:
            continue

        total_msgs = stats_sr["messages"] + stats_rs["messages"]
        if total_msgs < 6:
            continue

        balance = 1.0 - _safe_div(abs(stats_sr["bytes"] - stats_rs["bytes"]), max(stats_sr["bytes"], stats_rs["bytes"], 1))
        if balance >= 0.75:
            ping_pong_candidates.append({
                "ranks": [s, r],
                "messages": total_msgs,
                "bytes_total": stats_sr["bytes"] + stats_rs["bytes"],
                "balance": balance,
            })

    ping_pong_candidates.sort(key=lambda x: (x["bytes_total"], x["messages"]), reverse=True)
    if ping_pong_candidates:
        patterns.append({
            "type": "ping_pong",
            "strength": ping_pong_candidates[0]["balance"],
            "description": "Detected balanced two-way traffic between a small number of rank pairs.",
            "pairs": ping_pong_candidates[:5],
            "metrics": {
                "pair_count": len(ping_pong_candidates),
            },
        })

    # Issues / heuristic findings.
    issues = []

    # Issue: small-message dominated traffic
    small128_total = sum(1 for e in sorted_timeline if _is_canonical_transfer_event(e) and e.get("bytes", 0) < 128)
    small1k_total = sum(1 for e in sorted_timeline if _is_canonical_transfer_event(e) and e.get("bytes", 0) < 1024)
    small128_frac = _safe_div(small128_total, canonical_total_events)
    small1k_frac = _safe_div(small1k_total, canonical_total_events)

    if canonical_total_events >= 50 and (small128_frac >= 0.5 or small1k_frac >= 0.8):
        score = max(small128_frac, 0.8 * small1k_frac)
        issues.append({
            "type": "small_message_overhead",
            "severity": _severity(score),
            "score": score,
            "description": "A large fraction of canonical transfers are very small, which may indicate latency-dominated communication.",
            "metrics": {
                "small_under_128B_fraction": small128_frac,
                "small_under_1KB_fraction": small1k_frac,
                "canonical_transfer_events": canonical_total_events,
                "top_small_message_calls_under_128B": dict(_top_dict_items(small128_by_call, 8)),
                "top_small_message_calls_under_1KB": dict(_top_dict_items(small1k_by_call, 8)),
            },
        })

    # Issue: rank communication imbalance
    if total_touch_bytes:
        max_touch = max(total_touch_bytes)
        mean_touch = _mean(total_touch_bytes)
        cv_touch = _cv(total_touch_bytes)
        score = max(_safe_div(max_touch, max(mean_touch, 1.0)) / 4.0, min(cv_touch, 1.0))

        if mean_touch > 0 and (max_touch > 2.0 * mean_touch or cv_touch > 0.5):
            worst_rank = max(per_rank_list, key=lambda x: x["touch_bytes"])["rank"]
            issues.append({
                "type": "communication_imbalance",
                "severity": _severity(score),
                "score": score,
                "description": "Communication volume is imbalanced across ranks; rank {} is substantially busier than average.".format(
                    worst_rank
                ),
                "ranks": [worst_rank],
                "metrics": {
                    "max_touch_bytes": max_touch,
                    "mean_touch_bytes": mean_touch,
                    "touch_bytes_cv": cv_touch,
                    "max_over_mean": _safe_div(max_touch, max(mean_touch, 1.0)),
                },
            })

    # Issue: barrier skew / load imbalance
    if barrier_spreads and runtime > 0.0:
        relative_max_spread = _safe_div(max_barrier_spread, runtime)
        relative_avg_spread = _safe_div(avg_barrier_spread, runtime)
        score = max(relative_max_spread * 5.0, relative_avg_spread * 10.0)

        if relative_max_spread >= 0.02 or relative_avg_spread >= 0.005:
            issues.append({
                "type": "barrier_imbalance",
                "severity": _severity(score),
                "score": min(score, 1.0),
                "description": "Barrier arrival times are spread out, suggesting load imbalance or uneven progress before synchronization points.",
                "metrics": {
                    "common_barrier_count": common_barriers,
                    "max_barrier_spread": max_barrier_spread,
                    "avg_barrier_spread": avg_barrier_spread,
                    "max_spread_fraction_of_runtime": relative_max_spread,
                    "avg_spread_fraction_of_runtime": relative_avg_spread,
                    "top_spread_barriers": sorted(barrier_spreads, key=lambda x: x["spread"], reverse=True)[:10],
                },
            })

    # Issue: synchronization-heavy behaviour
    sync_completion_ratio = _safe_div(completion_total_events + barrier_total_events, max(1, canonical_total_events))
    if canonical_total_events > 0 and sync_completion_ratio >= 0.5:
        score = min(sync_completion_ratio / 2.0, 1.0)
        issues.append({
            "type": "synchronization_heavy",
            "severity": _severity(score),
            "score": score,
            "description": "The trace contains many wait/test/barrier events relative to canonical data transfers.",
            "metrics": {
                "completion_events": completion_total_events,
                "barrier_events": barrier_total_events,
                "canonical_transfer_events": canonical_total_events,
                "sync_plus_completion_per_transfer": sync_completion_ratio,
            },
        })

    # Issue: root bottleneck in rooted collectives
    if rooted_collective_summary:
        total_rooted_bytes = sum(x["bytes"] for x in rooted_collective_summary)
        if total_rooted_bytes > 0:
            top_root = rooted_collective_summary[0]
            top_root_frac = _safe_div(top_root["bytes"], total_rooted_bytes)
            score = top_root_frac

            if top_root_frac >= 0.5:
                issues.append({
                    "type": "collective_root_bottleneck",
                    "severity": _severity(score),
                    "score": score,
                    "description": "Rooted collective traffic is concentrated on rank {}, which may become a communication bottleneck.".format(
                        top_root["root"]
                    ),
                    "ranks": [top_root["root"]],
                    "metrics": {
                        "rooted_collective_fraction": top_root_frac,
                        "rooted_collective_bytes": top_root["bytes"],
                        "rooted_collective_events": top_root["events"],
                        "calls": top_root["calls"],
                    },
                })

    # Issue: hotspot link
    if top_links and canonical_total_bytes > 0:
        hottest = top_links[0]
        hottest_frac = _safe_div(hottest["bytes"], canonical_total_bytes)
        score = hottest_frac

        if hottest_frac >= 0.2:
            issues.append({
                "type": "link_hotspot",
                "severity": _severity(score),
                "score": score,
                "description": "A single sender/receiver link carries a large fraction of canonical traffic.",
                "pairs": [[hottest["sender"], hottest["receiver"]]],
                "metrics": {
                    "sender": hottest["sender"],
                    "receiver": hottest["receiver"],
                    "bytes": hottest["bytes"],
                    "messages": hottest["messages"],
                    "fraction_of_canonical_bytes": hottest_frac,
                },
            })

    # Issue: many globally synchronising collectives
    global_collective_ratio = _safe_div(global_collective_total_events, max(1, collective_total_events))
    if global_collective_total_events >= 20 and global_collective_ratio >= 0.5:
        score = min(global_collective_ratio, 1.0)
        issues.append({
            "type": "global_collective_heavy",
            "severity": _severity(score),
            "score": score,
            "description": "A large fraction of collective activity consists of globally synchronising collectives such as Allreduce/Allgather.",
            "metrics": {
                "global_collective_events": global_collective_total_events,
                "collective_events": collective_total_events,
                "global_collective_fraction": global_collective_ratio,
            },
        })

    # Sort issues so the most severe appear first.
    issues.sort(key=lambda x: x.get("score", 0.0), reverse=True)

    # Time windows for visualisation.
    window_count = 1
    if runtime > 0.0:
        window_count = min(40, max(10, len(sorted_timeline) // 50000 + 10))
    time_windows = _build_time_windows(sorted_timeline, window_count)

    print("\nAnalysis Complete.")

    return {
        "summary": {
            "total_events": len(sorted_timeline),
            "canonical_transfer_events": canonical_total_events,
            "canonical_transfer_bytes": canonical_total_bytes,
            "completion_events": completion_total_events,
            "barrier_events": barrier_total_events,
            "collective_events": collective_total_events,
            "estimated_runtime": runtime,
            "pair_density": pair_density,
            "avg_out_peers": avg_out_peers,
            "avg_in_peers": avg_in_peers,
        },
        "per_rank": per_rank_list,
        "top_ranks_by_out_bytes": top_ranks_by_out,
        "top_ranks_by_in_bytes": top_ranks_by_in,
        "top_ranks_by_touch_bytes": top_ranks_by_touch,
        "top_links": top_links,
        "collective_roots": rooted_collective_summary,
        "barrier_spreads": barrier_spreads,
        "patterns": patterns,
        "issues": issues,
        "time_windows": time_windows,
    }

# -----------------------------------------------------------------------------
# Main parser
# -----------------------------------------------------------------------------

def parse_mpic_file(mpic_filepath, hw_filepath=None):
    if not os.path.exists(mpic_filepath):
        print("Error: File '{}' not found.".format(mpic_filepath), file=sys.stderr)
        sys.exit(1)

    process_info_fmt = "=i i i i 1024s"
    p2p_small_fmt = "=d i i i i i i"
    p2p_large_fmt = "=d i i i i i i i i i i"

    process_info_size = struct.calcsize(process_info_fmt)

    data = {
        "metadata": {
            "total_ranks": 0,
            "date": "",
            "program": "unknown",
            "system_name": "Unknown Cluster",
        },
        "topology": [],
        "timeline": [],
        "statistics": {},
    }

    hw_lookup = load_hardware_map(hw_filepath) if hw_filepath else {}

    with _open_maybe_gzip(mpic_filepath) as f:
        global_header_fmt = "=i 64s 1024s"
        global_header_size = struct.calcsize(global_header_fmt)

        header_bytes = f.read(global_header_size)
        if not header_bytes or len(header_bytes) < global_header_size:
            print("Error: Empty or corrupted file header.", file=sys.stderr)
            sys.exit(1)

        total_ranks, raw_date, raw_prog = struct.unpack(global_header_fmt, header_bytes)

        run_date = _cstr(raw_date)
        prog_name = _cstr(raw_prog)

        data["metadata"]["total_ranks"] = total_ranks
        data["metadata"]["date"] = run_date
        data["metadata"]["program"] = prog_name

        # Process information.
        for idx in range(total_ranks):
            proc_bytes = f.read(process_info_size)
            if len(proc_bytes) != process_info_size:
                print("Error: truncated process info block at rank index {}.".format(idx), file=sys.stderr)
                sys.exit(1)

            rank, pid, core, chip, hostname_b = struct.unpack(process_info_fmt, proc_bytes)
            hostname = _cstr(hostname_b)

            hw_info = hw_lookup.get(hostname, {
                "x": rank * 15,
                "y": 0,
                "z": 0,
                "cab_id": None,
                "rack_id": None,
                "blade_id": None,
            })

            topo_entry = {
                "rank": rank,
                "pid": pid,
                "core": core,
                "chip": chip,
                "hostname": hostname,
                "x": hw_info.get("x", rank * 15),
                "y": hw_info.get("y", 0),
                "z": hw_info.get("z", 0),
            }

            if "cab_id" in hw_info:
                topo_entry["cab_id"] = hw_info.get("cab_id")
            if "rack_id" in hw_info:
                topo_entry["rack_id"] = hw_info.get("rack_id")
            if "blade_id" in hw_info:
                topo_entry["blade_id"] = hw_info.get("blade_id")

            data["topology"].append(topo_entry)

        raw_sections = f.read()

    # Preferred parse is strict; salvage mode if needed.
    try:
        _parse_sections_strict(
            raw_sections,
            data["metadata"]["total_ranks"],
            data,
            p2p_small_fmt,
            p2p_large_fmt,
        )
    except Exception as strict_err:
        print(
            "Warning: strict parse failed ({}). Falling back to salvage parser.".format(strict_err),
            file=sys.stderr
        )
        data["timeline"] = []
        data["statistics"] = {}
        _parse_sections_salvage(
            raw_sections,
            len(raw_sections),
            data,
            p2p_small_fmt,
            p2p_large_fmt,
        )


    print("Sorting the timeline...")

    # Chronological ordering.
    data["timeline"].sort(key=lambda x: (x["time"], x["event_id"], x["rank_recording"]))

    print("Reading in the hardware blueprint...");

    # Hardware blueprint.
    if hw_filepath and os.path.exists(hw_filepath):
        with open(hw_filepath, "r") as f:
            blueprint = json.load(f)
            data["hardware_blueprint"] = blueprint

            if "metadata" in blueprint and "system_name" in blueprint["metadata"]:
                data["metadata"]["system_name"] = blueprint["metadata"]["system_name"]
    else:
        data["hardware_blueprint"] = None
     
    print("Running analysis on the timeline...")
    data["analysis"] = analyse_trace(data)

    # Chunk and compress the timeline.
    CHUNK_SIZE = 500000
    chunks_index = []
    compressed_payloads = []
    current_byte_offset = 0

    total_events = len(data["timeline"])
    total_chunks = (total_events + CHUNK_SIZE - 1) // CHUNK_SIZE

    print("Compressing {} chunks...".format(total_chunks))
    print_progress(0, total_chunks, prefix='Compressing:  ', suffix='Complete', length=40)

    for idx, i in enumerate(range(0, total_events, CHUNK_SIZE)):

        chunk_data = data["timeline"][i:i + CHUNK_SIZE]
        chunk_json = json.dumps(chunk_data, separators=(",", ":")).encode("utf-8")
        compressed_chunk = zlib.compress(chunk_json)

        chunks_index.append({
            "t_start": chunk_data[0]["time"],
            "t_end": chunk_data[-1]["time"],
            "offset": current_byte_offset,
            "size": len(compressed_chunk),
        })

        compressed_payloads.append(compressed_chunk)
        current_byte_offset += len(compressed_chunk)

        print_progress(idx + 1, total_chunks, prefix='Compressing:  ', suffix='Complete', length=40)

    header_data = {
        "metadata": data["metadata"],
        "topology": data["topology"],
        "statistics": data["statistics"],
        "hardware_blueprint": data["hardware_blueprint"],
        "analysis": data["analysis"],
        "chunks": chunks_index,
    }

    header_json = json.dumps(header_data, separators=(",", ":")).encode("utf-8")
    compressed_header = zlib.compress(header_json)
    header_length = len(compressed_header)

    # Output filename handling.
    output_filename = mpic_filepath
    if output_filename.endswith(".mpic.gz"):
        output_filename = output_filename[:-8] + ".mpix"
    elif output_filename.endswith(".mpic"):
        output_filename = output_filename[:-5] + ".mpix"
    else:
        output_filename = output_filename + ".mpix"

    with open(output_filename, "wb") as f:
        f.write(struct.pack("<I", header_length))
        f.write(compressed_header)
        for payload in compressed_payloads:
            f.write(payload)

    print("Packed {} chunks into a single {} container.".format(len(chunks_index), output_filename))
    print("Parsed {} communication events.".format(len(data["timeline"])))
    print("Data saved to {}".format(output_filename))

    print_summary_table(data["statistics"])
    print_analysis_summary(data["analysis"])


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parse_mpic.py <filename.mpic|filename.mpic.gz> [hardware_map.json]")
        sys.exit(1)

    hw_file = sys.argv[2] if len(sys.argv) > 2 else None
    parse_mpic_file(sys.argv[1], hw_file)

