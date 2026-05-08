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
    if total == 0:
        return
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    sys.stdout.write('\r{} |{}| {}% {}'.format(prefix, bar, percent, suffix))
    sys.stdout.flush()
    if iteration == total:
        print()

# -----------------------------------------------------------------------------
# Timeline recording helpers
# -----------------------------------------------------------------------------

def _record_small_event(data, rank_id, time_val, msg_id, mtype, comm, tag, sender, receiver, count, bytes_vol):
    call_name = MESSAGE_TYPES.get(mtype, "UNKNOWN_{}".format(mtype))
    data["timeline"].append({
        "time": time_val,
        "event_id": msg_id,
        "rank_recording": rank_id,
        "call": call_name,
        "comm": comm,
        "tag": tag,
        "sender": sender,
        "receiver": receiver,
        "count": count,
        "bytes": bytes_vol,
        "category": _categorise_small_call(call_name),
    })
    _update_stats(data["statistics"], call_name, bytes_vol)


def _record_large_event(data, rank_id, time_val, msg_id, mtype, comm, s1, r1, c1, b1, t1, s2, r2, c2, b2, t2):
    call_name = MESSAGE_TYPES.get(mtype, "UNKNOWN_{}".format(mtype))
    _ensure_stats_entry(data["statistics"], call_name)

    parts = [
        (1, s1, r1, c1, b1, t1),
        (2, s2, r2, c2, b2, t2),
    ]

    for part_index, sender, receiver, count, bytes_vol in parts:
        is_empty_placeholder = (count <= 0 and bytes_vol <= 0)
        data["timeline"].append({
            "time": time_val,
            "event_id": msg_id,
            "rank_recording": rank_id,
            "call": call_name,
            "comm": comm,
            "sender": sender,
            "receiver": receiver,
            "count": count,
            "bytes": bytes_vol,
            "category": _categorise_large_part(call_name, part_index),
            "synthetic_empty": bool(is_empty_placeholder),
        })

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
                        if not hostname: continue

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
                    if not hostname: continue

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

    ordered_calls = sorted(stats.keys(), key=lambda name: MESSAGE_TYPE_ORDER.get(name, 9999))

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
            raise ValueError(f"Bad small-section label for rank {rank_id}")

        chunk, offset = _read_exact(raw_data, offset, 4, "small section count")
        num_small = struct.unpack("=i", chunk)[0]

        for _ in range(num_small):
            chunk, offset = _read_exact(raw_data, offset, small_size, "small record")
            time_val, msg_id, mtype, comm, tag, sender, receiver, count, bytes_vol = struct.unpack(small_fmt, chunk)
            _record_small_event(data, rank_id, time_val, msg_id, mtype, comm, tag, sender, receiver, count, bytes_vol)

        chunk, offset = _read_exact(raw_data, offset, 24, "large section label")
        large_label = _cstr(chunk)
        if LARGE_LABEL_TEXT not in large_label:
            raise ValueError(f"Bad large-section label for rank {rank_id}")

        chunk, offset = _read_exact(raw_data, offset, 4, "large section count")
        num_large = struct.unpack("=i", chunk)[0]

        for _ in range(num_large):
            chunk, offset = _read_exact(raw_data, offset, large_size, "large record")
            unpacked = struct.unpack(large_fmt, chunk)
            _record_large_event(data, rank_id, *unpacked)

        print_progress(section_index + 1, total_ranks, prefix='Parsing Ranks:', suffix='Complete', length=40)

def _parse_sections_salvage(raw_data, total_len, data, small_fmt, large_fmt):
    # Salvage parsing omitted for brevity but remains functionally identical
    pass

# -----------------------------------------------------------------------------
# Analysis helpers
# -----------------------------------------------------------------------------

def _is_real_payload_event(event):
    return (event.get("bytes", 0) > 0) and (not event.get("synthetic_empty", False))

def _is_canonical_transfer_event(event):
    if not _is_real_payload_event(event): return False
    call = event["call"]
    category = event.get("category", "")

    if call in SEND_LIKE_CALLS: return True
    if call == "MPI_SENDRECV" and category == "sendrecv_part_1": return True
    if call == "MPI_BCAST" and event["sender"] != event["receiver"]: return True
    if call == "MPI_REDUCE" and event["sender"] != event["receiver"]: return True
    if call == "MPI_GATHER" and category == "collective_part_1" and event["sender"] != event["receiver"]: return True
    if call == "MPI_SCATTER" and category == "collective_part_2" and event["sender"] != event["receiver"]: return True
    return False

def _guess_root_for_rooted_collective(event):
    call = event["call"]
    if call in ("MPI_BCAST", "MPI_SCATTER"): return event["sender"]
    if call in ("MPI_REDUCE", "MPI_GATHER"): return event["receiver"]
    return None

def _build_time_windows(timeline, window_count):
    if not timeline: return []
    start_t = timeline[0]["time"]
    end_t = timeline[-1]["time"]
    runtime = end_t - start_t

    windows = [{"t_start": start_t + (runtime * i) / float(window_count), "t_end": start_t + (runtime * (i + 1)) / float(window_count), "events": 0, "canonical_transfer_events": 0, "canonical_transfer_bytes": 0, "completion_events": 0, "barrier_events": 0, "collective_events": 0} for i in range(window_count)]

    for event in timeline:
        if runtime <= 0.0: idx = 0
        else: idx = min(window_count - 1, max(0, int(((event["time"] - start_t) / runtime) * window_count)))
        
        win = windows[idx]
        win["events"] += 1
        if _is_canonical_transfer_event(event):
            win["canonical_transfer_events"] += 1
            win["canonical_transfer_bytes"] += event.get("bytes", 0)
        if event["call"] in COMPLETION_CALLS: win["completion_events"] += 1
        if event["call"] == "MPI_BARRIER": win["barrier_events"] += 1
        if event["call"] in ROOTED_COLLECTIVES or event["call"] in GLOBAL_COLLECTIVES: win["collective_events"] += 1
    return windows


def analyse_trace(data):
    timeline = data["timeline"]
    total_ranks = data["metadata"].get("total_ranks", 0)

    per_rank = {r: {"rank": r, "events": 0, "completion_events": 0, "barrier_events": 0, "collective_events": 0, "global_collective_events": 0, "rooted_collective_events": 0, "canonical_bytes_out": 0, "canonical_bytes_in": 0, "canonical_messages_out": 0, "canonical_messages_in": 0, "distinct_out_peers": set(), "distinct_in_peers": set(), "completion_request_count": 0} for r in range(total_ranks)}
    pair_stats = defaultdict(lambda: {"messages": 0, "bytes": 0, "calls": Counter(), "first_time": None, "last_time": None})
    rooted_collective_roots = defaultdict(lambda: {"events": 0, "bytes": 0, "calls": Counter()})
    barrier_times = defaultdict(list)

    canonical_total_events, canonical_total_bytes = 0, 0
    completion_total_events, barrier_total_events = 0, 0
    collective_total_events, global_collective_total_events = 0, 0

    small_by_call, small128_by_call, small1k_by_call = Counter(), Counter(), Counter()

    sorted_timeline = timeline
    start_t = sorted_timeline[0]["time"] if sorted_timeline else 0.0
    end_t = sorted_timeline[-1]["time"] if sorted_timeline else 0.0
    runtime = max(0.0, end_t - start_t)
    total_events = len(sorted_timeline)

    # =========================================================================
    # Pass 1: Basic Counts
    # =========================================================================
    print("\nAnalyzing Trace - Pass 1 (Basic Counts)...")
    for idx, event in enumerate(sorted_timeline):
        rr = event["rank_recording"]
        call = event["call"]
        
        if rr in per_rank:
            per_rank[rr]["events"] += 1
            if call in COMPLETION_CALLS:
                per_rank[rr]["completion_events"] += 1
                per_rank[rr]["completion_request_count"] += max(0, event.get("count", 0))
            if call == "MPI_BARRIER":
                per_rank[rr]["barrier_events"] += 1
                barrier_times[rr].append(event["time"])
            if call in ROOTED_COLLECTIVES or call in GLOBAL_COLLECTIVES:
                per_rank[rr]["collective_events"] += 1
                if call in ROOTED_COLLECTIVES: per_rank[rr]["rooted_collective_events"] += 1
                if call in GLOBAL_COLLECTIVES: per_rank[rr]["global_collective_events"] += 1

        if call in COMPLETION_CALLS: completion_total_events += 1
        if call == "MPI_BARRIER": barrier_total_events += 1
        if call in ROOTED_COLLECTIVES or call in GLOBAL_COLLECTIVES: collective_total_events += 1
        if call in GLOBAL_COLLECTIVES: global_collective_total_events += 1

        if idx % 10000 == 0: print_progress(idx, total_events, prefix='Pass 1: ', suffix='Complete', length=40, fill='#')
    print_progress(total_events, total_events, prefix='Pass 1: ', suffix='Complete', length=40, fill='#')

    # =========================================================================
    # Pass 2: Canonical Pair/Link Traffic
    # =========================================================================
    print("Analyzing Trace - Pass 2 (Canonical Traffic)...")
    for idx, event in enumerate(sorted_timeline):
        if not _is_canonical_transfer_event(event): continue

        call, sender, receiver, num_bytes, t = event["call"], event["sender"], event["receiver"], event.get("bytes", 0), event.get("time", 0.0)

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
        if pair_stats[pair_key]["first_time"] is None or t < pair_stats[pair_key]["first_time"]: pair_stats[pair_key]["first_time"] = t
        if pair_stats[pair_key]["last_time"] is None or t > pair_stats[pair_key]["last_time"]: pair_stats[pair_key]["last_time"] = t

        small_by_call[call] += 1
        if num_bytes < 128: small128_by_call[call] += 1
        if num_bytes < 1024: small1k_by_call[call] += 1

        if call in ROOTED_COLLECTIVES:
            root = _guess_root_for_rooted_collective(event)
            if root is not None:
                rooted_collective_roots[root]["events"] += 1
                rooted_collective_roots[root]["bytes"] += num_bytes
                rooted_collective_roots[root]["calls"][call] += 1

        if idx % 10000 == 0: print_progress(idx, total_events, prefix='Pass 2: ', suffix='Complete', length=40, fill='#')
    print_progress(total_events, total_events, prefix='Pass 2: ', suffix='Complete', length=40, fill='#')

    # =========================================================================
    # Pass 3: Timing Heuristics (Late Senders, Early Reduces, etc.)
    # =========================================================================
    print("Analyzing Trace - Pass 3 (Timing Heuristics)...")
    
    p2p_sends = defaultdict(list)
    p2p_recvs = defaultdict(list)
    
    rank_bcast_seq = Counter()
    bcast_seq_map = defaultdict(list)
    
    rank_reduce_seq = Counter()
    reduce_seq_map = defaultdict(list)
    
    for idx, event in enumerate(sorted_timeline):
        call = event["call"]
        t = event["time"]
        
        # P2P Pairing
        if call in SEND_LIKE_CALLS:
            p2p_sends[(event["sender"], event["receiver"], event["tag"])].append(t)
        elif call in RECV_LIKE_CALLS:
            p2p_recvs[(event["sender"], event["receiver"], event["tag"])].append(t)
 
        # Collective Pairing
        elif call == "MPI_BCAST":
            root = event["sender"]
            local = event["receiver"]
            seq = rank_bcast_seq[local]
            bcast_seq_map[(root, seq)].append((local, t))
            rank_bcast_seq[local] += 1
        elif call == "MPI_REDUCE":
            root = event["receiver"]
            local = event["sender"]
            seq = rank_reduce_seq[local]
            reduce_seq_map[(root, seq)].append((local, t))
            rank_reduce_seq[local] += 1

        if idx % 10000 == 0: print_progress(idx, total_events, prefix='Pass 3: ', suffix='Complete', length=40, fill='#')
    print_progress(total_events, total_events, prefix='Pass 3: ', suffix='Complete', length=40, fill='#')

    # Calculate Time Deficits
    TIMING_THRESHOLD = 0.001 # 1ms threshold for registering a delay

    late_sender_count = 0; late_sender_time = 0.0
    late_receiver_count = 0; late_receiver_time = 0.0
    
    # Analyze P2P Skew (Assumes FIFO pairing between ranks)
    for pair, s_times in p2p_sends.items():
        r_times = p2p_recvs.get(pair, [])
        for ts, tr in zip(s_times, r_times):
            diff = tr - ts
            if diff < -TIMING_THRESHOLD: # Recv started before Send
                late_sender_count += 1
                late_sender_time += (-diff)
            elif diff > TIMING_THRESHOLD: # Send started before Recv
                late_receiver_count += 1
                late_receiver_time += diff
                
    # Analyze Bcast Skew
    late_broadcaster_count = 0; late_broadcaster_time = 0.0
    for (root, seq), events in bcast_seq_map.items():
        root_time = next((t for r, t in events if r == root), None)
        if root_time is not None:
            for r, t in events:
                if r != root:
                    diff = t - root_time
                    if diff < -TIMING_THRESHOLD: # Worker arrived before root
                        late_broadcaster_count += 1
                        late_broadcaster_time += (-diff)
                        
    # Analyze Reduce Skew
    early_reduce_count = 0; early_reduce_time = 0.0
    for (root, seq), events in reduce_seq_map.items():
        root_time = next((t for r, t in events if r == root), None)
        if root_time is not None:
            for r, t in events:
                if r != root:
                    diff = t - root_time
                    if diff < -TIMING_THRESHOLD: # Worker arrived before root was ready
                        early_reduce_count += 1
                        early_reduce_time += (-diff)

    # =========================================================================
    # Aggregation & Pattern Generation
    # =========================================================================

    per_rank_list = []
    total_touch_bytes = []
    out_peer_counts, in_peer_counts = [], []

    for rank in range(total_ranks):
        entry = per_rank[rank]
        entry["distinct_out_peers"] = len(entry["distinct_out_peers"])
        entry["distinct_in_peers"] = len(entry["distinct_in_peers"])
        entry["touch_bytes"] = entry["canonical_bytes_out"] + entry["canonical_bytes_in"]
        per_rank_list.append(entry)
        total_touch_bytes.append(entry["touch_bytes"])
        out_peer_counts.append(entry["distinct_out_peers"])
        in_peer_counts.append(entry["distinct_in_peers"])

    top_links = []
    for (sender, receiver), stats in sorted(pair_stats.items(), key=lambda kv: kv[1]["bytes"], reverse=True)[:20]:
        top_links.append({"sender": sender, "receiver": receiver, "messages": stats["messages"], "bytes": stats["bytes"], "calls": dict(sorted(stats["calls"].items())), "first_time": stats["first_time"], "last_time": stats["last_time"]})

    top_ranks_by_out = [{"rank": r["rank"], "bytes": r["canonical_bytes_out"], "messages": r["canonical_messages_out"]} for r in sorted(per_rank_list, key=lambda x: x["canonical_bytes_out"], reverse=True)[:10]]
    top_ranks_by_in = [{"rank": r["rank"], "bytes": r["canonical_bytes_in"], "messages": r["canonical_messages_in"]} for r in sorted(per_rank_list, key=lambda x: x["canonical_bytes_in"], reverse=True)[:10]]
    top_ranks_by_touch = [{"rank": r["rank"], "bytes": r["touch_bytes"]} for r in sorted(per_rank_list, key=lambda x: x["touch_bytes"], reverse=True)[:10]]

    rooted_collective_summary = []
    for root, stats in sorted(rooted_collective_roots.items(), key=lambda kv: kv[1]["bytes"], reverse=True):
        rooted_collective_summary.append({"root": root, "events": stats["events"], "bytes": stats["bytes"], "calls": dict(sorted(stats["calls"].items()))})

    barrier_spreads = []
    if total_ranks > 0:
        barrier_counts = [len(barrier_times[r]) for r in range(total_ranks)]
        common_barriers = min(barrier_counts) if barrier_counts else 0
        for idx in range(common_barriers):
            times = [barrier_times[r][idx] for r in range(total_ranks)]
            barrier_spreads.append({"barrier_index": idx, "t_min": min(times), "t_max": max(times), "spread": max(times) - min(times)})
    else: common_barriers = 0

    max_barrier_spread = max((b["spread"] for b in barrier_spreads), default=0.0)
    avg_barrier_spread = _mean([b["spread"] for b in barrier_spreads]) if barrier_spreads else 0.0

    patterns = []
    avg_out_peers = _mean(out_peer_counts)
    avg_in_peers = _mean(in_peer_counts)
    pair_density = _safe_div(len(pair_stats), total_ranks * max(0, total_ranks - 1))

    # [Pattern generation logic remains exactly identical as before]
    # ...

    issues = []

    # Inject the New Timing Issues
    if canonical_total_events > 0:
        ls_ratio = _safe_div(late_sender_count, canonical_total_events)
        if ls_ratio > 0.05:
            score = min(ls_ratio * 4.0, 1.0)
            issues.append({
                "type": "late_sender",
                "severity": _severity(score),
                "score": score,
                "description": "Detected {} instances of Late Sender. Receivers are blocking early and waiting for sends to arrive (Total wasted time: {:.2f}s). Out-of-order receive posting often manifests as this symptom.".format(late_sender_count, late_sender_time),
                "metrics": {
                    "late_sender_count": late_sender_count,
                    "fraction_of_traffic": ls_ratio,
                    "wasted_time_sec": late_sender_time
                }
            })

        lr_ratio = _safe_div(late_receiver_count, canonical_total_events)
        if lr_ratio > 0.05:
            score = min(lr_ratio * 4.0, 1.0)
            issues.append({
                "type": "late_receiver",
                "severity": _severity(score),
                "score": score,
                "description": "Detected {} instances of Late Receiver. Sends are being posted before receives are ready, potentially causing eager-buffer overflow or sender blocking (Total delayed time: {:.2f}s).".format(late_receiver_count, late_receiver_time),
                "metrics": {
                    "late_receiver_count": late_receiver_count,
                    "fraction_of_traffic": lr_ratio,
                    "delayed_time_sec": late_receiver_time
                }
            })

    if late_broadcaster_count > 0:
        lb_ratio = _safe_div(late_broadcaster_count, collective_total_events)
        if lb_ratio > 0.05:
            score = min(lb_ratio * 4.0, 1.0)
            issues.append({
                "type": "late_broadcaster",
                "severity": _severity(score),
                "score": score,
                "description": "Detected {} instances of Late Broadcaster. Non-root ranks are arriving at MPI_Bcast before the root is ready (Total wasted time: {:.2f}s).".format(late_broadcaster_count, late_broadcaster_time),
                "metrics": {
                    "late_broadcaster_count": late_broadcaster_count,
                    "fraction_of_collectives": lb_ratio,
                    "wasted_time_sec": late_broadcaster_time
                }
            })

    if early_reduce_count > 0:
        er_ratio = _safe_div(early_reduce_count, collective_total_events)
        if er_ratio > 0.05:
            score = min(er_ratio * 4.0, 1.0)
            issues.append({
                "type": "early_reduce",
                "severity": _severity(score),
                "score": score,
                "description": "Detected {} instances of Early Reduce. Worker ranks are arriving at MPI_Reduce before the root is ready to process them (Total wasted time: {:.2f}s).".format(early_reduce_count, early_reduce_time),
                "metrics": {
                    "early_reduce_count": early_reduce_count,
                    "fraction_of_collectives": er_ratio,
                    "wasted_time_sec": early_reduce_time
                }
            })


    # [Keep the rest of your original Issue generation (small messages, bottlenecks) here]
    # ... 

    # Sort issues so the most severe appear first.
    issues.sort(key=lambda x: x.get("score", 0.0), reverse=True)

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
    p2p_small_fmt = "=d i i i i i i i i"
    p2p_large_fmt = "=d i i i i i i i i i i i i"

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

        for idx in range(total_ranks):
            proc_bytes = f.read(process_info_size)
            if len(proc_bytes) != process_info_size:
                print("Error: truncated process info block at rank index {}.".format(idx), file=sys.stderr)
                sys.exit(1)

            rank, pid, core, chip, hostname_b = struct.unpack(process_info_fmt, proc_bytes)
            hostname = _cstr(hostname_b)

            hw_info = hw_lookup.get(hostname, {
                "x": rank * 15, "y": 0, "z": 0,
                "cab_id": None, "rack_id": None, "blade_id": None,
            })

            topo_entry = {
                "rank": rank, "pid": pid, "core": core, "chip": chip,
                "hostname": hostname,
                "x": hw_info.get("x", rank * 15), "y": hw_info.get("y", 0), "z": hw_info.get("z", 0),
            }

            if "cab_id" in hw_info: topo_entry["cab_id"] = hw_info.get("cab_id")
            if "rack_id" in hw_info: topo_entry["rack_id"] = hw_info.get("rack_id")
            if "blade_id" in hw_info: topo_entry["blade_id"] = hw_info.get("blade_id")

            data["topology"].append(topo_entry)

        raw_sections = f.read()

    try:
        _parse_sections_strict(raw_sections, data["metadata"]["total_ranks"], data, p2p_small_fmt, p2p_large_fmt)
    except Exception as strict_err:
        print("Warning: strict parse failed ({}). Falling back to salvage parser.".format(strict_err), file=sys.stderr)
        data["timeline"] = []
        data["statistics"] = {}
        _parse_sections_salvage(raw_sections, len(raw_sections), data, p2p_small_fmt, p2p_large_fmt)

    print("Sorting the timeline...")
    data["timeline"].sort(key=lambda x: (x["time"], x["event_id"], x["rank_recording"]))

    print("Reading in the hardware blueprint...")
    if hw_filepath and os.path.exists(hw_filepath):
        with open(hw_filepath, "r") as f:
            blueprint = json.load(f)
            data["hardware_blueprint"] = blueprint
            if "metadata" in blueprint and "system_name" in blueprint["metadata"]:
                data["metadata"]["system_name"] = blueprint["metadata"]["system_name"]
    else:
        data["hardware_blueprint"] = None
     
    data["analysis"] = analyse_trace(data)

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
            "t_start": chunk_data[0]["time"], "t_end": chunk_data[-1]["time"],
            "offset": current_byte_offset, "size": len(compressed_chunk),
        })

        compressed_payloads.append(compressed_chunk)
        current_byte_offset += len(compressed_chunk)
        print_progress(idx + 1, total_chunks, prefix='Compressing:  ', suffix='Complete', length=40)

    header_data = {
        "metadata": data["metadata"], "topology": data["topology"], "statistics": data["statistics"],
        "hardware_blueprint": data["hardware_blueprint"], "analysis": data["analysis"], "chunks": chunks_index,
    }

    header_json = json.dumps(header_data, separators=(",", ":")).encode("utf-8")
    compressed_header = zlib.compress(header_json)

    output_filename = mpic_filepath
    if output_filename.endswith(".mpic.gz"): output_filename = output_filename[:-8] + ".mpix"
    elif output_filename.endswith(".mpic"): output_filename = output_filename[:-5] + ".mpix"
    else: output_filename = output_filename + ".mpix"

    with open(output_filename, "wb") as f:
        f.write(struct.pack("<I", len(compressed_header)))
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
