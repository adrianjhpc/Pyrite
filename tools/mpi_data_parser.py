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
    39: "MPI_INIT",
    40: "MPI_FINALIZE",
}

MESSAGE_TYPE_ORDER = {name: code for code, name in MESSAGE_TYPES.items()}

SMALL_LABEL_TEXT = "P2P Small Type Messages"
LARGE_LABEL_TEXT = "P2P Large Type Messages"

MPIC_V2_MAGIC = b"MPICv002"
MPIC_V2_VERSION = 2

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
    "MPI_BARRIER", "MPI_INIT", "MPI_FINALIZE",
}

ROOTED_COLLECTIVES = {
    "MPI_BCAST", "MPI_REDUCE", "MPI_GATHER", "MPI_SCATTER",
}

GLOBAL_COLLECTIVES = {
    "MPI_ALLREDUCE", "MPI_ALLGATHER",
}

# -----------------------------------------------------------------------------
# Binary layouts
# -----------------------------------------------------------------------------
#
# These match the current writer behaviour, including padding where needed.
# The file format is not portable across arbitrary ABIs, because the writer
# emits raw C structs.
#

PROCESS_INFO_FMT = "=iiii1024s"
PROCESS_INFO_SIZE = struct.calcsize(PROCESS_INFO_FMT)

# int rank; padding 4; double mpi_time_zero; int64_t unix_time_zero_ns;
PROCESS_TIME_ANCHOR_FMT = "=i4xdq"
PROCESS_TIME_ANCHOR_SIZE = struct.calcsize(PROCESS_TIME_ANCHOR_FMT)

# double + 8 ints = 40 bytes
P2P_SMALL_FMT = "=diiiiiiii"
P2P_SMALL_SIZE = struct.calcsize(P2P_SMALL_FMT)

# double + 13 ints + 4 bytes tail padding = 64 bytes
P2P_LARGE_FMT = "=diiiiiiiiiiiii4x"
P2P_LARGE_SIZE = struct.calcsize(P2P_LARGE_FMT)

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

def _record_small_event(data, rank_id, local_time, msg_id, mtype, comm, tag,
                        sender, receiver, count, bytes_vol):
    call_name = MESSAGE_TYPES.get(mtype, "UNKNOWN_{}".format(mtype))
    data["timeline"].append({
        "local_time": local_time,
        "time": local_time,          # overwritten later if anchors are present
        "epoch_ns": None,            # filled later if anchors are present
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

def _record_large_event(data, rank_id, local_time, msg_id, mtype, comm,
                        s1, r1, c1, b1, t1, s2, r2, c2, b2, t2):
    call_name = MESSAGE_TYPES.get(mtype, "UNKNOWN_{}".format(mtype))
    _ensure_stats_entry(data["statistics"], call_name)

    parts = [
        (1, s1, r1, c1, b1, t1),
        (2, s2, r2, c2, b2, t2),
    ]

    for part_index, sender, receiver, count, bytes_vol, tag in parts:
        is_empty_placeholder = (count <= 0 and bytes_vol <= 0)
        data["timeline"].append({
            "local_time": local_time,
            "time": local_time,       # overwritten later if anchors are present
            "epoch_ns": None,         # filled later if anchors are present
            "event_id": msg_id,
            "rank_recording": rank_id,
            "call": call_name,
            "comm": comm,
            "tag": tag,
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
# Header parsing
# -----------------------------------------------------------------------------

def _make_topology_entry(rank, pid, core, chip, hostname, hw_lookup, anchor=None):
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

    if anchor is not None:
        topo_entry["mpi_time_zero"] = anchor["mpi_time_zero"]
        topo_entry["unix_time_zero_ns"] = anchor["unix_time_zero_ns"]

    return topo_entry

def _parse_v2_header(raw_bytes, hw_lookup):
    offset = 0

    magic, offset = _read_exact(raw_bytes, offset, 8, "v2 magic")
    if magic != MPIC_V2_MAGIC:
        raise ValueError("Not a MPICv002 file")

    chunk, offset = _read_exact(raw_bytes, offset, 4, "v2 format version")
    version = struct.unpack("=I", chunk)[0]
    if version != MPIC_V2_VERSION:
        raise ValueError("Unsupported MPIC format version {}".format(version))

    chunk, offset = _read_exact(raw_bytes, offset, 4, "world size")
    total_ranks = struct.unpack("=i", chunk)[0]
    if total_ranks < 0:
        raise ValueError("Invalid world size {}".format(total_ranks))

    chunk, offset = _read_exact(raw_bytes, offset, 64, "date")
    run_date = _cstr(chunk)

    chunk, offset = _read_exact(raw_bytes, offset, 1024, "program")
    prog_name = _cstr(chunk)

    processes = []
    topology = []
    anchors = [None] * total_ranks

    for idx in range(total_ranks):
        chunk, offset = _read_exact(raw_bytes, offset, PROCESS_INFO_SIZE, "process info {}".format(idx))
        rank, pid, core, chip, hostname_b = struct.unpack(PROCESS_INFO_FMT, chunk)
        hostname = _cstr(hostname_b)
        processes.append({
            "rank": rank,
            "pid": pid,
            "core": core,
            "chip": chip,
            "hostname": hostname,
        })

    for idx in range(total_ranks):
        chunk, offset = _read_exact(raw_bytes, offset, PROCESS_TIME_ANCHOR_SIZE, "time anchor {}".format(idx))
        rank, mpi_time_zero, unix_time_zero_ns = struct.unpack(PROCESS_TIME_ANCHOR_FMT, chunk)

        if 0 <= rank < total_ranks:
            anchors[rank] = {
                "rank": rank,
                "mpi_time_zero": mpi_time_zero,
                "unix_time_zero_ns": unix_time_zero_ns,
            }
        else:
            # fallback to index if rank is somehow invalid
            anchors[idx] = {
                "rank": rank,
                "mpi_time_zero": mpi_time_zero,
                "unix_time_zero_ns": unix_time_zero_ns,
            }

    for proc in processes:
        rank = proc["rank"]
        anchor = anchors[rank] if 0 <= rank < total_ranks else None
        topology.append(_make_topology_entry(
            proc["rank"], proc["pid"], proc["core"], proc["chip"],
            proc["hostname"], hw_lookup, anchor
        ))

    metadata = {
        "total_ranks": total_ranks,
        "date": run_date,
        "program": prog_name if prog_name else "unknown",
        "system_name": "Unknown Cluster",
        "file_format": "MPICv002",
        "file_format_version": version,
        "time_registration": "per-rank-unix-anchor",
    }

    return metadata, topology, anchors, offset

def _parse_v1_header(raw_bytes, hw_lookup):
    offset = 0
    global_header_fmt = "=i64s1024s"
    global_header_size = struct.calcsize(global_header_fmt)

    chunk, offset = _read_exact(raw_bytes, offset, global_header_size, "legacy global header")
    total_ranks, raw_date, raw_prog = struct.unpack(global_header_fmt, chunk)

    if total_ranks < 0:
        raise ValueError("Invalid legacy world size {}".format(total_ranks))

    run_date = _cstr(raw_date)
    prog_name = _cstr(raw_prog)

    topology = []

    for idx in range(total_ranks):
        chunk, offset = _read_exact(raw_bytes, offset, PROCESS_INFO_SIZE, "legacy process info {}".format(idx))
        rank, pid, core, chip, hostname_b = struct.unpack(PROCESS_INFO_FMT, chunk)
        hostname = _cstr(hostname_b)

        topology.append(_make_topology_entry(rank, pid, core, chip, hostname, hw_lookup, None))

    metadata = {
        "total_ranks": total_ranks,
        "date": run_date,
        "program": prog_name if prog_name else "unknown",
        "system_name": "Unknown Cluster",
        "file_format": "legacy-v1",
        "file_format_version": 1,
        "time_registration": "legacy-local-time",
    }

    anchors = [None] * total_ranks
    return metadata, topology, anchors, offset

def _parse_mpic_header(raw_bytes, hw_lookup):
    if len(raw_bytes) >= 8 and raw_bytes[:8] == MPIC_V2_MAGIC:
        return _parse_v2_header(raw_bytes, hw_lookup)
    return _parse_v1_header(raw_bytes, hw_lookup)

# -----------------------------------------------------------------------------
# Time registration
# -----------------------------------------------------------------------------

def _apply_time_registration(data):
    anchors = data.get("time_anchors", [])
    timeline = data["timeline"]

    valid_anchor_epochs = []
    for anchor in anchors:
        if anchor is not None and anchor.get("unix_time_zero_ns", 0) > 0:
            valid_anchor_epochs.append(anchor["unix_time_zero_ns"])

    if not valid_anchor_epochs:
        for event in timeline:
            event["time"] = event.get("local_time", event.get("time", 0.0))
            event["epoch_ns"] = None
        data["metadata"]["timeline_clock"] = "local-relative"
        data["metadata"]["timeline_origin"] = "as-recorded"
        return

    global_origin_ns = min(valid_anchor_epochs)

    for event in timeline:
        rank = event["rank_recording"]
        local_time = event.get("local_time", 0.0)

        if 0 <= rank < len(anchors) and anchors[rank] is not None and anchors[rank].get("unix_time_zero_ns", 0) > 0:
            epoch_ns = anchors[rank]["unix_time_zero_ns"] + int(round(local_time * 1.0e9))
            event["epoch_ns"] = epoch_ns
            event["time"] = (epoch_ns - global_origin_ns) / 1.0e9
        else:
            event["epoch_ns"] = None
            event["time"] = local_time

    data["metadata"]["timeline_clock"] = "global-epoch-registered"
    data["metadata"]["timeline_origin_unix_ns"] = global_origin_ns
    data["metadata"]["timeline_origin"] = "minimum-rank-anchor"

# -----------------------------------------------------------------------------
# Human-readable summaries
# -----------------------------------------------------------------------------

def print_summary_table(stats, total_ranks):
    print("\n" + "=" * 115) # Increased width
    print(f" MPI COMMUNICATION SUMMARY ({total_ranks} Ranks)")
    print("=" * 115)

    if not stats:
        print(" No communication events found.")
        print("=" * 115 + "\n")
        return

    ordered_calls = sorted(stats.keys(), key=lambda name: MESSAGE_TYPE_ORDER.get(name, 9999))

    # Add the Per-Rank Avg column to the header
    header = " {:<13} | ".format("MPI Call") + " | ".join("{:<10}".format(b) for b in BINS) + " | {:<8} | {:<12}".format("Total", "Per-Rank Avg")
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
            
        # Calculate the average
        avg = total / total_ranks if total_ranks > 0 else 0
        
        row += "{:<8} | {:<12.1f}".format(total, avg)
        print(row)

    print("=" * 115 + "\n")

def print_analysis_summary(analysis, total_ranks):
    print("\n" + "=" * 95)
    print(f" TRACE ANALYSIS SUMMARY ({total_ranks} Ranks)")
    print("=" * 95)

    summary = analysis.get("summary", {})
    
    tot_events = summary.get('total_events', 0)
    can_events = summary.get('canonical_transfer_events', 0)
    comp_events = summary.get('completion_events', 0)
    bar_events = summary.get('barrier_events', 0)
    
    # Calculate Averages safely
    avg_events = tot_events / total_ranks if total_ranks > 0 else 0
    avg_can = can_events / total_ranks if total_ranks > 0 else 0
    avg_comp = comp_events / total_ranks if total_ranks > 0 else 0
    avg_bar = bar_events / total_ranks if total_ranks > 0 else 0

    print(" Total events:              {:<15} (Avg per rank: {:.1f})".format(tot_events, avg_events))
    print(" Canonical transfers:       {:<15} (Avg per rank: {:.1f})".format(can_events, avg_can))
    print(" Canonical transfer bytes:  {:<15}".format(summary.get("canonical_transfer_bytes", 0)))
    print(" Completion events:         {:<15} (Avg per rank: {:.1f})".format(comp_events, avg_comp))
    print(" Barrier events:            {:<15} (Avg per rank: {:.1f})".format(bar_events, avg_bar))
    print(" Estimated runtime:         {:.6f}s".format(summary.get("estimated_runtime", 0.0)))

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
# Section parsing
# -----------------------------------------------------------------------------

def _parse_sections_strict(raw_data, total_ranks, data):
    offset = 0

    for section_index in range(total_ranks):
        chunk, offset = _read_exact(raw_data, offset, 4, "rank id")
        rank_id = struct.unpack("=i", chunk)[0]

        chunk, offset = _read_exact(raw_data, offset, 24, "small section label")
        small_label = _cstr(chunk)
        if small_label != SMALL_LABEL_TEXT:
            raise ValueError("Bad small-section label for rank {}: {!r}".format(rank_id, small_label))

        chunk, offset = _read_exact(raw_data, offset, 4, "small section count")
        num_small = struct.unpack("=i", chunk)[0]
        if num_small < 0:
            raise ValueError("Negative small record count {} for rank {}".format(num_small, rank_id))

        for _ in range(num_small):
            chunk, offset = _read_exact(raw_data, offset, P2P_SMALL_SIZE, "small record")
            unpacked = struct.unpack(P2P_SMALL_FMT, chunk)
            _record_small_event(data, rank_id, *unpacked)

        chunk, offset = _read_exact(raw_data, offset, 24, "large section label")
        large_label = _cstr(chunk)
        if large_label != LARGE_LABEL_TEXT:
            raise ValueError("Bad large-section label for rank {}: {!r}".format(rank_id, large_label))

        chunk, offset = _read_exact(raw_data, offset, 4, "large section count")
        num_large = struct.unpack("=i", chunk)[0]
        if num_large < 0:
            raise ValueError("Negative large record count {} for rank {}".format(num_large, rank_id))

        for _ in range(num_large):
            chunk, offset = _read_exact(raw_data, offset, P2P_LARGE_SIZE, "large record")
            unpacked = struct.unpack(P2P_LARGE_FMT, chunk)
            _record_large_event(data, rank_id, *unpacked)

        print_progress(section_index + 1, total_ranks, prefix='Parsing Ranks:', suffix='Complete', length=40)

    # Allow trailing zero padding only
    if offset < len(raw_data):
        trailing = raw_data[offset:]
        if any(b != 0 for b in trailing):
            raise ValueError("Unexpected non-zero trailing bytes after strict parse")

def _parse_sections_salvage(raw_data, total_len, data):
    offset = 0
    parsed_sections = 0
    seen_ranks = set()

    print("Running salvage parser...")

    while offset + 32 <= total_len:
        rank_bytes = raw_data[offset:offset + 4]
        label_bytes = raw_data[offset + 4:offset + 28]

        if len(rank_bytes) < 4 or len(label_bytes) < 24:
            break

        rank_id = struct.unpack("=i", rank_bytes)[0]
        small_label = _cstr(label_bytes)

        if small_label != SMALL_LABEL_TEXT:
            offset += 1
            continue

        try:
            num_small = struct.unpack("=i", raw_data[offset + 28:offset + 32])[0]
        except struct.error:
            offset += 1
            continue

        if num_small < 0:
            offset += 1
            continue

        small_bytes_end = offset + 32 + num_small * P2P_SMALL_SIZE
        if small_bytes_end + 28 > total_len:
            offset += 1
            continue

        large_label = _cstr(raw_data[small_bytes_end:small_bytes_end + 24])
        if large_label != LARGE_LABEL_TEXT:
            offset += 1
            continue

        try:
            num_large = struct.unpack("=i", raw_data[small_bytes_end + 24:small_bytes_end + 28])[0]
        except struct.error:
            offset += 1
            continue

        if num_large < 0:
            offset += 1
            continue

        section_end = small_bytes_end + 28 + num_large * P2P_LARGE_SIZE
        if section_end > total_len:
            offset += 1
            continue

        # Avoid duplicate salvage of the same rank section
        if rank_id in seen_ranks:
            offset += 1
            continue

        # Parse the section
        local_offset = offset + 32

        for _ in range(num_small):
            chunk = raw_data[local_offset:local_offset + P2P_SMALL_SIZE]
            unpacked = struct.unpack(P2P_SMALL_FMT, chunk)
            _record_small_event(data, rank_id, *unpacked)
            local_offset += P2P_SMALL_SIZE

        local_offset += 24  # large label
        local_offset += 4   # large count

        large_start = small_bytes_end + 28
        local_offset = large_start

        for _ in range(num_large):
            chunk = raw_data[local_offset:local_offset + P2P_LARGE_SIZE]
            unpacked = struct.unpack(P2P_LARGE_FMT, chunk)
            _record_large_event(data, rank_id, *unpacked)
            local_offset += P2P_LARGE_SIZE

        seen_ranks.add(rank_id)
        parsed_sections += 1
        offset = section_end
        print_progress(offset, total_len, prefix='Salvaging:   ', suffix='Complete', length=40)

    print_progress(total_len, total_len, prefix='Salvaging:   ', suffix='Complete', length=40)

    if parsed_sections == 0:
        raise ValueError("Salvage parser found no valid rank sections")

    if parsed_sections < data["metadata"].get("total_ranks", 0):
        print("Warning: salvage parser recovered {} / {} rank sections".format(
            parsed_sections, data["metadata"].get("total_ranks", 0)
        ), file=sys.stderr)

# -----------------------------------------------------------------------------
# Analysis helpers
# -----------------------------------------------------------------------------

def _is_real_payload_event(event):
    return (event.get("bytes", 0) > 0) and (not event.get("synthetic_empty", False))

def _is_canonical_transfer_event(event):
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

    windows = [{
        "t_start": start_t + (runtime * i) / float(window_count),
        "t_end": start_t + (runtime * (i + 1)) / float(window_count),
        "events": 0,
        "canonical_transfer_events": 0,
        "canonical_transfer_bytes": 0,
        "completion_events": 0,
        "barrier_events": 0,
        "collective_events": 0
    } for i in range(window_count)]

    for event in timeline:
        if runtime <= 0.0:
            idx = 0
        else:
            idx = min(window_count - 1, max(0, int(((event["time"] - start_t) / runtime) * window_count)))

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
    timeline = data["timeline"]
    total_ranks = data["metadata"].get("total_ranks", 0)

    per_rank = {
        r: {
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
        } for r in range(total_ranks)
    }

    pair_stats = defaultdict(lambda: {
        "messages": 0,
        "bytes": 0,
        "calls": Counter(),
        "first_time": None,
        "last_time": None,
        "comm": None,
        "sender": None,
        "receiver": None,
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

        if idx % 10000 == 0:
            print_progress(idx, total_events, prefix='Pass 1: ', suffix='Complete', length=40, fill='#')
    print_progress(total_events, total_events, prefix='Pass 1: ', suffix='Complete', length=40, fill='#')

    # =========================================================================
    # Pass 2: Canonical Pair/Link Traffic
    # =========================================================================
    print("Analyzing Trace - Pass 2 (Canonical Traffic)...")
    for idx, event in enumerate(sorted_timeline):
        if not _is_canonical_transfer_event(event):
            continue

        call = event["call"]
        sender = event["sender"]
        receiver = event["receiver"]
        num_bytes = event.get("bytes", 0)
        t = event.get("time", 0.0)
        comm = event.get("comm", 0)

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

        pair_key = (comm, sender, receiver)
        pair_stats[pair_key]["messages"] += 1
        pair_stats[pair_key]["bytes"] += num_bytes
        pair_stats[pair_key]["calls"][call] += 1
        pair_stats[pair_key]["comm"] = comm
        pair_stats[pair_key]["sender"] = sender
        pair_stats[pair_key]["receiver"] = receiver

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

        if idx % 10000 == 0:
            print_progress(idx, total_events, prefix='Pass 2: ', suffix='Complete', length=40, fill='#')
    print_progress(total_events, total_events, prefix='Pass 2: ', suffix='Complete', length=40, fill='#')

    # =========================================================================
    # Pass 3: Timing Heuristics
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
        comm = event.get("comm", 0)
        tag = event.get("tag", 0)

        # P2P pairing
        if call in SEND_LIKE_CALLS:
            p2p_sends[(comm, event["sender"], event["receiver"], tag)].append(t)
        elif call in RECV_LIKE_CALLS:
            p2p_recvs[(comm, event["sender"], event["receiver"], tag)].append(t)
        elif call == "MPI_SENDRECV":
            if event.get("category") == "sendrecv_part_1":
                p2p_sends[(comm, event["sender"], event["receiver"], tag)].append(t)
            elif event.get("category") == "sendrecv_part_2":
                p2p_recvs[(comm, event["sender"], event["receiver"], tag)].append(t)

        # Collective pairing
        elif call == "MPI_BCAST":
            root = event["sender"]
            local = event["receiver"]
            seq_key = (comm, local)
            seq = rank_bcast_seq[seq_key]
            bcast_seq_map[(comm, root, seq)].append((local, t))
            rank_bcast_seq[seq_key] += 1

        elif call == "MPI_REDUCE":
            root = event["receiver"]
            local = event["sender"]
            seq_key = (comm, local)
            seq = rank_reduce_seq[seq_key]
            reduce_seq_map[(comm, root, seq)].append((local, t))
            rank_reduce_seq[seq_key] += 1

        if idx % 10000 == 0:
            print_progress(idx, total_events, prefix='Pass 3: ', suffix='Complete', length=40, fill='#')
    print_progress(total_events, total_events, prefix='Pass 3: ', suffix='Complete', length=40, fill='#')

    TIMING_THRESHOLD = 0.001  # 1ms

    late_sender_count = 0
    late_sender_time = 0.0
    late_receiver_count = 0
    late_receiver_time = 0.0

    for pair, s_times in p2p_sends.items():
        r_times = p2p_recvs.get(pair, [])
        for ts, tr in zip(s_times, r_times):
            diff = tr - ts
            if diff < -TIMING_THRESHOLD:
                late_sender_count += 1
                late_sender_time += (-diff)
            elif diff > TIMING_THRESHOLD:
                late_receiver_count += 1
                late_receiver_time += diff

    late_broadcaster_count = 0
    late_broadcaster_time = 0.0
    for (_comm, root, seq), events in bcast_seq_map.items():
        root_time = next((t for r, t in events if r == root), None)
        if root_time is not None:
            for r, t in events:
                if r != root:
                    diff = t - root_time
                    if diff < -TIMING_THRESHOLD:
                        late_broadcaster_count += 1
                        late_broadcaster_time += (-diff)

    early_reduce_count = 0
    early_reduce_time = 0.0
    for (_comm, root, seq), events in reduce_seq_map.items():
        root_time = next((t for r, t in events if r == root), None)
        if root_time is not None:
            for r, t in events:
                if r != root:
                    diff = t - root_time
                    if diff < -TIMING_THRESHOLD:
                        early_reduce_count += 1
                        early_reduce_time += (-diff)

    # =========================================================================
    # Aggregation
    # =========================================================================

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

    top_links = []
    for (_comm, _sender, _receiver), stats in sorted(pair_stats.items(), key=lambda kv: kv[1]["bytes"], reverse=True)[:20]:
        top_links.append({
            "comm": stats["comm"],
            "sender": stats["sender"],
            "receiver": stats["receiver"],
            "messages": stats["messages"],
            "bytes": stats["bytes"],
            "calls": dict(sorted(stats["calls"].items())),
            "first_time": stats["first_time"],
            "last_time": stats["last_time"],
        })

    top_ranks_by_out = [{
        "rank": r["rank"],
        "bytes": r["canonical_bytes_out"],
        "messages": r["canonical_messages_out"]
    } for r in sorted(per_rank_list, key=lambda x: x["canonical_bytes_out"], reverse=True)[:10]]

    top_ranks_by_in = [{
        "rank": r["rank"],
        "bytes": r["canonical_bytes_in"],
        "messages": r["canonical_messages_in"]
    } for r in sorted(per_rank_list, key=lambda x: x["canonical_bytes_in"], reverse=True)[:10]]

    top_ranks_by_touch = [{
        "rank": r["rank"],
        "bytes": r["touch_bytes"]
    } for r in sorted(per_rank_list, key=lambda x: x["touch_bytes"], reverse=True)[:10]]

    rooted_collective_summary = []
    for root, stats in sorted(rooted_collective_roots.items(), key=lambda kv: kv[1]["bytes"], reverse=True):
        rooted_collective_summary.append({
            "root": root,
            "events": stats["events"],
            "bytes": stats["bytes"],
            "calls": dict(sorted(stats["calls"].items())),
        })

    barrier_spreads = []
    if total_ranks > 0:
        barrier_counts = [len(barrier_times[r]) for r in range(total_ranks)]
        common_barriers = min(barrier_counts) if barrier_counts else 0
        for idx in range(common_barriers):
            times = [barrier_times[r][idx] for r in range(total_ranks)]
            barrier_spreads.append({
                "barrier_index": idx,
                "t_min": min(times),
                "t_max": max(times),
                "spread": max(times) - min(times),
            })
    else:
        common_barriers = 0

    max_barrier_spread = max((b["spread"] for b in barrier_spreads), default=0.0)
    avg_barrier_spread = _mean([b["spread"] for b in barrier_spreads]) if barrier_spreads else 0.0

    avg_out_peers = _mean(out_peer_counts)
    avg_in_peers = _mean(in_peer_counts)
    pair_density = _safe_div(len(pair_stats), total_ranks * max(0, total_ranks - 1))
    touch_cv = _cv(total_touch_bytes)
    top_link_share = _safe_div(top_links[0]["bytes"], canonical_total_bytes) if top_links and canonical_total_bytes > 0 else 0.0

    small128_total = sum(small128_by_call.values())
    small1k_total = sum(small1k_by_call.values())
    small1k_ratio = _safe_div(small1k_total, canonical_total_events)

    rooted_total_bytes = sum(r["bytes"] for r in rooted_collective_summary)
    top_root_share = _safe_div(rooted_collective_summary[0]["bytes"], rooted_total_bytes) if rooted_collective_summary and rooted_total_bytes > 0 else 0.0

    completion_to_transfer_ratio = _safe_div(completion_total_events, canonical_total_events)

    # =========================================================================
    # Patterns
    # =========================================================================

    patterns = []

    if pair_density >= 0.5 and total_ranks >= 8:
        patterns.append({
            "type": "dense-communication",
            "description": "Communication graph is dense (pair density {:.3f}), suggesting all-to-all or highly connected traffic.".format(pair_density),
            "metrics": {
                "pair_density": pair_density,
                "pairs_observed": len(pair_stats),
            }
        })
    elif pair_density > 0.0 and pair_density <= 0.1 and total_ranks >= 8:
        patterns.append({
            "type": "sparse-communication",
            "description": "Communication graph is sparse (pair density {:.3f}), suggesting nearest-neighbour or structured exchange.".format(pair_density),
            "metrics": {
                "pair_density": pair_density,
                "pairs_observed": len(pair_stats),
            }
        })

    if small1k_ratio >= 0.5 and canonical_total_events > 0:
        patterns.append({
            "type": "small-message-dominated",
            "description": "A large fraction of canonical transfers are under 1KB ({:.1%}).".format(small1k_ratio),
            "metrics": {
                "small_lt_1kb_ratio": small1k_ratio,
                "small_lt_1kb_events": small1k_total,
            }
        })

    if rooted_collective_summary and top_root_share >= 0.5:
        patterns.append({
            "type": "root-concentrated-collectives",
            "description": "One rank dominates rooted collective traffic ({:.1%} of rooted collective bytes).".format(top_root_share),
            "metrics": {
                "top_root": rooted_collective_summary[0]["root"],
                "top_root_share": top_root_share,
            }
        })

    if top_link_share >= 0.2 and canonical_total_bytes > 0:
        patterns.append({
            "type": "dominant-link",
            "description": "A single communication link carries {:.1%} of canonical traffic bytes.".format(top_link_share),
            "metrics": {
                "top_link_share": top_link_share,
                "top_link": top_links[0] if top_links else None,
            }
        })

    if touch_cv >= 1.0 and total_ranks > 1:
        patterns.append({
            "type": "rank-communication-imbalance",
            "description": "Communication volume per rank is imbalanced (CV {:.3f}).".format(touch_cv),
            "metrics": {
                "touch_bytes_cv": touch_cv,
            }
        })

    if completion_to_transfer_ratio >= 0.5:
        patterns.append({
            "type": "completion-heavy",
            "description": "Completion calls are frequent relative to canonical transfers (ratio {:.3f}).".format(completion_to_transfer_ratio),
            "metrics": {
                "completion_to_transfer_ratio": completion_to_transfer_ratio,
            }
        })

    if barrier_total_events > 0 and _safe_div(barrier_total_events, total_events) >= 0.05:
        patterns.append({
            "type": "barrier-heavy",
            "description": "Barrier usage is prominent in the trace ({} barrier events).".format(barrier_total_events),
            "metrics": {
                "barrier_events": barrier_total_events,
                "barrier_fraction": _safe_div(barrier_total_events, total_events),
            }
        })

    # =========================================================================
    # Issues
    # =========================================================================

    issues = []

    if canonical_total_events > 0:
        ls_ratio = _safe_div(late_sender_count, canonical_total_events)
        if ls_ratio > 0.05:
            score = min(ls_ratio * 4.0, 1.0)
            issues.append({
                "type": "late_sender",
                "severity": _severity(score),
                "score": score,
                "description": "Detected {} instances of Late Sender. Receivers are posting before sends arrive (total wasted time {:.3f}s).".format(
                    late_sender_count, late_sender_time
                ),
                "metrics": {
                    "late_sender_count": late_sender_count,
                    "fraction_of_traffic": ls_ratio,
                    "wasted_time_sec": late_sender_time,
                }
            })

        lr_ratio = _safe_div(late_receiver_count, canonical_total_events)
        if lr_ratio > 0.05:
            score = min(lr_ratio * 4.0, 1.0)
            issues.append({
                "type": "late_receiver",
                "severity": _severity(score),
                "score": score,
                "description": "Detected {} instances of Late Receiver. Sends are posted before receives are ready (total delayed time {:.3f}s).".format(
                    late_receiver_count, late_receiver_time
                ),
                "metrics": {
                    "late_receiver_count": late_receiver_count,
                    "fraction_of_traffic": lr_ratio,
                    "delayed_time_sec": late_receiver_time,
                }
            })

    if collective_total_events > 0:
        lb_ratio = _safe_div(late_broadcaster_count, collective_total_events)
        if lb_ratio > 0.05:
            score = min(lb_ratio * 4.0, 1.0)
            issues.append({
                "type": "late_broadcaster",
                "severity": _severity(score),
                "score": score,
                "description": "Detected {} cases where non-root ranks reach MPI_Bcast before the broadcaster is ready (total wasted time {:.3f}s).".format(
                    late_broadcaster_count, late_broadcaster_time
                ),
                "metrics": {
                    "late_broadcaster_count": late_broadcaster_count,
                    "fraction_of_collectives": lb_ratio,
                    "wasted_time_sec": late_broadcaster_time,
                }
            })

        er_ratio = _safe_div(early_reduce_count, collective_total_events)
        if er_ratio > 0.05:
            score = min(er_ratio * 4.0, 1.0)
            issues.append({
                "type": "early_reduce_arrival",
                "severity": _severity(score),
                "score": score,
                "description": "Detected {} cases where workers arrive at MPI_Reduce before the root is ready (total wasted time {:.3f}s).".format(
                    early_reduce_count, early_reduce_time
                ),
                "metrics": {
                    "early_reduce_count": early_reduce_count,
                    "fraction_of_collectives": er_ratio,
                    "wasted_time_sec": early_reduce_time,
                }
            })

    if small1k_ratio >= 0.6 and canonical_total_events > 0:
        score = min(small1k_ratio, 1.0)
        issues.append({
            "type": "small_message_pressure",
            "severity": _severity(score),
            "score": score,
            "description": "A high fraction of canonical traffic uses very small payloads (<1KB: {:.1%}), which may increase message-rate overhead.".format(
                small1k_ratio
            ),
            "metrics": {
                "small_lt_1kb_ratio": small1k_ratio,
                "small_lt_1kb_events": small1k_total,
            }
        })

    if touch_cv >= 1.2 and total_ranks > 1:
        score = min(touch_cv / 2.0, 1.0)
        issues.append({
            "type": "rank_traffic_imbalance",
            "severity": _severity(score),
            "score": score,
            "description": "Per-rank communication volume is strongly imbalanced (CV {:.3f}).".format(touch_cv),
            "metrics": {
                "touch_bytes_cv": touch_cv,
                "top_ranks_by_touch": top_ranks_by_touch[:5],
            }
        })

    if top_root_share >= 0.7 and rooted_total_bytes > 0:
        score = min(top_root_share, 1.0)
        issues.append({
            "type": "collective_root_bottleneck",
            "severity": _severity(score),
            "score": score,
            "description": "One root rank accounts for {:.1%} of rooted collective bytes, which may indicate a root bottleneck.".format(
                top_root_share
            ),
            "metrics": {
                "top_root": rooted_collective_summary[0]["root"] if rooted_collective_summary else None,
                "top_root_share": top_root_share,
            }
        })

    if barrier_spreads and runtime > 0.0:
        rel_barrier_spread = _safe_div(max_barrier_spread, runtime)
        if max_barrier_spread >= 0.001 and rel_barrier_spread >= 0.01:
            score = min(rel_barrier_spread * 5.0, 1.0)
            issues.append({
                "type": "barrier_arrival_skew",
                "severity": _severity(score),
                "score": score,
                "description": "Barriers exhibit noticeable arrival skew (max spread {:.6f}s, average {:.6f}s).".format(
                    max_barrier_spread, avg_barrier_spread
                ),
                "metrics": {
                    "max_barrier_spread": max_barrier_spread,
                    "avg_barrier_spread": avg_barrier_spread,
                    "barrier_instances": len(barrier_spreads),
                }
            })

    if completion_to_transfer_ratio >= 1.0 and completion_total_events > 0:
        score = min(completion_to_transfer_ratio / 2.0, 1.0)
        issues.append({
            "type": "completion_overhead",
            "severity": _severity(score),
            "score": score,
            "description": "Completion calls are very frequent relative to canonical transfers (ratio {:.3f}).".format(
                completion_to_transfer_ratio
            ),
            "metrics": {
                "completion_events": completion_total_events,
                "completion_to_transfer_ratio": completion_to_transfer_ratio,
            }
        })

    if top_link_share >= 0.35 and canonical_total_bytes > 0:
        score = min(top_link_share * 2.0, 1.0)
        issues.append({
            "type": "link_hotspot",
            "severity": _severity(score),
            "score": score,
            "description": "A single sender/receiver pair carries a large share of bytes ({:.1%}).".format(top_link_share),
            "metrics": {
                "top_link_share": top_link_share,
                "top_link": top_links[0] if top_links else None,
            }
        })

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

    data = {
        "metadata": {
            "total_ranks": 0,
            "date": "",
            "program": "unknown",
            "system_name": "Unknown Cluster",
        },
        "topology": [],
        "time_anchors": [],
        "timeline": [],
        "statistics": {},
        "hardware_blueprint": None,
    }

    hw_lookup = load_hardware_map(hw_filepath) if hw_filepath else {}

    with _open_maybe_gzip(mpic_filepath) as f:
        raw_file = f.read()

    try:
        metadata, topology, anchors, sections_offset = _parse_mpic_header(raw_file, hw_lookup)
    except Exception as exc:
        print("Error: failed to parse header: {}".format(exc), file=sys.stderr)
        sys.exit(1)

    data["metadata"].update(metadata)
    data["topology"] = topology
    data["time_anchors"] = anchors

    raw_sections = raw_file[sections_offset:]

    try:
        _parse_sections_strict(raw_sections, data["metadata"]["total_ranks"], data)
    except Exception as strict_err:
        print("Warning: strict parse failed ({}). Falling back to salvage parser.".format(strict_err), file=sys.stderr)
        data["timeline"] = []
        data["statistics"] = {}
        try:
            _parse_sections_salvage(raw_sections, len(raw_sections), data)
        except Exception as salvage_err:
            print("Error: salvage parser failed: {}".format(salvage_err), file=sys.stderr)
            sys.exit(1)

    _apply_time_registration(data)

    print("Sorting the timeline...")
    data["timeline"].sort(
        key=lambda x: (
            x["epoch_ns"] if x.get("epoch_ns") is not None else float("inf"),
            x["time"],
            x["event_id"],
            x["rank_recording"],
        )
    )

    print("Reading in the hardware blueprint...")
    if hw_filepath and os.path.exists(hw_filepath):
        with open(hw_filepath, "r") as f:
            blueprint = json.load(f)
            data["hardware_blueprint"] = blueprint
            if "metadata" in blueprint and "system_name" in blueprint["metadata"]:
                data["metadata"]["system_name"] = blueprint["metadata"]["system_name"]

    data["analysis"] = analyse_trace(data)

    CHUNK_SIZE = 500000
    chunks_index = []
    compressed_payloads = []
    current_byte_offset = 0

    total_events = len(data["timeline"])
    total_chunks = (total_events + CHUNK_SIZE - 1) // CHUNK_SIZE if total_events > 0 else 0

    print("Compressing {} chunks...".format(total_chunks))
    print_progress(0, max(total_chunks, 1), prefix='Compressing:  ', suffix='Complete', length=40)

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

        print_progress(idx + 1, max(total_chunks, 1), prefix='Compressing:  ', suffix='Complete', length=40)

    header_data = {
        "metadata": data["metadata"],
        "topology": data["topology"],
        "time_anchors": data["time_anchors"],
        "statistics": data["statistics"],
        "hardware_blueprint": data["hardware_blueprint"],
        "analysis": data["analysis"],
        "chunks": chunks_index,
    }

    header_json = json.dumps(header_data, separators=(",", ":")).encode("utf-8")
    compressed_header = zlib.compress(header_json)

    output_filename = mpic_filepath
    if output_filename.endswith(".mpic.gz"):
        output_filename = output_filename[:-8] + ".mpix"
    elif output_filename.endswith(".mpic"):
        output_filename = output_filename[:-5] + ".mpix"
    else:
        output_filename = output_filename + ".mpix"

    with open(output_filename, "wb") as f:
        f.write(struct.pack("<I", len(compressed_header)))
        f.write(compressed_header)
        for payload in compressed_payloads:
            f.write(payload)

    print("Packed {} chunks into a single {} container.".format(len(chunks_index), output_filename))
    print("Parsed {} communication events.".format(len(data["timeline"])))
    print("Data saved to {}".format(output_filename))

    total_ranks = data["metadata"].get("total_ranks", 0)
    print_summary_table(data["statistics"], total_ranks)
    print_analysis_summary(data["analysis"], total_ranks)

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parse_mpic.py <filename.mpic|filename.mpic.gz> [hardware_map.json]")
        sys.exit(1)

    hw_file = sys.argv[2] if len(sys.argv) > 2 else None
    parse_mpic_file(sys.argv[1], hw_file)

