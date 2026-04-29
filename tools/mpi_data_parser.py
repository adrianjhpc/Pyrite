import struct
import json
import sys
import os
import gzip
import zlib

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


# -----------------------------------------------------------------------------
# Helpers
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
    """
    Open plain .mpic or gzip-compressed .mpic.gz transparently.
    """
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


def _categorise_small_call(call_name):
    if call_name in (
        "MPI_WAIT", "MPI_WAITALL", "MPI_WAITANY", "MPI_WAITSOME",
        "MPI_TEST", "MPI_TESTANY", "MPI_TESTALL", "MPI_TESTSOME"
    ):
        return "completion"
    if call_name == "MPI_BARRIER":
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

        # Do not count synthetic empty large parts in the communication summary.
        if not is_empty_placeholder:
            _update_stats(data["statistics"], call_name, bytes_vol)


def load_hardware_map(filepath):
    """
    Flattens the hardware map into a quick lookup dictionary.
    """
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


def print_summary_table(stats):
    """
    Prints a formatted ASCII table of the message statistics to the terminal.
    """
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


# -----------------------------------------------------------------------------
# Parsing backends
# -----------------------------------------------------------------------------

def _parse_sections_strict(raw_data, total_ranks, data, small_fmt, large_fmt):
    """
    Parse exactly using the record counts stored in the file.
    This is the preferred path for well-formed files.
    """
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

    # If anything remains, the file may contain junk/trailing data.
    trailing = raw_data[offset:]
    if trailing not in (b"",):
        raise ValueError("Strict parse finished with {} unexpected trailing bytes".format(len(trailing)))


def _parse_sections_salvage(raw_data, total_len, data, small_fmt, large_fmt):
    """
    Fallback parser that scans for known labels if the file is partially corrupt.
    """
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
        offset += 4  # Skip num_small

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
        offset += 24  # Skip large label

        if offset + 4 > total_len:
            break
        offset += 4  # Skip num_large

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

        # Read process information.
        for idx in range(total_ranks):
            proc_bytes = f.read(process_info_size)
            if len(proc_bytes) != process_info_size:
                print(
                    "Error: truncated process info block at rank index {}.".format(idx),
                    file=sys.stderr
                )
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

    # Prefer the strict parser, fall back to salvage mode if needed.
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

    # Chronological ordering for visualisation.
    data["timeline"].sort(key=lambda x: (x["time"], x["event_id"], x["rank_recording"]))

    # Attach hardware blueprint if provided.
    if hw_filepath and os.path.exists(hw_filepath):
        with open(hw_filepath, "r") as f:
            blueprint = json.load(f)
            data["hardware_blueprint"] = blueprint

            if "metadata" in blueprint and "system_name" in blueprint["metadata"]:
                data["metadata"]["system_name"] = blueprint["metadata"]["system_name"]
    else:
        data["hardware_blueprint"] = None

    # Chunk and compress the timeline.
    CHUNK_SIZE = 500000
    chunks_index = []
    compressed_payloads = []
    current_byte_offset = 0

    print("Compressing chunks...")
    for i in range(0, len(data["timeline"]), CHUNK_SIZE):
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

    header_data = {
        "metadata": data["metadata"],
        "topology": data["topology"],
        "statistics": data["statistics"],
        "hardware_blueprint": data["hardware_blueprint"],
        "chunks": chunks_index,
    }

    header_json = json.dumps(header_data, separators=(",", ":")).encode("utf-8")
    compressed_header = zlib.compress(header_json)
    header_length = len(compressed_header)

    # Safer output naming.
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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parse_mpic.py <filename.mpic|filename.mpic.gz> [hardware_map.json]")
        sys.exit(1)

    hw_file = sys.argv[2] if len(sys.argv) > 2 else None
    parse_mpic_file(sys.argv[1], hw_file)

