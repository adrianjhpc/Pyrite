import struct
import json
import sys
import os
import gzip
import zlib

# --- MPI Message Type Mapping ---
MESSAGE_TYPES = {
    13: "MPI_SEND",      14: "MPI_RECV",      15: "MPI_BSEND",
    16: "MPI_SSEND",     17: "MPI_RSEND",     18: "MPI_ISEND",
    19: "MPI_IBSEND",    20: "MPI_ISSEND",    21: "MPI_IRSEND",
    22: "MPI_IRECV",     23: "MPI_SENDRECV",  24: "MPI_WAIT",
    25: "MPI_WAITALL",   26: "MPI_BARRIER",   27: "MPI_BCAST",
    28: "MPI_REDUCE",    29: "MPI_ALLREDUCE", 30: "MPI_GATHER",
    31: "MPI_SCATTER",   32: "MPI_ALLGATHER"
}

def load_hardware_map(filepath):
    """Flattens the hardware map into a quick lookup dictionary."""
    if not os.path.exists(filepath):
        return {}
    
    with open(filepath, 'r') as f:
        hw = json.load(f)
        
    lookup = {}
    for cab in hw.get("cabinets", []):
        for rack in cab.get("racks", []):
            # Check if the Rack uses blades
            if "blades" in rack:
                for blade in rack["blades"]:
                    for node in blade.get("nodes", []):
                        lookup[node["hostname"]] = {
                            "cab_id": cab["id"],
                            "rack_id": rack["id"],
                            "blade_id": blade["id"],
                            # Nodes sit side-by-side (X), Blades stack vertically (Y)
                            "x": cab["x"] + rack.get("x_offset", 0) + node.get("x_offset", node.get("slot", 0) * 12),
                            "y": blade.get("y_offset", 0), 
                            "z": cab["z"] + rack.get("z_offset", 0)
                        }
            else:
                # Support for node-only racks
                for node in rack.get("nodes", []):
                    lookup[node["hostname"]] = {
                        "cab_id": cab["id"],
                        "rack_id": rack["id"],
                        "x": cab["x"] + rack.get("x_offset", 0),
                        "y": node.get("slot", 0) * 15,
                        "z": cab["z"] + rack.get("z_offset", 0)
                    }
    return lookup

def print_summary_table(stats):
    """Prints a formatted ASCII table of the message statistics to the terminal."""
    print("\n" + "="*95)
    print(" MPI COMMUNICATION SUMMARY")
    print("="*95)
    
    if not stats:
        print(" No communication events found.")
        print("="*95 + "\n")
        return

    bins = ["< 128B", "128B - 1KB", "1KB - 64KB", "64KB - 1MB", "1MB - 16MB", "> 16MB"]
    
    header = f" {'MPI Call':<13} | " + " | ".join([f"{b:<10}" for b in bins]) + " | {Total}"
    print(header)
    print("-" * len(header))
    
    for call, bin_data in stats.items():
        short_call = call.replace("MPI_", "") 
        row_str = f" {short_call:<13} | "
        
        total = 0
        for b in bins:
            count = bin_data.get(b, 0)
            total += count
            row_str += f"{count:<10} | "
            
        row_str += f"{total:<8}"
        print(row_str)
        
    print("="*95 + "\n")

def parse_mpic_file(mpic_filepath, hw_filepath=None):
    if not os.path.exists(mpic_filepath):
        print(f"Error: File '{mpic_filepath}' not found.")
        sys.exit(1)

    process_info_fmt = '=i i i i 1024s'
    p2p_small_fmt = '=d i i i i i i'
    p2p_large_fmt = '=d i i i i i i i i i i'

    process_info_size = struct.calcsize(process_info_fmt)
    small_size = struct.calcsize(p2p_small_fmt)
    large_size = struct.calcsize(p2p_large_fmt)

    data = {
        # --- NEW: Added system_name default ---
        "metadata": {"total_ranks": 0, "date": "" , "program": "unknown", "system_name": "Unknown Cluster"},
        "topology": [],
        "timeline": [],
        "statistics": {}
    }

    hw_lookup = load_hardware_map(hw_filepath) if hw_filepath else {}
    
    # Template for single-pass stats binning
    bins_template = {
        "< 128B": 0, "128B - 1KB": 0, "1KB - 64KB": 0, 
        "64KB - 1MB": 0, "1MB - 16MB": 0, "> 16MB": 0
    }

    with open(mpic_filepath, 'rb') as f:
        # i = integer (4 bytes), 64s = string (64 bytes), 1024s = string (1024 bytes)
        global_header_fmt = '=i 64s 1024s' 
        global_header_size = struct.calcsize(global_header_fmt)
        
        header_bytes = f.read(global_header_size)
        if not header_bytes or len(header_bytes) < global_header_size:
            print("Error: Empty or corrupted file header.")
            sys.exit(1)
        
        total_ranks, raw_date, raw_prog = struct.unpack(global_header_fmt, header_bytes)
        
        # Clean up the C-strings (remove null bytes and decode)
        run_date = raw_date.decode('utf-8', errors='ignore').rstrip('\x00')
        prog_name = raw_prog.decode('utf-8', errors='ignore').rstrip('\x00')
        
        # Assign to our metadata dictionary
        data["metadata"]["total_ranks"] = total_ranks
        data["metadata"]["date"] = run_date
        data["metadata"]["program"] = prog_name

        # Read Process Information
        for _ in range(data["metadata"]["total_ranks"]):
            proc_bytes = f.read(process_info_size)
            rank, pid, core, chip, hostname_b = struct.unpack(process_info_fmt, proc_bytes)
            hostname = hostname_b.decode('utf-8', errors='ignore').rstrip('\x00')
            
            hw_info = hw_lookup.get(hostname, {"x": rank*15, "y": 0, "z": 0})
            
            data["topology"].append({
                "rank": rank, "pid": pid, "core": core, "chip": chip,
                "hostname": hostname, "x": hw_info["x"], "y": hw_info["y"], "z": hw_info["z"]
            })

         # Read the entire remainder of the file into memory
        raw_data = f.read()
        total_len = len(raw_data)
        offset = 0

        # --- Bulletproof Anchor-Based Parsing ---
        while offset < total_len:
            # 1. SAFE READ: Rank ID
            if offset + 4 > total_len: break
            rank_id = struct.unpack('=i', raw_data[offset:offset+4])[0]
            offset += 4

            # 2. SAFE READ: Small Message Header
            if offset + 24 > total_len: break
            small_str = raw_data[offset:offset+24]
            offset += 24
            
            # If we lost sync due to corruption, scan forward to find the next valid anchor
            if b"P2P Small Type Messages" not in small_str:
                next_idx = raw_data.find(b"P2P Small Type Messages", offset)
                if next_idx == -1: break
                offset = next_idx - 4 # Rewind to capture the Rank ID
                continue

            # Skip the untrustworthy num_small counter
            if offset + 4 > total_len: break
            offset += 4

            # 3. Find where the large messages begin to isolate the small message block
            next_large_idx = raw_data.find(b"P2P Large Type Messages", offset)
            if next_large_idx == -1:
                small_bytes_len = total_len - offset
                next_large_idx = total_len # Reached EOF
            else:
                small_bytes_len = next_large_idx - offset

            # Trim any corrupted/partial bytes at the end of the block
            valid_small_len = small_bytes_len - (small_bytes_len % small_size)
            small_buffer = raw_data[offset : offset + valid_small_len]
            
            for time_val, msg_id, mtype, sender, receiver, count, bytes_vol in struct.iter_unpack(p2p_small_fmt, small_buffer):
                call_name = MESSAGE_TYPES.get(mtype, f"UNKNOWN_{mtype}")
                data["timeline"].append({
                    "time": time_val, "event_id": msg_id, "rank_recording": rank_id,
                    "call": call_name, "sender": sender, "receiver": receiver,
                    "count": count, "bytes": bytes_vol, "category": "point-to-point"
                })
                
                if call_name not in data["statistics"]:
                    data["statistics"][call_name] = dict(bins_template)
                if bytes_vol < 128: data["statistics"][call_name]["< 128B"] += 1
                elif bytes_vol < 1024: data["statistics"][call_name]["128B - 1KB"] += 1
                elif bytes_vol < 65536: data["statistics"][call_name]["1KB - 64KB"] += 1
                elif bytes_vol < 1048576: data["statistics"][call_name]["64KB - 1MB"] += 1
                elif bytes_vol < 16777216: data["statistics"][call_name]["1MB - 16MB"] += 1
                else: data["statistics"][call_name]["> 16MB"] += 1

            # Jump offset exactly to the large messages anchor
            offset = next_large_idx

            # 4. SAFE READ: Large Message Header
            if offset + 24 > total_len: break
            offset += 24 # Skip "P2P Large Type Messages" string

            if offset + 4 > total_len: break
            offset += 4  # Skip the untrustworthy num_large counter

            # 5. The large messages end where the NEXT rank begins (denoted by the next Small anchor)
            next_small_idx = raw_data.find(b"P2P Small Type Messages", offset)
            if next_small_idx == -1:
                large_bytes_len = total_len - offset
                offset = total_len # EOF
            else:
                large_bytes_len = (next_small_idx - 4) - offset
                offset = next_small_idx - 4 # Set offset to the next Rank ID

            # Trim any corrupted/partial bytes at the end of the block
            valid_large_len = large_bytes_len - (large_bytes_len % large_size)
            large_buffer = raw_data[offset - large_bytes_len : offset - large_bytes_len + valid_large_len]
            
            for time_val, msg_id, mtype, s1, r1, c1, b1, s2, r2, c2, b2 in struct.iter_unpack(p2p_large_fmt, large_buffer):
                call_name = MESSAGE_TYPES.get(mtype, f"UNKNOWN_{mtype}")
                data["timeline"].extend([
                    {"time": time_val, "event_id": msg_id, "rank_recording": rank_id, "call": call_name, "sender": s1, "receiver": r1, "count": c1, "bytes": b1, "category": "collective_part_1"},
                    {"time": time_val, "event_id": msg_id, "rank_recording": rank_id, "call": call_name, "sender": s2, "receiver": r2, "count": c2, "bytes": b2, "category": "collective_part_2"}
                ])
                
                if call_name not in data["statistics"]:
                    data["statistics"][call_name] = dict(bins_template)
                for b_vol in (b1, b2):
                    if b_vol < 128: data["statistics"][call_name]["< 128B"] += 1
                    elif b_vol < 1024: data["statistics"][call_name]["128B - 1KB"] += 1
                    elif b_vol < 65536: data["statistics"][call_name]["1KB - 64KB"] += 1
                    elif b_vol < 1048576: data["statistics"][call_name]["64KB - 1MB"] += 1
                    elif b_vol < 16777216: data["statistics"][call_name]["1MB - 16MB"] += 1
                    else: data["statistics"][call_name]["> 16MB"] += 1

    # Sort all events chronologically
    data["timeline"].sort(key=lambda x: x["time"])

    # Attach the hardware blueprint
    if hw_filepath and os.path.exists(hw_filepath):
        with open(hw_filepath, 'r') as f:
            blueprint = json.load(f)
            data["hardware_blueprint"] = blueprint
            
            # Extract the system_name from the topology json
            if "metadata" in blueprint and "system_name" in blueprint["metadata"]:
                data["metadata"]["system_name"] = blueprint["metadata"]["system_name"]
    else:
        data["hardware_blueprint"] = None 

    # Prepare to chunk the timeline
    CHUNK_SIZE = 500000 # 500k events per chunk
    chunks_index = []
    compressed_payloads = []
    
    current_byte_offset = 0

    print("Compressing chunks...")
    for i in range(0, len(data["timeline"]), CHUNK_SIZE):
        chunk_data = data["timeline"][i : i + CHUNK_SIZE]
        
        # Compress this specific chunk
        chunk_json = json.dumps(chunk_data, separators=(',', ':')).encode('utf-8')
        compressed_chunk = zlib.compress(chunk_json)
        
        # Record where this chunk will live, and what time it covers
        chunks_index.append({
            "t_start": chunk_data[0]["time"],
            "t_end": chunk_data[-1]["time"],
            "offset": current_byte_offset,
            "size": len(compressed_chunk)
        })
        
        compressed_payloads.append(compressed_chunk)
        current_byte_offset += len(compressed_chunk)

    # Build the Header (Everything except the timeline)
    header_data = {
        "metadata": data["metadata"],
        "topology": data["topology"],
        "statistics": data["statistics"],
        "hardware_blueprint": data["hardware_blueprint"],
        "chunks": chunks_index 
    }

    header_json = json.dumps(header_data, separators=(',', ':')).encode('utf-8')
    compressed_header = zlib.compress(header_json)
    header_length = len(compressed_header)

    # Write the Custom Wrapper File
    output_filename = mpic_filepath.replace(".mpic", ".mpix")
    with open(output_filename, 'wb') as f:
        # Write exactly 4 bytes (an unsigned integer) telling the browser how big the header is
        f.write(struct.pack('<I', header_length)) 
        
        # Write the compressed header
        f.write(compressed_header)
        
        # Write the compressed chunks one by one
        for payload in compressed_payloads:
            f.write(payload)

    print(f"Packed {len(chunks_index)} chunks into a single {output_filename} container.")
    
    print(f"Parsed {len(data['timeline'])} communication events.")
    print(f"Data saved to {output_filename}")

    # Print the terminal summary table
    print_summary_table(data["statistics"])

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parse_mpic.py <filename.mpic> [hardware_map.json]")
    else:
        hw_file = sys.argv[2] if len(sys.argv) > 2 else None
        parse_mpic_file(sys.argv[1], hw_file)
