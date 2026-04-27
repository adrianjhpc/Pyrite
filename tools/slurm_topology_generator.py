import re
import json
import argparse
import sys
import subprocess

def expand_slurm_nodes(node_str):
    """
    Expands Slurm's bracketed node notation into a list of strings.
    Example: 'cs-n[0000-0015]' -> ['cs-n0000', 'cs-n0001', ..., 'cs-n0015']
    """
    nodes = []
    # Match patterns like prefix[00-05,08]
    match = re.search(r'([^[\]]+)\[([^[\]]+)\]', node_str)
    
    if match:
        prefix = match.group(1)
        ranges = match.group(2)
        
        # Slurm ranges are comma-separated (e.g., '01-05,08,10-15')
        for r in ranges.split(','):
            if '-' in r:
                start_str, end_str = r.split('-')
                width = len(start_str) # Detect the zero-padding width
                for i in range(int(start_str), int(end_str) + 1):
                    # Re-apply the zero padding and append
                    nodes.append(f"{prefix}{str(i).zfill(width)}")
            else:
                nodes.append(f"{prefix}{r}")
    else:
        # If there are no brackets, it's just a single node or comma-separated list
        nodes.extend(node_str.split(','))
        
    return nodes

def get_slurm_cluster_name():
    """Attempts to query Slurm locally for the cluster name."""
    try:
        # Run 'scontrol show config' and capture the output
        result = subprocess.run(['scontrol', 'show', 'config'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
        for line in result.stdout.split('\n'):
            if 'ClusterName' in line:
                # Output looks like: "ClusterName             = my_cluster"
                return line.split('=')[1].strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Command failed or scontrol is not installed on this machine
        pass
    
    return "Unknown Slurm Cluster"

def parse_topo_file(filepath, racks_per_cabinet, system_name):
    """Parses a Slurm topo file and builds the hardware JSON map."""
    switches = []
    
    try:
        with open(filepath, 'r') as f:
            for line in f:
                # We only care about Level 0 (Leaf) switches for node placement
                if 'Level=0' in line:
                    # Extract the SwitchName and the Nodes string
                    name_match = re.search(r'SwitchName=(\S+)', line)
                    nodes_match = re.search(r'Nodes=(\S+)', line)
                    
                    if name_match and nodes_match:
                        switch_name = name_match.group(1)
                        node_str = nodes_match.group(1)
                        
                        switches.append({
                            "name": switch_name,
                            "nodes": expand_slurm_nodes(node_str)
                        })
    except FileNotFoundError:
        print(f"Error: Could not find {filepath}")
        sys.exit(1)

    return build_json_topology(switches, racks_per_cabinet, system_name)

def build_json_topology(switches, racks_per_cabinet, system_name):
    topology = {
        "metadata": {
            "system_name": system_name
        },
        "cabinets": []
    }
    
    # 3D Visualizer spacing settings
    cabinet_spacing_x = 75
    rack_spacing_x = 15
    
    cab_idx = 0
    rack_idx = 0
    current_cabinet = None

    for switch in switches:
        # If we need a new cabinet, initialize it
        if rack_idx % racks_per_cabinet == 0:
            if current_cabinet:
                topology["cabinets"].append(current_cabinet)
            
            current_cabinet = {
                "id": f"CAB-{cab_idx + 1:02d}",
                "x": cab_idx * cabinet_spacing_x,
                "z": 0,
                "racks": []
            }
            cab_idx += 1
            rack_idx = 0 # Reset rack counter for the new cabinet

        # Build the Rack based on this specific switch
        rack = {
            "id": f"RACK-{switch['name']}", # Name the rack after the switch
            "x_offset": rack_idx * rack_spacing_x,
            "z_offset": 0,
            "nodes": []
        }
        
        # Populate the slots
        for slot, hostname in enumerate(switch["nodes"]):
            rack["nodes"].append({
                "hostname": hostname,
                "slot": slot
            })
            
        current_cabinet["racks"].append(rack)
        rack_idx += 1

    if current_cabinet:
        topology["cabinets"].append(current_cabinet)

    return topology

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Slurm topo output to a JSON hardware map.")
    parser.add_argument("topo_file", type=str, help="Text file containing the output of 'scontrol show topo'")
    
    # System metadata
    parser.add_argument("--system_name", type=str, default=None, help="Global name of the cluster. If omitted, attempts to fetch automatically from Slurm.")
    
    parser.add_argument("--racks_per_cab", type=int, default=4, help="How many switches/racks to group visually into one cabinet")
    parser.add_argument("--out", type=str, default="hardware_map.json", help="Output filename")
    
    args = parser.parse_args()
    
    # Determine system name automatically if not provided
    sys_name = args.system_name
    if not sys_name:
        sys_name = get_slurm_cluster_name()
    
    final_topology = parse_topo_file(args.topo_file, args.racks_per_cab, sys_name)
    
    with open(args.out, 'w') as f:
        json.dump(final_topology, f, indent=2)
        
    print(f"Map generated for '{sys_name}' with {sum(len(c['racks']) for c in final_topology['cabinets'])} switches.")
    print(f"Saved to {args.out}")
