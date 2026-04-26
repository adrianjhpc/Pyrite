import json
import argparse

def generate_topology(num_cabinets, racks_per_cab, nodes_per_rack, prefix, zero_pad, num_width, cpus_per_node, cores_per_cpu, system_name, output_file):
    # Restructured to include a root-level metadata object
    topology = {
        "metadata": {
            "system_name": system_name
        },
        "cabinets": []
    }

    # Standard physical spacing modifiers (adjust these to scale the 3D visualizer)
    cabinet_spacing_x = 100
    cabinet_spacing_z = 0
    rack_spacing_x = 20
    rack_spacing_z = 0

    node_counter = 1

    for cab_idx in range(num_cabinets):
        cabinet = {
            "id": f"CAB-{cab_idx + 1:02d}",
            "x": cab_idx * cabinet_spacing_x,
            "z": cab_idx * cabinet_spacing_z,
            "racks": []
        }

        for rack_idx in range(racks_per_cab):
            rack = {
                "id": f"RACK-{cab_idx + 1:02d}-{rack_idx + 1:02d}",
                "x_offset": rack_idx * rack_spacing_x,
                "z_offset": rack_idx * rack_spacing_z,
                "nodes": []
            }

            for slot_idx in range(nodes_per_rack):
                # Apply dynamic padding based on user arguments
                if zero_pad:
                    # Dynamically inject the width into the format string (e.g., :02d)
                    hostname = f"{prefix}{node_counter:0{num_width}d}"
                else:
                    # Just append the raw number
                    hostname = f"{prefix}{node_counter}"

                rack["nodes"].append({
                    "hostname": hostname,
                    "slot": slot_idx,
                    "cpus": cpus_per_node,         
                    "cores_per_cpu": cores_per_cpu
                })
                node_counter += 1

            cabinet["racks"].append(rack)

        topology["cabinets"].append(cabinet)

    with open(output_file, 'w') as f:
        json.dump(topology, f, indent=2)

    print(f"Generated topology for '{system_name}' ({node_counter - 1} total nodes).")
    print(f"Hardware Specs: {cpus_per_node} CPUs/Node, {cores_per_cpu} Cores/CPU")
    print(f"Saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a 3D hardware map for MPI Profiler.")
    
    # System Metadata
    parser.add_argument("--system_name", type=str, default="Generic Cluster", help="Global name of the system/cluster")
    
    parser.add_argument("--cabinets", type=int, default=2, help="Number of physical cabinets")
    parser.add_argument("--racks", type=int, default=1, help="Number of racks per cabinet")
    parser.add_argument("--nodes", type=int, default=8, help="Number of nodes per rack")
    parser.add_argument("--prefix", type=str, default="node", help="Hostname prefix (e.g., 'node' for node001)")

    # Padding arguments
    parser.add_argument("--zero_pad", action="store_true", help="Include this flag to pad numbers with leading zeros")
    parser.add_argument("--num_width", type=int, default=1, help="The total width of the numeric part of the hostname")

    parser.add_argument("--cpus", type=int, default=2, help="Number of physical CPUs per node")
    parser.add_argument("--cores", type=int, default=32, help="Number of processing cores per CPU")

    parser.add_argument("--out", type=str, default="hardware_map.json", help="Output filename")

    args = parser.parse_args()

    generate_topology(
        args.cabinets, 
        args.racks, 
        args.nodes, 
        args.prefix, 
        args.zero_pad, 
        args.num_width, 
        args.cpus,      
        args.cores,
        args.system_name, # Passed to generator
        args.out
    )
