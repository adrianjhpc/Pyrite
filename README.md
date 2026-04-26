# MPI Communication Tracker and 3D Hardware Visualiser

This project is a lightweight, low-overhead profiling tool designed to intercept Message Passing Interface (MPI) communications, record them to a highly compressed binary file, and visualise the data flowing across a 3D representation of your physical HPC hardware topology.

## Visualiser Interface

![MPI Communication Visualiser GUI](images/mpicommvis-260426.png)
*The web-based visualiser rendering core-to-core network traffic using physical 3D tubes and directional arrowheads, alongside live message statistics.*

## Project Architecture

The tool is divided into three distinct components:

1. **The Interceptor (`src/`)**: A C-based shared library that uses the PMPI profiling interface to intercept standard MPI calls. It records timestamps (via `MPI_Wtime`), message sizes, global metadata, and endpoints with minimal I/O overhead by buffering data in memory before flushing it to disk.
2. **The Parser & Generators (`tools/`)**: Python scripts that generate physical hardware maps (either synthetically or by querying Slurm). The main parser aligns timestamps, maps MPI ranks to physical hardware coordinates, and heavily compresses the trace into a chunked `.mpix` wrapper file.
3. **The Visualiser (`vis/`)**: A client-side web application built with Three.js. It streams the chunked `.mpix` file to handle massive traces without crashing the browser, rendering the physical hardware (cabinets, racks, nodes, chips, and cores) in 3D.

---

## Key Features

* **Zero-Code Instrumentation:** Uses `LD_PRELOAD` to profile applications without requiring recompilation or source code modifications.
* **Massive Trace Streaming:** The parser chunks and zlib-compresses the trace timeline into an `.mpix` format. The browser async-loads only the chunks required for the current timestamp, preventing Out-Of-Memory crashes.
* **Hardware-Accurate Topology:** Draws exact CPU and Core layouts. Intra-node traffic jumps over cores, while inter-node traffic routes cleanly across the network.
* **Directional Data Flow:** Network traffic is rendered as physical 3D tubes capped with arrowheads to explicitly show the direction of data flow (Sender -> Receiver).
* **Interactive Dashboards:** Features a global spectrogram, a live active-message statistics board, and automatic Run Metadata extraction (Date, Executable Name, Active Scale).
* **Cinematic 3D Controls:** Click any node or core to automatically fly the camera to it. Hover over active ranks for a detailed hardware tooltip.

---

## Directory Structure

```text
mpi-comm-tracker/
├── CMakeLists.txt               # Root CMake configuration
├── src/                         # C Profiler Backend
│   ├── CMakeLists.txt           
│   ├── mpi_communication_tracking.c
│   └── mpi_communication_tracking.h
├── tools/                       # Python Data Parsers & Generators
│   ├── parse_mpic.py            # Main binary parser to .mpix
│   ├── generate_topology.py     # Synthetic hardware map generator
│   └── slurm_topology.py        # Automatic Slurm hardware map generator
└── vis/                         # 3D Web Visualiser
    ├── index.html               
    ├── style.css                
    └── visualiser.js
```
---

## Prerequisites

* **Backend**: C Compiler, CMake (>= 3.10), and an MPI implementation (OpenMPI, MPICH, etc.)

* **Parser**: Python 3.x (No external libraries required; uses standard `struct` and `zlib`)

* **Frontend**: A modern web browser with WebGL support (Chrome, Firefox, Edge)

---

## Usage Guide

### 1. Build the Interceptor Library
You do not need to modify or recompile your existing MPI applications. First, compile the tracking library into a shared object (`.so`):

```bash
mkdir build
cd build
cmake ..
make
```
This will generate `libmpi_comm_tracker.so` inside the `build/src/` directory.

### 2. Run Your MPI Application
Run your standard MPI application, but use the `LD_PRELOAD` environment variable to inject the tracking library before the system MPI library loads. 

```bash
LD_PRELOAD=/path/to/build/src/libmpi_comm_tracker.so mpirun -n 16 ./your_mpi_application
```

When the application finishes (and calls `MPI_Finalize`), Rank 0 will aggregate the buffered data and output a single binary file named something like:
`your_mpi_application-202610151230.mpic`

---

### 3. Define Your Hardware Map (Optional but Recommended)
To visualise the data in 3D space, create a `hardware_map.json` file. This tells the visualiser where each hostname physically resides in your datacenter.

```json
{
  "cabinets": [
    {
      "id": "CAB-01", "x": -50, "z": 0,
      "racks": [
        {
          "id": "RACK-1", "x_offset": 0, "z_offset": 0,
          "nodes": [
            {"hostname": "node001", "slot": 0},
            {"hostname": "node002", "slot": 1}
          ]
        }
      ]
    }
  ]
}
```

To generate the hardware map you can use one of the tools we provide:

#### Option A: Auto-generate from Slurm
If you are running on a Slurm cluster, export the topology and let the script build the 3D map:

```bash
scontrol show topo > my_topo.txt
python tools/slurm_topology.py my_topo.txt --racks_per_cab 4 --out hardware_map.json
```

##### Option B: Synthetic Generation

If you want to manually design a cluster layout (e.g., for local testing or if you don't have a Slurm topology):

```bash
python tools/generate_topology.py --cabinets 2 --racks 2 --nodes 16 --cpus 2 --cores 32 --system_name "My Local Cluster"
```
*Note: If no hardware map is provided, the parser will default to a 1D scatter layout.*

---

### 4. Parse the Binary Data
Use the Python script to convert the raw `.mpic` binary into a visualiser-ready JSON file. Pass the `.mpic` file as the first argument, and your hardware map as the second.

```bash
cd tools/
python parse_mpic.py ../your_mpi_application-202610151230.mpic ../hardware_map.json
```
This will generate a new file named `your_mpi_application-202610151230.mpix`.

---

### 5. Visualise the Profile
The visualiser runs entirely in your browser without needing a web server. 

1. Open `vis/index.html` in your web browser.
2. Click **"Load Profile"** in the sidebar.
3. Select the `.mpix` file generated in the previous step.
4. Camera Controls:
 * **Orbit**: Left Click + Drag
 * **Pan**: Right Click + Drag (or use the arrow keys)
 * **Zoom**: Scroll Wheel
 * **Focus**: Click on any node or core to automatically fly the camera to that location.
5. Hit **Play** (or scrub the timeline slider) to watch data packets move. Hover your mouse over any glowing core to see its hardware assignment tooltips.

---

## Supported MPI Calls
The tool currently tracks the following operations:
* **Point-to-Point:** `MPI_Send`, `MPI_Recv`, `MPI_Isend`, `MPI_Irecv`, `MPI_Bsend`, `MPI_Ssend`, `MPI_Rsend`, `MPI_Ibsend`, `MPI_Issend`, `MPI_Irsend`, `MPI_Sendrecv`
* **Collectives:** `MPI_Bcast`, `MPI_Reduce`, `MPI_Allreduce`, `MPI_Gather`, `MPI_Scatter`, `MPI_Allgather`
* **Synchronization:** `MPI_Wait`, `MPI_Waitall`, `MPI_Barrier`
