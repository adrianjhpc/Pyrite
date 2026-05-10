# Pyrite: MPI Communication Tracking and Visualiser

Pyrite is a lightweight MPI tracing and visual analytics toolchain for HPC applications. 

Designed for both **offline post-mortem analysis** and **live real-time telemetry**, this project intercepts MPI communication at runtime. It records compact binary traces (or streams them to a time-series database), extracts complex communication patterns, identifies performance bottlenecks (such as Late Senders and load imbalance), and provides a web-based rendering tool to display the results on a 3D representation of your physical hardware topology.

## Features

- **Zero-code instrumentation** via `LD_PRELOAD`
- **Dual-Mode Architecture:**
  - **Offline Mode:** Compact binary trace output (`.mpic`) compressed into chunked visualisation containers (`.mpix`).
  - **Live Mode:** Lock-free shared memory ring-buffers streamed via a C daemon to VictoriaMetrics, served to the browser via a lightweight FastAPI gateway.
- **Deep Context Tracking:** Records `MPI_Comm` handles and `MPI_Tag` IDs for mathematically perfect point-to-point pairing.
- **Advanced Timing Heuristics:** Calculates exact CPU block-time during Wait/Test calls to identify silent scaling killers like *Late Senders*, *Late Receivers*, and *Late Broadcasters*.
- **Lifecycle Bookends:** Automatically captures `MPI_Init` and `MPI_Finalize` to visualise rank spin-up and shutdown skew.
- **3D Hardware-Aware Visualisation** in the browser with interchangeable layouts (Blueprint, Top-Down, Rack-Front).
- **Persistent Analytics Overlays** isolating problem ranks, hotspot links, and communication patterns directly in 3D space.


## Visualiser Interface

![MPI Communication Visualiser GUI](images/mpicommvis-260426.png)
*The web-based visualiser rendering core-to-core network traffic using physical 3D tubes and directional arrowheads, alongside live message statistics and analytics overlays.*

## Project Layout

```text
mpi-comm-tracker/
├── CMakeLists.txt
├── src/
│   ├── CMakeLists.txt
│   ├── mpi_communication_tracking.c
│   ├── mpi_communication_tracking.h
│   ├── backend_file.c         # File-based trace writer (.mpic)
│   ├── backend_shm.c          # Live shared-memory telemetry provider
│   └── telemetry_daemon.c     # Async HTTP ingestion daemon for Live Mode
├── gateway/
│   ├── Dockerfile
│   ├── docker-compose.yml     # Orchestrates VictoriaMetrics + FastAPI
│   ├── requirements.txt
│   └── gateway.py             # Python API serving DB data to the browser
├── tools/
│   ├── mpi_data_parser.py     # Offline trace parser and heuristics engine
│   ├── topology_generator.py
│   └── slurm_topology_generator.py
├── vis/
│   ├── index_offline.html     # Dashboard for .mpix file playback
│   ├── index_live.html        # Dashboard for real-time cluster monitoring
│   ├── style.css
│   ├── visualiser_core.js     # Shared 3D rendering engine (Three.js)
│   ├── provider_offline.js    # Data provider for file playback
│   ├── provider_live.js       # Data provider for gateway polling
│   ├── analytics.js
│   ├── analytics-3d.js
│   └── analytics-controls.js
├── tests/
│   ├── CMakeLists.txt
│   ├── ctest_driver.py
│   ├── trace_parser.py
│   ├── test_*.c
│   └── test_*.f90
└── docs/
    └── developer-guide.md
```

## Quick Start

### 1. Build

```bash
mkdir build
cd build
cmake ..
make
```

This produces:

```text
build/src/libmpitrace_file.so
build/src/libmpitrace_shm.so
build/src/telemetry_daemon
```

### 2. Run an MPI application under the tracker

```bash
LD_PRELOAD=/path/to/build/src/libmpitrace_file.so mpirun -n 16 ./your_mpi_application
```

At `MPI_Finalize`, rank 0 writes a trace file like:

```text
your_mpi_application-YYYYMMDDHHMMSS.mpic
```

### 3. Generate or provide a hardware map

Optional, but recommended for meaningful 3D placement.

#### From Slurm

```bash
scontrol show topo > my_topo.txt
python tools/slurm_topology_generator.py my_topo.txt --racks_per_cab 4 --out hardware_map.json
```

#### Synthetic

```bash
python tools/topology_generator.py \
  --cabinets 2 \
  --racks 2 \
  --nodes 16 \
  --cpus 2 \
  --cores 32 \
  --system_name "My Local Cluster"
```

### 4. Parse and analyse the trace

```bash
python tools/mpi_data_parser.py your_mpi_application-YYYYMMDDHHMMSS.mpic hardware_map.json
```

This creates:

```text
your_mpi_application-YYYYMMDDHHMMSS.mpix
```


## Visuaisation

There are two visualisation modes that can be used. Offline visualisation uses the `libmpitrace_file.so` to produce the `*.mpix` file for a given application run, and then allows that to be visualised and explored.

Online or live visualisation uses `libmpitrace_shm.so` and the telemetry daemon (`telemetry_daemon`) to record events from running applications in a central database and then visualise running applications in realtime.;


### Offline visualiser

Open:

```text
vis/index_offline.html
```

Then load the generated `.mpix` file.

### Online/Live visualiser

#### Spin up the Database and Gateway
Move to a management or login node and start the Docker orchestration:
```bash
cd gateway
docker compose up -d --build
```
This starts VictoriaMetrics (port 8428) and the FastAPI Gateway (port 8000). Make sure you place a `cluster_topology.json` file in this directory so the 3D engine knows how to build the system.

#### Run the application
Ensure your compute nodes are configured to point to the VictoriaMetrics IP in `telemetry_daemon.c`, then launch:

```bash
LD_PRELOAD=/path/to/build/src/libmpitrace_shm.so mpirun -n 16 ./your_mpi_application
```

#### Open the Live Dashboard
Open `vis/index_live.html` in your browser. Ensure `provider_live.js` is pointed to your FastAPI gateway IP, and watch the cluster traffic stream in real-time.

---

## What the offline parser produces

The `.mpix` container includes:

- metadata
- topology
- binned message statistics
- hardware blueprint
- extracted analytics
- compressed time chunks

The analysis layer includes:

- top communicating ranks
- hottest sender/receiver links
- collective root summaries
- barrier skew estimates
- time-window / phase summaries
- detected communication patterns
- heuristic performance issues

---

## Visual Analytics

The frontend provides:

- a 3D hardware view
- timeline playback
- overall and active message statistics
- analytics cards
- persistent 3D overlays for:
  - issue-related ranks
  - hotspot links
  - collective roots
  - top ranks
  - pattern-related highlights
- issue-card-to-3D isolation
- analytics legend and highlight filters

---

## Supported MPI Calls

### Lifecycle and State
- `MPI_Init`
- `MPI_Finalize`

### Point-to-point
- `MPI_Send`
- `MPI_Recv`
- `MPI_Bsend`
- `MPI_Ssend`
- `MPI_Rsend`
- `MPI_Isend`
- `MPI_Ibsend`
- `MPI_Issend`
- `MPI_Irsend`
- `MPI_Irecv`
- `MPI_Sendrecv`

### Completion / synchronization
- `MPI_Wait`
- `MPI_Waitall`
- `MPI_Waitany`
- `MPI_Waitsome`
- `MPI_Test`
- `MPI_Testany`
- `MPI_Testall`
- `MPI_Testsome`
- `MPI_Barrier`

### Collectives
- `MPI_Bcast`
- `MPI_Reduce`
- `MPI_Allreduce`
- `MPI_Gather`
- `MPI_Scatter`
- `MPI_Allgather`

---

## Running Tests

```bash
cd build
ctest --output-on-failure
```

Optional Fortran test support:

```bash
cmake -S . -B build -DMPI_TRACE_FORTRAN_TESTS=AUTO
cmake -S . -B build -DMPI_TRACE_FORTRAN_TESTS=ON
cmake -S . -B build -DMPI_TRACE_FORTRAN_TESTS=OFF
```

---

## Limitations

- Communicator identity is not yet processed explicitly, meaning we only record global ranks for communications and do not differentiate between communicators
- Some collective behaviour is approximated in the visualisation to make it visible 
- The analytic functionality is currently based on heuristic approaches, not formal proof based functionality
- Persistent MPI requests are not yet fully modelled
- Thread safety/heavy `MPI_THREAD_MULTIPLE` usage is not yet tested or ensured

---

## Documentation

For internal details, trace format notes, parser behaviour, frontend module layout, analytics overlays, testing, and extension guidance, see [docs/developer-guide.md](docs/developer-guide.md).

## Authors
This has been developed by Adrian Jackson.

## License

Apache 2.0. See [LICENSE](LICENSE).
