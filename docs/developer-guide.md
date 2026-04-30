# Developer Guide

This document describes the internal structure of the MPI communication tracer, trace consumer, analytics functionality, and visualisation functionality. This includes the interceptor library, parser, analytics pipeline, frontend display architecture, and test suite.

---

## 1. Overview

The project has three main runtime stages:

1. **MPI interception**
   - PMPI wrappers record MPI activity to a binary `.mpic` trace.

2. **Parsing and analysis**
   - `tools/mpi_data_parser.py` converts `.mpic` into `.mpix`.
   - It also extracts summary analytics and heuristic performance findings.

3. **Frontend visualisation**
   - `vis/` loads `.mpix`, streams timeline chunks on demand, renders hardware and traffic in 3D, and overlays analytics.

---

## 2. Source Tree

```text
src/
  mpi_communication_tracking.c
  mpi_communication_tracking.h

tools/
  mpi_data_parser.py
  topology_generator.py
  slurm_topology_generator.py

vis/
  index.html
  style.css
  visualiser.js
  analytics.js
  analytics-3d.js
  analytics-controls.js

tests/
  CMakeLists.txt
  ctest_driver.py
  trace_parser.py
  test_*.c
  test_*.f90
```

---

## 3. Interceptor Library

### 3.1 Purpose

The interceptor is a shared library loaded with `LD_PRELOAD`. It wraps MPI calls via PMPI and records trace records with low overhead.

### 3.2 Core responsibilities

- initialise internal tracing state in `MPI_Init` / `MPI_Init_thread`
- intercept supported MPI calls
- translate communicator-local ranks into world-rank space
- buffer trace events in memory
- flush to local files as needed
- aggregate per-process outputs into a single `.mpic` file at `MPI_Finalize`

### 3.3 Trace record types

The library writes two record classes:

- **small records**
  - point-to-point calls
  - wait/test/completion events
  - barrier and smaller collective summaries

- **large records**
  - operations represented as two directional parts
  - e.g. `MPI_Sendrecv`, `MPI_Gather`, `MPI_Scatter`, `MPI_Allgather`

### 3.4 Supported calls

#### Point-to-point
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

#### Completion and sync
- `MPI_Wait`
- `MPI_Waitall`
- `MPI_Waitany`
- `MPI_Waitsome`
- `MPI_Test`
- `MPI_Testany`
- `MPI_Testall`
- `MPI_Testsome`
- `MPI_Barrier`

#### Collectives
- `MPI_Bcast`
- `MPI_Reduce`
- `MPI_Allreduce`
- `MPI_Gather`
- `MPI_Scatter`
- `MPI_Allgather`

### 3.5 Nonblocking tracking 

The interceptor also tracks pending non-blocking requests internally. Instead of logging nonblocking communication only at posting time, it:

- records request metadata on `MPI_Isend`, `MPI_Irecv`, etc.
- resolves requests on:
  - `MPI_Wait`
  - `MPI_Waitall`
  - `MPI_Waitany`
  - `MPI_Waitsome`
  - `MPI_Test`
  - `MPI_Testall`
  - `MPI_Testany`
  - `MPI_Testsome`

This allows:

- correct completion-time recording
- actual receive sender resolution for `MPI_ANY_SOURCE`
- actual completed receive counts where available

### 3.6 Fortran wrappers

The library also includes symbol-level Fortran wrappers, primarily intended for:

- `mpif.h`
- many `use mpi` implementations

Coverage includes non-blocking completions and request-handling routines, but it is not intended as a full portable `mpi_f08` interception layer yet.

---

## 4. `.mpic` Binary Format

The `.mpic` file is the raw output of the interceptor.

### 4.1 Global header

The file begins with:

- total ranks
- date string
- program name

### 4.2 Process info records

One per rank, containing:

- rank
- process id
- core
- chip
- hostname

### 4.3 Per-rank sections

For each rank:

1. rank id
2. `"P2P Small Type Messages"`
3. number of small records
4. small records
5. `"P2P Large Type Messages"`
6. number of large records
7. large records

### 4.4 Parser strategy

To process a trace file we provide a tool; `tools/mpi_data_parser.py`, which is designed to try and parse the file as created, but can also deal with malformed or truncated files, using the following approach:

1. **strict parsing**
   - trusts section counts from the trace file
   - detects malformed structure early

2. **salvage parsing**
   - scans for section anchors
   - useful for partially damaged files

This is provided to try and enable some profile/tracing data to be analysed if in a program run fails to complete successfully, but the primary mode of working requires a correctly formed trace file.

---

## 5. Parser and Analysis Pipeline

### 5.1 Purpose

`tools/mpi_data_parser.py` converts `.mpic` into `.mpix` and creates a visualisation-friendly analysis layer.

### 5.2 Outputs

The parser emits a compressed `.mpix` container with:

- metadata
- topology
- message statistics
- hardware blueprint
- analysis
- compressed timeline chunks

### 5.3 Timeline chunking

The timeline is split into fixed-size event chunks and compressed with zlib.  This allows the visualisation tool to only loads only the chunks needed for the current time window, enabling large traces to be visualised without requiring the full data to be in memory at the same time.

---

## 6. Analytics Model

The parser constructs an `analysis` object in the `.mpix` header.

### 6.1 High-level structure

```json
{
  "summary": { ... },
  "per_rank": [ ... ],
  "top_ranks_by_out_bytes": [ ... ],
  "top_ranks_by_in_bytes": [ ... ],
  "top_ranks_by_touch_bytes": [ ... ],
  "top_links": [ ... ],
  "collective_roots": [ ... ],
  "barrier_spreads": [ ... ],
  "patterns": [ ... ],
  "issues": [ ... ],
  "time_windows": [ ... ]
}
```

### 6.2 Summary
Includes:
- total events
- canonical transfer events
- transfer bytes
- completion events
- barrier events
- estimated runtime
- pair density
- average peer counts

### 6.3 Per-rank summaries
Includes:
- bytes out / in
- message counts out / in
- distinct peers
- touch volume
- completion events
- barrier events
- collective event counts

### 6.4 Top links
Hottest sender/receiver links from the canonical transfer subset.

### 6.5 Collective roots
Summaries for concentrated rooted collective traffic.

### 6.6 Barrier spreads
Heuristic skew estimates based on repeated barrier timing order.

### 6.7 Time windows
Coarse phase summaries, including per-window:
- events
- transfer bytes
- transfer counts
- completion counts
- barrier counts
- collective counts

---

## 7. Pattern Detection

The parser currently infers several communication motifs heuristically.

### 7.1 Master/worker or star-like behaviour
Triggered when one rank dominates communication volume and degree.

### 7.2 Rooted collective concentration
Triggered when rooted collective traffic is concentrated on a small number of roots.

### 7.3 Ring / nearest-neighbour
Triggered when traffic is dominated by short rank-distance communication.

### 7.4 Neighbourhood exchange / halo-like behaviour
Triggered when communication concentrates on a small set of offsets and reciprocal pairs.

### 7.5 All-to-all-like behaviour
Triggered when the pair graph is dense and peer counts are high.

### 7.6 Ping-pong pairs
Triggered when balanced two-way communication is detected between a small number of pairs.

---

## 8. Issue Detection

The parser emits heuristic issue records for likely performance problems.

Current issue types include:

- `small_message_overhead`
- `communication_imbalance`
- `barrier_imbalance`
- `synchronization_heavy`
- `collective_root_bottleneck`
- `link_hotspot`
- `global_collective_heavy`

Each issue may contain:

- `severity`
- `score`
- `description`
- `ranks`
- `pairs`
- `metrics`

Important: these are diagnostics for visualisation and first-pass investigation, not formal proof of a problem.

---

## 9. Frontend Architecture

The frontend is split into several files.

### 9.1 `visualiser.js`
Main application logic:

- Three.js setup
- camera/control state
- chunk loading
- playback
- hardware topology building
- dynamic communication rendering
- metadata and spectrograms
- saved views and recording

### 9.2 `analytics.js`
Renders the analytics panel:

- summary
- detected patterns
- issue list
- top ranks
- top links
- collective roots
- barrier skew
- communication phases

Also supports:
- focusing ranks/links from analytics cards
- issue-card-to-3D isolation UI

### 9.3 `analytics-3d.js`
Renders persistent 3D analytics overlays:

- issue-related rank halos
- pattern rank halos
- collective root halos
- top rank halos
- hotspot / issue / ping-pong links

Also supports:
- global enable/disable
- overlay configuration
- isolating a single issue’s related overlays

### 9.4 `analytics-controls.js`
Renders:
- analytics 3D legend
- overlay filters
- overlay limits
- enable/disable UI

---

## 10. Frontend Data Flow

### 10.1 File loading
`index.html` loads `.mpix` through the browser file picker.

### 10.2 Header load
The frontend reads the compressed header first and populates:

- metadata
- topology
- statistics
- analysis
- chunk index

### 10.3 Timeline chunk load
As playback advances, the frontend loads only the relevant chunk using the offsets stored in the header.

### 10.4 Live rendering
At each playback step, the frontend:

- selects active events in the current time window
- aggregates them for rendering
- draws active communication tubes/arcs
- updates dynamic message stats

### 10.5 Analytics overlays
These are independent of playback-time communication rendering and are built from `parsedData.analysis`.

---

## 11. 3D Analytics Overlay Behaviour

### 11.1 Persistent overlays
These remain visible even when playback is paused or the current active timeline has moved elsewhere.

### 11.2 Isolation
When the user clicks **Isolate 3D** on an issue card:

- only ranks/links tied to that issue remain highlighted
- unrelated overlays are hidden temporarily
- a banner indicates active isolation
- the issue card is visually marked

### 11.3 Layout changes
After layout changes, overlays are refreshed so arcs and halos align with the new positions.

---

## 12. Testing

The project uses CTest-native integration tests.

### 12.1 C tests
Coverage includes:
- blocking point-to-point
- `MPI_ANY_SOURCE`
- subcommunicator rank translation
- collectives
- nonblocking request completion
- `Wait`, `Waitall`, `Waitany`, `Waitsome`
- `Test`, `Testall`, `Testany`, `Testsome`

### 12.2 Fortran tests
Optional Fortran tests cover:
- nonblocking send/receive
- `MPI_WAIT`
- `MPI_WAITALL`
- `MPI_WAITANY`
- `MPI_TESTALL`

#### Configure Fortran support

```bash
cmake -S . -B build -DMPI_TRACE_FORTRAN_TESTS=AUTO
cmake -S . -B build -DMPI_TRACE_FORTRAN_TESTS=ON
cmake -S . -B build -DMPI_TRACE_FORTRAN_TESTS=OFF
```

#### Run tests

```bash
cd build
ctest --output-on-failure
```

---

## 13. Extending the Interceptor

Common extension points in `src/` include:

### Add a new MPI wrapper
1. define a message type id in the header
2. add the C wrapper
3. decide whether it maps to:
   - small record
   - large record
   - request-tracked completion behaviour
4. extend parser message type tables
5. extend visualiser category tables
6. add tests

### Add new Fortran coverage
1. add symbol-layer wrapper(s)
2. convert handles with `MPI_Fint` conversion routines
3. convert statuses carefully
4. add CTest Fortran coverage

---

## 14. Extending the Parser

Common extensions in `tools/mpi_data_parser.py`:

### Add new MPI message types
- extend `MESSAGE_TYPES`
- update any categorisation logic
- update any analysis rules that depend on call type

### Add new analytics
- extend `analyse_trace(data)`
- keep outputs in `analysis`
- prefer JSON-serialisable structures
- avoid raw Python-only objects in the final header

### Design guidance
Analytics should be:
- cheap enough to run offline in Python
- interpretable in the UI
- heuristic and explainable
- easy to map back to ranks, links, or time windows

---

## 15. Extending the Frontend

### Add new analytics cards
Use `analytics.js` and follow the existing card helper pattern.

### Add new persistent overlays
Use `analytics-3d.js` and:
- collect highlight targets from `parsedData.analysis`
- map those to ranks/links
- render persistent halos/arcs

### Add new controls
Use `analytics-controls.js` and wire them to `Analytics3D.configure(...)`.

---

## 16. Current Limitations

- communicator identity is not yet stored explicitly in the trace
- some collective semantics are approximated
- analytics are heuristic
- persistent requests are not yet fully modelled
- full `MPI_THREAD_MULTIPLE` hardening is still incomplete
- `mpi_f08` interception is not guaranteed portably
- very dense traces can still become visually cluttered in the frontend

---

## 17. Potential Future Work

Potential next improvements include:

- Network link identification and visualisation
- Supporting GPU-aware MPI communications and visualisation
- Persistent request tracking
- Communicator id/versioning in the trace format
- Richer phase segmentation
- Stronger stencil / halo detection
- Direct export of issues/patterns as CSV/JSON
- Full visual linking between issue cards and relevant time windows
- More browser-side filtering and comparison tools

---

## 18. Typical Development Workflow

```bash
# Configure
cmake -S . -B build -DMPI_TRACE_FORTRAN_TESTS=AUTO

# Build
cmake --build build

# Run tests
ctest --test-dir build --output-on-failure

# Profile an application
LD_PRELOAD=$PWD/build/src/libmpi_comm_tracker.so mpirun -n 16 ./your_mpi_application

# Parse trace
python tools/parse_mpic.py your_mpi_application-YYYYMMDDHHMMSS.mpic hardware_map.json

# Open frontend
# Load vis/index.html in a browser and open the .mpix file
```

---

## 19. Notes for Maintainers

When changing one layer, remember the others:

- **interceptor change**  
  usually implies parser and frontend and tests updates

- **parser analytics change**  
  may imply analytics panel and analytics 3D updates

- **frontend visual change**  
  may require new fields in `analysis`

A safe workflow is:

1. update or add tests
2. update interceptor/parser/frontend
3. re-run CTest
4. validate with a known `.mpix` trace in the browser

