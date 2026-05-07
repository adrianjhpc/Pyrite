/* shared_telemetry.h */
#ifndef SHARED_TELEMETRY_H
#define SHARED_TELEMETRY_H

#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>

#include "mpi_communication_tracking.h" /* Includes telemetry_event_t */

#define CACHELINE_SIZE 64u
#define RING_BUFFER_SIZE 65536u
#define RING_BUFFER_MASK (RING_BUFFER_SIZE - 1u)
#define SHARED_TELEMETRY_LAYOUT_VERSION 3

_Static_assert((RING_BUFFER_SIZE & (RING_BUFFER_SIZE - 1u)) == 0u,
               "RING_BUFFER_SIZE must be a power of two");

/*
 * SPSC ring:
 *   producer writes event, then head (release)
 *   consumer reads head (acquire), then event, then writes tail (release)
 *
 * Head/tail/dropped are each aligned to their own cache line to reduce
 * false sharing between producer and consumer.
 */
typedef struct {
    _Alignas(CACHELINE_SIZE) atomic_size_t head;
    _Alignas(CACHELINE_SIZE) atomic_size_t tail;
    _Alignas(CACHELINE_SIZE) atomic_size_t dropped_events;

    telemetry_event_t events[RING_BUFFER_SIZE];
} spsc_ring_buffer_t;

/*
 * One producer slot per rank in the shared-memory subset.
 *
 * Time model:
 *   event.time = PMPI_Wtime() - mpi_time_zero
 *
 * Daemon converts to Unix epoch nanoseconds as:
 *   unix_time_zero_ns + event.time * 1e9
 *
 * Each producer writes its metadata, then sets anchor_ready=1 with release
 * semantics. The creator waits until all anchor_ready flags are visible
 * before publishing shared ready=1.
 */
typedef struct {
    atomic_int anchor_ready;   /* producer sets to 1 when slot metadata is valid */

    int subset_rank;           /* rank within the node/NUMA subset */
    int world_rank;            /* rank in MPI_COMM_WORLD */
    int reserved0;

    double  mpi_time_zero;     /* local MPI time corresponding to event.time == 0 */
    int64_t unix_time_zero_ns; /* local CLOCK_REALTIME corresponding to event.time == 0 */

    int64_t reserved1[4];

    spsc_ring_buffer_t ring;
} telemetry_slot_t;

/*
 * Shared memory region for one subset/NUMA region.
 *
 * Initialisation protocol:
 *   1. creator creates and zeroes the region
 *   2. creator initialises header and all slots with ready=0
 *   3. all ranks attach
 *   4. each rank writes its own slot metadata and sets slot.anchor_ready=1
 *   5. creator waits until all slot.anchor_ready are observed
 *   6. creator stores ready=1 (release)
 *   7. daemon may safely consume
 */
typedef struct {
    atomic_int ready;           /* 0 until fully initialised */
    atomic_int daemon_running;  /* creator sets to 0 to request daemon shutdown */
    atomic_int unlink_on_exit;  /* daemon unlinks shm iff non-zero */

    int layout_version;
    int num_ranks_in_subset;

    int reserved0[6];

    telemetry_slot_t slots[];
} shared_telemetry_state_t;

#endif

