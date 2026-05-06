/* shared_telemetry.h */
#ifndef SHARED_TELEMETRY_H
#define SHARED_TELEMETRY_H

#include <stdatomic.h>
#include "mpi_communication_tracking.h" // Includes telemetry_event_t

#define RING_BUFFER_SIZE 65536
#define RING_BUFFER_MASK (RING_BUFFER_SIZE - 1)

// The Ring Buffer (Padded to 64 bytes to prevent False Sharing)
typedef struct {
    telemetry_event_t events[RING_BUFFER_SIZE];
    atomic_size_t head;
    atomic_size_t tail;
    atomic_size_t dropped_events;
    
    // Pad out to 64-byte boundary to prevent cache-line bouncing
    char padding[64 - (3 * sizeof(atomic_size_t)) % 64]; 
} spsc_ring_buffer_t;

// The Shared Memory Layout for a single Subset/NUMA region
typedef struct {
    atomic_int daemon_running; // Set to 0 to tell daemon to gracefully exit
    int num_ranks_in_subset;
    spsc_ring_buffer_t buffers[]; // Flexible array member
} shared_telemetry_state_t;

#endif
