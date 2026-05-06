#define _GNU_SOURCE
#include <sys/mman.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include "shared_telemetry.h" 

static shared_telemetry_state_t *shm_state = NULL;
static spsc_ring_buffer_t *my_ring_buffer = NULL;
static char shm_name[256];

/* 
 * Dynamically queries the Linux Kernel to find which NUMA region 
 * this specific MPI process is currently executing on.
 */
static int get_my_numa_node(int local_rank) {
    unsigned cpu, node;
    
#ifdef SYS_getcpu
    // SYS_getcpu puts the Core ID in arg 1, and the NUMA Node in arg 2
    if (syscall(SYS_getcpu, &cpu, &node, NULL) == 0) {
        return (int)node;
    }
#endif

    // Fallback if the syscall fails or isn't supported on this OS:
    // Distribute across an assumed 4 sockets based on local rank parity.
    return local_rank % 4; 
}

static int shm_init(int rank, int size) {
    MPI_Comm node_comm, subset_comm;
    int local_rank, subset_rank, subset_size;

    // 1. Find all processes on this physical server
    PMPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &node_comm);
    PMPI_Comm_rank(node_comm, &local_rank);

    // 2. Discover our physical NUMA hardware location
    int my_numa_region = get_my_numa_node(local_rank);

    // 3. Magic: MPI automatically groups all ranks on the same NUMA node together
    PMPI_Comm_split(node_comm, my_numa_region, local_rank, &subset_comm);
    PMPI_Comm_rank(subset_comm, &subset_rank);
    PMPI_Comm_size(subset_comm, &subset_size);

    // Create a unique SHM name using the Hostname AND the NUMA Region ID
    snprintf(shm_name, sizeof(shm_name), "/mpi_telemetry_%s_numa%d", tracking_hostname, my_numa_region);
    size_t shm_size = sizeof(shared_telemetry_state_t) + (subset_size * sizeof(spsc_ring_buffer_t));

    // 4. "Subset Rank 0" (The first process on this specific NUMA node) creates the memory
    if (subset_rank == 0) {
        int fd = shm_open(shm_name, O_CREAT | O_RDWR | O_TRUNC, 0666);
        ftruncate(fd, shm_size);
        shm_state = mmap(NULL, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        
        atomic_init(&shm_state->daemon_running, 1);
        shm_state->num_ranks_in_subset = subset_size;

        /* 
         * LINUX FIRST-TOUCH POLICY IN ACTION:
         * Because this initialization loop (writing 0s) is executed by a process
         * bound to this NUMA node, the Linux kernel permanently maps these 
         * physical RAM pages to the local RAM banks of this specific CPU socket.
         */
        for(int i = 0; i < subset_size; i++) {
            atomic_init(&shm_state->buffers[i].head, 0);
            atomic_init(&shm_state->buffers[i].tail, 0);
            atomic_init(&shm_state->buffers[i].dropped_events, 0);
        }
     
        // Get the parent process pid to enable checking for crashes by the daemon
        char pid_str[32];
        snprintf(pid_str, sizeof(pid_str), "%d", getpid());

        // Spawn the daemon for this specific NUMA region
        if (fork() == 0) {
            // Pass the PID as the third argument (argv[2])
            execl("/opt/mpi_telemetry/bin/telemetry_daemon", 
                  "telemetry_daemon", shm_name, pid_str, NULL);
            exit(1); 
        }
    }

    // Synchronize to ensure the memory is mapped before others attach
    PMPI_Barrier(subset_comm);

    // 5. All other ranks on this NUMA node attach to the memory
    if (subset_rank != 0) {
        int fd = shm_open(shm_name, O_RDWR, 0666);
        shm_state = mmap(NULL, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    }

    // Map this process to its specific ring buffer slot
    my_ring_buffer = &shm_state->buffers[subset_rank];

    PMPI_Comm_free(&subset_comm);
    PMPI_Comm_free(&node_comm);
    return 0;
}

static void shm_record_event(telemetry_event_t *event) {
    if (!my_ring_buffer) return;

    size_t head = atomic_load_explicit(&my_ring_buffer->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&my_ring_buffer->tail, memory_order_acquire);

    if (head - tail >= RING_BUFFER_SIZE) {
        atomic_fetch_add_explicit(&my_ring_buffer->dropped_events, 1, memory_order_relaxed);
        return; 
    }

    my_ring_buffer->events[head & RING_BUFFER_MASK] = *event;
    atomic_store_explicit(&my_ring_buffer->head, head + 1, memory_order_release);
}

static void shm_finalize(void) {
    // Only the rank that started the daemon is allowed to kill it
    if (my_ring_buffer && my_ring_buffer == &shm_state->buffers[0]) {
        atomic_store_explicit(&shm_state->daemon_running, 0, memory_order_release);
    }
}

static tracking_backend_t shm_backend = {
    .init_backend = shm_init,
    .record_event = shm_record_event,
    .finalize_backend = shm_finalize
};

__attribute__((constructor))
static void register_shm_backend(void) {
    register_tracking_backend(&shm_backend);
}
