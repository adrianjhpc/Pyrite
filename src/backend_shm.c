#define _GNU_SOURCE
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "mpi_communication_tracking.h"
#include "shared_telemetry.h"

/* -------------------------------------------------------------------------- */
/* Local backend state                                                        */
/* -------------------------------------------------------------------------- */

static shared_telemetry_state_t *shm_state = NULL;
static spsc_ring_buffer_t *my_ring_buffer = NULL;

static char shm_name[256] = {0};
static size_t shm_region_size = 0;

static MPI_Comm shm_subset_comm = MPI_COMM_NULL;
static int my_subset_rank = -1;
static int my_subset_size = 0;
static int my_local_rank = -1;
static int my_numa_region = -1;

static int shm_creator = 0;
static pid_t daemon_pid = -1;

/* -------------------------------------------------------------------------- */
/* Utility helpers                                                            */
/* -------------------------------------------------------------------------- */

static void reset_backend_state(void) {
    shm_state = NULL;
    my_ring_buffer = NULL;
    shm_name[0] = '\0';
    shm_region_size = 0;
    shm_subset_comm = MPI_COMM_NULL;
    my_subset_rank = -1;
    my_subset_size = 0;
    my_local_rank = -1;
    my_numa_region = -1;
    shm_creator = 0;
    daemon_pid = -1;
}

static void log_shm_error(const char *where, int errnum) {
    fprintf(stderr,
            "[mpi-trace][shm] ERROR on rank %d (%s): %s failed: %s\n",
            tracking_my_rank,
            (tracking_hostname[0] != '\0') ? tracking_hostname : "unknown-host",
            where,
            strerror(errnum));
    fflush(stderr);
}

static void log_shm_message(const char *msg) {
    fprintf(stderr,
            "[mpi-trace][shm] rank %d (%s): %s\n",
            tracking_my_rank,
            (tracking_hostname[0] != '\0') ? tracking_hostname : "unknown-host",
            msg);
    fflush(stderr);
}

static int close_fd_quietly(int fd) {
    if (fd >= 0) {
        while (close(fd) != 0) {
            if (errno != EINTR) {
                return -1;
            }
        }
    }
    return 0;
}

static int set_fd_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFD);
    if (flags < 0) {
        return -1;
    }
    if (fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
        return -1;
    }
    return 0;
}

static unsigned long long hash_string_u64(const char *s) {
    /* 64-bit FNV-1a */
    unsigned long long h = 1469598103934665603ULL;
    while (s != NULL && *s != '\0') {
        h ^= (unsigned long long)(unsigned char)(*s);
        h *= 1099511628211ULL;
        s++;
    }
    return h;
}

static unsigned long long determine_job_tag(MPI_Comm subset_comm, int subset_rank) {
    unsigned long long tag = 0;

    if (subset_rank == 0) {
        const char *job_env = NULL;

        job_env = getenv("SLURM_JOB_ID");
        if (job_env == NULL || job_env[0] == '\0') job_env = getenv("PMI_JOBID");
        if (job_env == NULL || job_env[0] == '\0') job_env = getenv("PMIX_NAMESPACE");
        if (job_env == NULL || job_env[0] == '\0') job_env = getenv("OMPI_MCA_orte_hnp_uri");

        if (job_env != NULL && job_env[0] != '\0') {
            tag = hash_string_u64(job_env);
        } else {
            /* Fallback: subset leader PID is host-local unique for live jobs */
            tag = (unsigned long long)getpid();
        }
    }

    PMPI_Bcast(&tag, 1, MPI_UNSIGNED_LONG_LONG, 0, subset_comm);
    return tag;
}

static int build_shm_name(char *out, size_t out_len, const char *host, unsigned long long job_tag, int numa_region) {
    int rc;

    if (out == NULL || out_len == 0) {
        return -1;
    }

    rc = snprintf(out, out_len, "/mpi_telemetry_%s_job%llx_numa%d",
                  (host != NULL && host[0] != '\0') ? host : "unknownhost",
                  (unsigned long long)job_tag,
                  numa_region);

    if (rc < 0 || (size_t)rc >= out_len) {
        return -1;
    }

    return 0;
}

/*
 * Dynamically query Linux for the current CPU/NUMA location.
 * If unavailable, fall back to a single-region view for correctness.
 */
static int get_my_numa_node(int local_rank) {
    unsigned cpu = 0;
    unsigned node = 0;
    (void)local_rank;

#ifdef SYS_getcpu
    if (syscall(SYS_getcpu, &cpu, &node, NULL) == 0) {
        return (int)node;
    }
#endif

    /* Correctness-first fallback: one shared region per node */
    return 0;
}

static int create_and_map_region(const char *name, size_t bytes, shared_telemetry_state_t **state_out) {
    int fd = -1;
    shared_telemetry_state_t *mapped = NULL;

    if (state_out == NULL) {
        return MPI_ERR_ARG;
    }
    *state_out = NULL;

    fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) {
        log_shm_error("shm_open(create)", errno);
        return MPI_ERR_IO;
    }

    if (ftruncate(fd, (off_t)bytes) != 0) {
        log_shm_error("ftruncate", errno);
        close_fd_quietly(fd);
        shm_unlink(name);
        return MPI_ERR_IO;
    }

    mapped = (shared_telemetry_state_t *)mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        log_shm_error("mmap(create)", errno);
        close_fd_quietly(fd);
        shm_unlink(name);
        return MPI_ERR_NO_MEM;
    }

    close_fd_quietly(fd);
    *state_out = mapped;
    return MPI_SUCCESS;
}

static int attach_and_map_region(const char *name, size_t bytes, shared_telemetry_state_t **state_out) {
    int fd = -1;
    shared_telemetry_state_t *mapped = NULL;

    if (state_out == NULL) {
        return MPI_ERR_ARG;
    }
    *state_out = NULL;

    fd = shm_open(name, O_RDWR, 0600);
    if (fd < 0) {
        log_shm_error("shm_open(attach)", errno);
        return MPI_ERR_IO;
    }

    mapped = (shared_telemetry_state_t *)mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        log_shm_error("mmap(attach)", errno);
        close_fd_quietly(fd);
        return MPI_ERR_NO_MEM;
    }

    close_fd_quietly(fd);
    *state_out = mapped;
    return MPI_SUCCESS;
}

static void cleanup_partial_mapping(shared_telemetry_state_t **state_ptr, size_t bytes) {
    if (state_ptr != NULL && *state_ptr != NULL && *state_ptr != MAP_FAILED) {
        munmap(*state_ptr, bytes);
        *state_ptr = NULL;
    }
}

static int spawn_daemon_checked(const char *region_name, pid_t parent_pid, pid_t *daemon_pid_out) {
    int pipefd[2] = { -1, -1 };
    pid_t pid;
    int exec_err = 0;
    ssize_t nread;
    char pid_str[32];

    if (daemon_pid_out == NULL) {
        return MPI_ERR_ARG;
    }
    *daemon_pid_out = -1;

    if (pipe(pipefd) != 0) {
        log_shm_error("pipe", errno);
        return MPI_ERR_IO;
    }

    if (set_fd_cloexec(pipefd[1]) != 0) {
        log_shm_error("fcntl(FD_CLOEXEC)", errno);
        close_fd_quietly(pipefd[0]);
        close_fd_quietly(pipefd[1]);
        return MPI_ERR_IO;
    }

    pid = fork();
    if (pid < 0) {
        log_shm_error("fork", errno);
        close_fd_quietly(pipefd[0]);
        close_fd_quietly(pipefd[1]);
        return MPI_ERR_IO;
    }

    if (pid == 0) {
        int errnum;
        close_fd_quietly(pipefd[0]);

        snprintf(pid_str, sizeof(pid_str), "%d", (int)parent_pid);

        execl("/opt/mpi_telemetry/bin/telemetry_daemon",
              "telemetry_daemon",
              region_name,
              pid_str,
              NULL);

        errnum = errno;
        (void)!write(pipefd[1], &errnum, sizeof(errnum));
        _exit(127);
    }

    close_fd_quietly(pipefd[1]);

    do {
        nread = read(pipefd[0], &exec_err, sizeof(exec_err));
    } while (nread < 0 && errno == EINTR);

    close_fd_quietly(pipefd[0]);

    if (nread > 0) {
        int status;
        while (waitpid(pid, &status, 0) < 0) {
            if (errno != EINTR) {
                break;
            }
        }
        log_shm_error("execl(telemetry_daemon)", exec_err);
        return MPI_ERR_IO;
    }

    /* nread == 0 means the write end was closed on successful exec */
    *daemon_pid_out = pid;
    return MPI_SUCCESS;
}

/* -------------------------------------------------------------------------- */
/* Backend Implementation                                                     */
/* -------------------------------------------------------------------------- */

static int shm_init(int rank, int size) {
    MPI_Comm node_comm = MPI_COMM_NULL;
    MPI_Comm subset_comm = MPI_COMM_NULL;
    shared_telemetry_state_t *local_state = NULL;
    unsigned long long job_tag = 0;
    size_t local_region_size = 0;
    int local_rank = -1;
    int subset_rank = -1;
    int subset_size = 0;
    int numa_region = -1;
    int creator = 0;
    int create_ok = 1;
    int attach_ok = 1;
    int all_attach_ok = 1;
    int daemon_ok = 1;
    pid_t local_daemon_pid = -1;

    (void)rank;
    (void)size;

    reset_backend_state();

    if (PMPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &node_comm) != MPI_SUCCESS) {
        log_shm_message("PMPI_Comm_split_type(MPI_COMM_TYPE_SHARED) failed");
        return MPI_ERR_COMM;
    }

    if (PMPI_Comm_rank(node_comm, &local_rank) != MPI_SUCCESS) {
        PMPI_Comm_free(&node_comm);
        log_shm_message("PMPI_Comm_rank(node_comm) failed");
        return MPI_ERR_COMM;
    }

    numa_region = get_my_numa_node(local_rank);

    if (PMPI_Comm_split(node_comm, numa_region, local_rank, &subset_comm) != MPI_SUCCESS) {
        PMPI_Comm_free(&node_comm);
        log_shm_message("PMPI_Comm_split(node_comm, numa_region, ...) failed");
        return MPI_ERR_COMM;
    }

    if (PMPI_Comm_rank(subset_comm, &subset_rank) != MPI_SUCCESS ||
        PMPI_Comm_size(subset_comm, &subset_size) != MPI_SUCCESS) {
        PMPI_Comm_free(&subset_comm);
        PMPI_Comm_free(&node_comm);
        log_shm_message("PMPI_Comm_rank/size(subset_comm) failed");
        return MPI_ERR_COMM;
    }

    job_tag = determine_job_tag(subset_comm, subset_rank);

    if (build_shm_name(shm_name, sizeof(shm_name), tracking_hostname, job_tag, numa_region) != 0) {
        PMPI_Comm_free(&subset_comm);
        PMPI_Comm_free(&node_comm);
        log_shm_message("failed to construct a unique shared-memory name");
        return MPI_ERR_INTERN;
    }

    local_region_size = offsetof(shared_telemetry_state_t, buffers) + ((size_t)subset_size * sizeof(spsc_ring_buffer_t));

    /* Phase 1: creator allocates and initialises the region */
    if (subset_rank == 0) {
        int rc;
        creator = 1;

        rc = create_and_map_region(shm_name, local_region_size, &local_state);
        if (rc != MPI_SUCCESS) {
            create_ok = 0;
        } else {
            atomic_init(&local_state->daemon_running, 1);
            local_state->num_ranks_in_subset = subset_size;

            /*
             * Linux first-touch policy:
             * The creator is assumed to be running on the local NUMA region, so
             * zeroing the buffers here encourages local physical placement.
             */
            for (int i = 0; i < subset_size; i++) {
                atomic_init(&local_state->buffers[i].head, 0);
                atomic_init(&local_state->buffers[i].tail, 0);
                atomic_init(&local_state->buffers[i].dropped_events, 0);
            }
        }
    }

    /* Ensure all ranks see whether the region was successfully created */
    PMPI_Bcast(&create_ok, 1, MPI_INT, 0, subset_comm);
    if (!create_ok) {
        cleanup_partial_mapping(&local_state, local_region_size);
        if (creator) {
            shm_unlink(shm_name);
        }
        PMPI_Comm_free(&subset_comm);
        PMPI_Comm_free(&node_comm);
        return MPI_ERR_IO;
    }

    /* Phase 2: non-creators attach */
    if (subset_rank != 0) {
        int rc = attach_and_map_region(shm_name, local_region_size, &local_state);
        if (rc != MPI_SUCCESS) {
            attach_ok = 0;
        }
    }

    /* Ensure everyone agrees whether all attachers succeeded */
    PMPI_Allreduce(&attach_ok, &all_attach_ok, 1, MPI_INT, MPI_MIN, subset_comm);
    if (!all_attach_ok) {
        cleanup_partial_mapping(&local_state, local_region_size);

        if (creator) {
            shm_unlink(shm_name);
        }

        PMPI_Comm_free(&subset_comm);
        PMPI_Comm_free(&node_comm);
        return MPI_ERR_IO;
    }

    /* Phase 3: only after all ranks are attached do we spawn the daemon */
    if (subset_rank == 0) {
        int rc = spawn_daemon_checked(shm_name, getpid(), &local_daemon_pid);
        if (rc != MPI_SUCCESS) {
            daemon_ok = 0;
        }
    }

    PMPI_Bcast(&daemon_ok, 1, MPI_INT, 0, subset_comm);
    if (!daemon_ok) {
        cleanup_partial_mapping(&local_state, local_region_size);

        if (creator) {
            shm_unlink(shm_name);
        }

        PMPI_Comm_free(&subset_comm);
        PMPI_Comm_free(&node_comm);
        return MPI_ERR_IO;
    }

    /* Success: publish backend state */
    shm_state = local_state;
    shm_region_size = local_region_size;
    my_ring_buffer = &shm_state->buffers[subset_rank];
    shm_subset_comm = subset_comm;
    my_subset_rank = subset_rank;
    my_subset_size = subset_size;
    my_local_rank = local_rank;
    my_numa_region = numa_region;
    shm_creator = creator;
    daemon_pid = local_daemon_pid;

    PMPI_Comm_free(&node_comm);

    return MPI_SUCCESS;
}

static void shm_record_event(telemetry_event_t *event) {
    size_t head;
    size_t tail;

    if (my_ring_buffer == NULL || event == NULL) {
        return;
    }

    head = atomic_load_explicit(&my_ring_buffer->head, memory_order_relaxed);
    tail = atomic_load_explicit(&my_ring_buffer->tail, memory_order_acquire);

    if (head - tail >= RING_BUFFER_SIZE) {
        atomic_fetch_add_explicit(&my_ring_buffer->dropped_events, 1, memory_order_relaxed);
        return;
    }

    my_ring_buffer->events[head & RING_BUFFER_MASK] = *event;
    atomic_store_explicit(&my_ring_buffer->head, head + 1, memory_order_release);
}

static void shm_finalize(void) {
    /*
     * Important protocol note:
     * The daemon is expected to continue draining buffers after daemon_running
     * becomes 0, and only exit once all SPSC queues are empty.
     */

    if (shm_subset_comm != MPI_COMM_NULL) {
        PMPI_Barrier(shm_subset_comm);
    }

    if (shm_creator && shm_state != NULL) {
        atomic_store_explicit(&shm_state->daemon_running, 0, memory_order_release);
    }

    if (shm_state != NULL && shm_region_size > 0) {
        munmap(shm_state, shm_region_size);
        shm_state = NULL;
        my_ring_buffer = NULL;
    }

    if (shm_creator && daemon_pid > 0) {
        int status;
        while (waitpid(daemon_pid, &status, 0) < 0) {
            if (errno != EINTR) {
                log_shm_error("waitpid(daemon)", errno);
                break;
            }
        }
    }

    if (shm_creator && shm_name[0] != '\0') {
        if (shm_unlink(shm_name) != 0) {
            log_shm_error("shm_unlink", errno);
        }
    }

    if (shm_subset_comm != MPI_COMM_NULL) {
        PMPI_Comm_free(&shm_subset_comm);
        shm_subset_comm = MPI_COMM_NULL;
    }

    reset_backend_state();
}

/* -------------------------------------------------------------------------- */
/* Backend registration                                                       */
/* -------------------------------------------------------------------------- */

static tracking_backend_t shm_backend = {
    .init_backend = shm_init,
    .record_event = shm_record_event,
    .finalize_backend = shm_finalize
};

__attribute__((constructor))
static void register_shm_backend(void) {
    register_tracking_backend(&shm_backend);
}

