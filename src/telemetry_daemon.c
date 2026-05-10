#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <stddef.h>
#include <errno.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>

#include <curl/curl.h>

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "shared_telemetry.h"


#define PAYLOAD_BUFFER_SIZE   (2u * 1024u * 1024u)
#define LINE_BUFFER_SIZE      1024u
#define BATCH_CAPACITY        4096u
#define PER_RING_BURST        64u

#define OPEN_RETRY_TIMEOUT_MS 10000
#define READY_TIMEOUT_MS      10000

#define NORMAL_SLEEP_NS       50000000L
#define SHUTDOWN_SLEEP_NS     20000000L
#define SHUTDOWN_IDLE_PASSES  5

#define PARENT_CHECK_INTERVAL_NS 2000000000LL
#define DROP_LOG_INTERVAL_NS     2000000000LL

static char payload_buffer[PAYLOAD_BUFFER_SIZE];
static char daemon_hostname[256];
static char escaped_hostname[512];

static CURL *curl_handle = NULL;
static struct curl_slist *curl_headers = NULL;

static volatile sig_atomic_t stop_requested = 0;

typedef struct {
    telemetry_event_t event;
    int slot_index;
} drained_event_t;

static void on_signal(int signo) {
    (void)signo;
    stop_requested = 1;
}

const char *db_url = getenv("PYRITE_DB_URL");
if (db_url == NULL || db_url[0] == '\0') {
    db_url = "http://127.0.0.1:8428/api/v1/import/prometheus"; // Default
}

static void install_signal_handlers(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);

    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGHUP,  &sa, NULL);
}

static int64_t monotonic_now_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

static void sleep_ns(long ns) {
    struct timespec req, rem;

    req.tv_sec = ns / 1000000000L;
    req.tv_nsec = ns % 1000000000L;

    while (nanosleep(&req, &rem) == -1 && errno == EINTR) {
        if (stop_requested) {
            break;
        }
        req = rem;
    }
}

static void escape_influx_tag_value(const char *src, char *dst, size_t dst_size) {
    size_t di = 0;
    size_t i;

    if (dst == NULL || dst_size == 0) {
        return;
    }

    for (i = 0; src != NULL && src[i] != '\0' && di + 1 < dst_size; i++) {
        char c = src[i];

        if ((c == ',' || c == ' ' || c == '=') && di + 2 < dst_size) {
            dst[di++] = '\\';
            dst[di++] = c;
        } else if (c != '\n' && c != '\r') {
            dst[di++] = c;
        }
    }

    dst[di] = '\0';
}

static size_t discard_response_cb(char *ptr, size_t size, size_t nmemb, void *userdata) {
    (void)ptr;
    (void)userdata;
    return size * nmemb;
}

static int init_network(void) {
    CURLcode cc;

    cc = curl_global_init(CURL_GLOBAL_ALL);
    if (cc != CURLE_OK) {
        fprintf(stderr, "[Daemon] curl_global_init failed: %s\n", curl_easy_strerror(cc));
        return -1;
    }

    curl_handle = curl_easy_init();
    if (curl_handle == NULL) {
        fprintf(stderr, "[Daemon] curl_easy_init failed\n");
        curl_global_cleanup();
        return -1;
    }

    if (gethostname(daemon_hostname, sizeof(daemon_hostname)) != 0) {
        strncpy(daemon_hostname, "unknown-host", sizeof(daemon_hostname) - 1);
    }
    daemon_hostname[sizeof(daemon_hostname) - 1] = '\0';

    escape_influx_tag_value(daemon_hostname, escaped_hostname, sizeof(escaped_hostname));

    curl_headers = curl_slist_append(curl_headers, "Content-Type: text/plain; charset=utf-8");
    if (curl_headers == NULL) {
        fprintf(stderr, "[Daemon %s] curl_slist_append failed\n", daemon_hostname);
        curl_easy_cleanup(curl_handle);
        curl_handle = NULL;
        curl_global_cleanup();
        return -1;
    }

    cc = curl_easy_setopt(curl_handle, CURLOPT_URL, DB_URL);
    if (cc != CURLE_OK) goto fail;
    cc = curl_easy_setopt(curl_handle, CURLOPT_POST, 1L);
    if (cc != CURLE_OK) goto fail;
    cc = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, curl_headers);
    if (cc != CURLE_OK) goto fail;
    cc = curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT_MS, 3000L);
    if (cc != CURLE_OK) goto fail;
    cc = curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT_MS, 1000L);
    if (cc != CURLE_OK) goto fail;
    cc = curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPALIVE, 1L);
    if (cc != CURLE_OK) goto fail;
    cc = curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
    if (cc != CURLE_OK) goto fail;
    cc = curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, discard_response_cb);
    if (cc != CURLE_OK) goto fail;
    cc = curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "mpi-telemetry-daemon/3");
    if (cc != CURLE_OK) goto fail;

    return 0;

fail:
    fprintf(stderr, "[Daemon %s] curl_easy_setopt failed: %s\n",
            daemon_hostname, curl_easy_strerror(cc));

    if (curl_headers != NULL) {
        curl_slist_free_all(curl_headers);
        curl_headers = NULL;
    }
    curl_easy_cleanup(curl_handle);
    curl_handle = NULL;
    curl_global_cleanup();
    return -1;
}

static void cleanup_network(void) {
    if (curl_headers != NULL) {
        curl_slist_free_all(curl_headers);
        curl_headers = NULL;
    }

    if (curl_handle != NULL) {
        curl_easy_cleanup(curl_handle);
        curl_handle = NULL;
    }

    curl_global_cleanup();
}

static int post_payload_to_db(const char *buf, size_t len) {
    int attempt;

    if (curl_handle == NULL || buf == NULL || len == 0) {
        return 0;
    }

    for (attempt = 0; attempt < 2; attempt++) {
        CURLcode res;
        long http_code = 0;

        (void)curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, buf);
        (void)curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)len);

        res = curl_easy_perform(curl_handle);
        if (res == CURLE_OK) {
            (void)curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_code);
            if (http_code >= 200 && http_code < 300) {
                return 0;
            }

            fprintf(stderr,
                    "[Daemon %s] HTTP POST returned status %ld for %zu-byte payload\n",
                    daemon_hostname, http_code, len);
        } else {
            fprintf(stderr,
                    "[Daemon %s] HTTP POST failed (%s) for %zu-byte payload\n",
                    daemon_hostname, curl_easy_strerror(res), len);
        }

        if (attempt == 0) {
            sleep_ns(50000000L);
        }
    }

    return -1;
}

static int append_line_to_payload(const char *line, size_t line_len, size_t *offset) {
    if (line == NULL || offset == NULL || line_len == 0) {
        return 0;
    }

    if (line_len > PAYLOAD_BUFFER_SIZE) {
        fprintf(stderr,
                "[Daemon %s] Single telemetry line too large (%zu bytes), dropping\n",
                daemon_hostname, line_len);
        return -1;
    }

    if (*offset > 0 && line_len > (PAYLOAD_BUFFER_SIZE - *offset)) {
        (void)post_payload_to_db(payload_buffer, *offset);
        *offset = 0;
    }

    memcpy(payload_buffer + *offset, line, line_len);
    *offset += line_len;
    return 0;
}

static int64_t event_to_epoch_ns(const telemetry_slot_t *slot,
                                 const telemetry_event_t *ev) {
    double rel_seconds;

    if (slot == NULL || ev == NULL) {
        return 0;
    }

    rel_seconds = ev->time;
    if (rel_seconds < 0.0) {
        rel_seconds = 0.0;
    }

    return slot->unix_time_zero_ns +
           (int64_t)(rel_seconds * 1000000000.0 + 0.5);
}

static void flush_batch_to_db(const shared_telemetry_state_t *shm_state,
                              const drained_event_t *batch,
                              size_t count) {
    size_t offset = 0;
    size_t i;

    if (curl_handle == NULL || shm_state == NULL || batch == NULL || count == 0) {
        return;
    }

    for (i = 0; i < count; i++) {
        const telemetry_event_t *ev = &batch[i].event;
        const telemetry_slot_t *slot;
        int64_t ts_ns;
        char line[LINE_BUFFER_SIZE];
        int written;

        if (batch[i].slot_index < 0 ||
            batch[i].slot_index >= shm_state->num_ranks_in_subset) {
            continue;
        }

        slot = &shm_state->slots[batch[i].slot_index];
        ts_ns = event_to_epoch_ns(slot, ev);

        if (ev->is_large == 0) {
            written = snprintf(
                line, sizeof(line),
                "mpi_call,host=%s "
                "producer_world_rank=%di,producer_subset_rank=%di,"
                "message_type=%di,sender=%di,receiver=%di,"
                "bytes=%di,count=%di,id=%di,is_large=0i %" PRId64 "\n",
                escaped_hostname,
                slot->world_rank,
                slot->subset_rank,
                ev->message_type,
                ev->sender,
                ev->receiver,
                ev->bytes,
                ev->count,
                ev->id,
                ts_ns
            );
        } else {
            written = snprintf(
                line, sizeof(line),
                "mpi_call,host=%s "
                "producer_world_rank=%di,producer_subset_rank=%di,"
                "message_type=%di,sender1=%di,receiver1=%di,"
                "bytes1=%di,count1=%di,"
                "sender2=%di,receiver2=%di,bytes2=%di,count2=%di,"
                "id=%di,is_large=1i %" PRId64 "\n",
                escaped_hostname,
                slot->world_rank,
                slot->subset_rank,
                ev->message_type,
                ev->sender,
                ev->receiver,
                ev->bytes,
                ev->count,
                ev->sender2,
                ev->receiver2,
                ev->bytes2,
                ev->count2,
                ev->id,
                ts_ns
            );
        }

        if (written < 0) {
            fprintf(stderr, "[Daemon %s] snprintf failed\n", daemon_hostname);
            continue;
        }

        if ((size_t)written >= sizeof(line)) {
            fprintf(stderr,
                    "[Daemon %s] Telemetry line truncated; dropping event id=%d\n",
                    daemon_hostname, ev->id);
            continue;
        }

        (void)append_line_to_payload(line, (size_t)written, &offset);
    }

    if (offset > 0) {
        (void)post_payload_to_db(payload_buffer, offset);
    }
}

static int parent_looks_dead(pid_t parent_pid) {
    if (parent_pid <= 1) {
        return 0;
    }

    if (kill(parent_pid, 0) == 0) {
        return 0;
    }

    if (errno == ESRCH) {
        return 1;
    }

    return 0;
}

static int open_shm_with_retry(const char *shm_name, int timeout_ms) {
    int64_t deadline = monotonic_now_ns() + (int64_t)timeout_ms * 1000000LL;

    while (!stop_requested) {
        int fd = shm_open(shm_name, O_RDWR, 0666);
        if (fd != -1) {
            return fd;
        }

        if (errno != ENOENT) {
            perror("shm_open");
            return -1;
        }

        if (monotonic_now_ns() >= deadline) {
            fprintf(stderr, "[Daemon] Timed out waiting for shm '%s'\n", shm_name);
            return -1;
        }

        sleep_ns(10000000L);
    }

    return -1;
}

static int wait_until_ready(shared_telemetry_state_t *header, int timeout_ms) {
    int64_t deadline = monotonic_now_ns() + (int64_t)timeout_ms * 1000000LL;

    if (header == NULL) {
        return -1;
    }

    while (!stop_requested) {
        if (atomic_load_explicit(&header->ready, memory_order_acquire) != 0) {
            return 0;
        }

        if (monotonic_now_ns() >= deadline) {
            return -1;
        }

        sleep_ns(10000000L);
    }

    return -1;
}

static int validate_header(const shared_telemetry_state_t *hdr) {
    if (hdr == NULL) {
        return -1;
    }

    if (hdr->layout_version != SHARED_TELEMETRY_LAYOUT_VERSION) {
        fprintf(stderr,
                "[Daemon] Shared-memory layout mismatch: got %d expected %d\n",
                hdr->layout_version, SHARED_TELEMETRY_LAYOUT_VERSION);
        return -1;
    }

    if (hdr->num_ranks_in_subset <= 0) {
        fprintf(stderr,
                "[Daemon] Invalid num_ranks_in_subset=%d\n",
                hdr->num_ranks_in_subset);
        return -1;
    }

    return 0;
}

static int validate_slots(const shared_telemetry_state_t *state) {
    int i;

    if (state == NULL) {
        return -1;
    }

    for (i = 0; i < state->num_ranks_in_subset; i++) {
        if (atomic_load_explicit(&state->slots[i].anchor_ready,
                                 memory_order_acquire) == 0) {
            fprintf(stderr, "[Daemon] slot %d anchor_ready is not set\n", i);
            return -1;
        }

        if (state->slots[i].subset_rank != i) {
            fprintf(stderr,
                    "[Daemon] slot %d has mismatched subset_rank=%d\n",
                    i, state->slots[i].subset_rank);
            return -1;
        }

        if (state->slots[i].world_rank < 0) {
            fprintf(stderr,
                    "[Daemon] slot %d has invalid world_rank=%d\n",
                    i, state->slots[i].world_rank);
            return -1;
        }

        if (state->slots[i].unix_time_zero_ns <= 0) {
            fprintf(stderr,
                    "[Daemon] slot %d has invalid unix_time_zero_ns=%" PRId64 "\n",
                    i, state->slots[i].unix_time_zero_ns);
            return -1;
        }
    }

    return 0;
}

static int compute_expected_shm_size(int num_ranks, size_t *out_size) {
    size_t header_size = offsetof(shared_telemetry_state_t, slots);
    size_t slots_size;

    if (out_size == NULL || num_ranks <= 0) {
        return -1;
    }

    if ((size_t)num_ranks > (SIZE_MAX - header_size) / sizeof(telemetry_slot_t)) {
        return -1;
    }

    slots_size = (size_t)num_ranks * sizeof(telemetry_slot_t);
    *out_size = header_size + slots_size;
    return 0;
}

static size_t drain_batch_round_robin(shared_telemetry_state_t *shm_state,
                                      int num_ranks,
                                      size_t *next_ring_start,
                                      drained_event_t *batch,
                                      size_t batch_cap) {
    size_t total = 0;
    size_t pass;

    if (shm_state == NULL || next_ring_start == NULL || batch == NULL || batch_cap == 0) {
        return 0;
    }

    for (pass = 0; pass < (size_t)num_ranks && total < batch_cap; pass++) {
        size_t idx = (*next_ring_start + pass) % (size_t)num_ranks;
        spsc_ring_buffer_t *ring = &shm_state->slots[idx].ring;
        size_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
        size_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
        size_t initial_tail = tail;
        size_t taken = 0;

        while (tail != head && total < batch_cap && taken < PER_RING_BURST) {
            batch[total].event = ring->events[tail & RING_BUFFER_MASK];
            batch[total].slot_index = (int)idx;
            total++;
            tail++;
            taken++;
        }

        if (tail != initial_tail) {
            atomic_store_explicit(&ring->tail, tail, memory_order_release);
        }
    }

    *next_ring_start = (*next_ring_start + 1u) % (size_t)num_ranks;
    return total;
}

static void log_dropped_event_deltas(shared_telemetry_state_t *state,
                                     int num_ranks,
                                     size_t *last_dropped_totals) {
    int i;

    if (state == NULL || last_dropped_totals == NULL) {
        return;
    }

    for (i = 0; i < num_ranks; i++) {
        size_t total = atomic_load_explicit(&state->slots[i].ring.dropped_events,
                                            memory_order_acquire);
        if (total != last_dropped_totals[i]) {
            size_t delta = total - last_dropped_totals[i];
            fprintf(stderr,
                    "[Daemon %s] slot=%d world_rank=%d dropped_events delta=%zu total=%zu\n",
                    daemon_hostname,
                    i,
                    state->slots[i].world_rank,
                    delta,
                    total);
            last_dropped_totals[i] = total;
        }
    }
}

int main(int argc, char *argv[]) {
    const char *shm_name;
    pid_t parent_pid;
    int fd = -1;
    shared_telemetry_state_t *header_map = NULL;
    shared_telemetry_state_t *shm_state = NULL;
    size_t shm_size = 0;
    drained_event_t batch[BATCH_CAPACITY];
    size_t next_ring_start = 0;
    size_t *last_dropped_totals = NULL;
    int unlink_on_exit = 0;
    bool exit_requested = false;
    int shutdown_idle_passes = 0;
    int64_t last_parent_check_ns = 0;
    int64_t last_drop_log_ns = 0;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <shm_name> <parent_pid>\n", argv[0]);
        return 1;
    }

    shm_name = argv[1];
    parent_pid = (pid_t)strtol(argv[2], NULL, 10);

    install_signal_handlers();

#ifdef __linux__
    if (getppid() == parent_pid) {
        (void)prctl(PR_SET_PDEATHSIG, SIGTERM);
    }
#endif

    fd = open_shm_with_retry(shm_name, OPEN_RETRY_TIMEOUT_MS);
    if (fd == -1) {
        return 1;
    }

    header_map = mmap(NULL,
                      sizeof(shared_telemetry_state_t),
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED,
                      fd,
                      0);
    if (header_map == MAP_FAILED) {
        perror("mmap(header)");
        close(fd);
        return 1;
    }

    if (wait_until_ready(header_map, READY_TIMEOUT_MS) != 0) {
        fprintf(stderr, "[Daemon] Timed out waiting for shared-memory readiness\n");
        munmap(header_map, sizeof(shared_telemetry_state_t));
        close(fd);
        return 1;
    }

    if (validate_header(header_map) != 0) {
        munmap(header_map, sizeof(shared_telemetry_state_t));
        close(fd);
        return 1;
    }

    if (compute_expected_shm_size(header_map->num_ranks_in_subset, &shm_size) != 0) {
        fprintf(stderr, "[Daemon] Failed to compute shared-memory size\n");
        munmap(header_map, sizeof(shared_telemetry_state_t));
        close(fd);
        return 1;
    }

    {
        struct stat st;
        if (fstat(fd, &st) != 0) {
            perror("fstat");
            munmap(header_map, sizeof(shared_telemetry_state_t));
            close(fd);
            return 1;
        }

        if ((size_t)st.st_size < shm_size) {
            fprintf(stderr,
                    "[Daemon] Shared-memory object too small: actual=%zu expected=%zu\n",
                    (size_t)st.st_size, shm_size);
            munmap(header_map, sizeof(shared_telemetry_state_t));
            close(fd);
            return 1;
        }
    }

    shm_state = mmap(NULL,
                     shm_size,
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED,
                     fd,
                     0);
    if (shm_state == MAP_FAILED) {
        perror("mmap(full)");
        munmap(header_map, sizeof(shared_telemetry_state_t));
        close(fd);
        return 1;
    }

    munmap(header_map, sizeof(shared_telemetry_state_t));
    header_map = NULL;
    close(fd);
    fd = -1;

    if (validate_slots(shm_state) != 0) {
        munmap(shm_state, shm_size);
        return 1;
    }

    unlink_on_exit = atomic_load_explicit(&shm_state->unlink_on_exit, memory_order_acquire);

    if (init_network() != 0) {
        munmap(shm_state, shm_size);
        return 1;
    }

    last_dropped_totals = (size_t *)calloc((size_t)shm_state->num_ranks_in_subset,
                                           sizeof(size_t));
    if (last_dropped_totals == NULL) {
        fprintf(stderr, "[Daemon %s] Failed to allocate drop-tracking array\n",
                daemon_hostname);
        cleanup_network();
        munmap(shm_state, shm_size);
        return 1;
    }

    last_parent_check_ns = monotonic_now_ns();
    last_drop_log_ns = last_parent_check_ns;

    for (;;) {
        size_t batch_count = drain_batch_round_robin(
            shm_state,
            shm_state->num_ranks_in_subset,
            &next_ring_start,
            batch,
            BATCH_CAPACITY
        );

        if (batch_count > 0) {
            flush_batch_to_db(shm_state, batch, batch_count);
            shutdown_idle_passes = 0;
        }

        if (!exit_requested) {
            int daemon_running;
            int64_t now_ns = monotonic_now_ns();

            daemon_running = atomic_load_explicit(&shm_state->daemon_running,
                                                  memory_order_acquire);

            if (stop_requested || daemon_running == 0) {
                exit_requested = true;
            } else if ((now_ns - last_parent_check_ns) >= PARENT_CHECK_INTERVAL_NS) {
                if (parent_looks_dead(parent_pid)) {
                    fprintf(stderr,
                            "[Daemon %s] Parent PID %d appears dead; draining and exiting\n",
                            daemon_hostname, (int)parent_pid);
                    exit_requested = true;
                }
                last_parent_check_ns = now_ns;
            }

            if ((now_ns - last_drop_log_ns) >= DROP_LOG_INTERVAL_NS) {
                log_dropped_event_deltas(shm_state,
                                         shm_state->num_ranks_in_subset,
                                         last_dropped_totals);
                last_drop_log_ns = now_ns;
            }
        }

        if (exit_requested) {
            if (batch_count == 0) {
                shutdown_idle_passes++;
                if (shutdown_idle_passes >= SHUTDOWN_IDLE_PASSES) {
                    break;
                }
                sleep_ns(SHUTDOWN_SLEEP_NS);
            }
            continue;
        }

        if (batch_count == 0) {
            sleep_ns(NORMAL_SLEEP_NS);
        }
    }

    log_dropped_event_deltas(shm_state,
                             shm_state->num_ranks_in_subset,
                             last_dropped_totals);

    free(last_dropped_totals);
    cleanup_network();

    munmap(shm_state, shm_size);

    if (unlink_on_exit) {
        if (shm_unlink(shm_name) != 0 && errno != ENOENT) {
            perror("shm_unlink");
        }
    }

    return 0;
}

