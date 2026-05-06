#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <curl/curl.h>
#include <signal.h> 
#include <errno.h>   

#include "shared_telemetry.h"

// Central database endpoint (InfluxDB Line Protocol format)
#define DB_URL "http://192.168.1.100:8428/write"

// Pre-allocate a 2MB buffer for the text payload so we don't malloc in the hot loop
static char payload_buffer[2 * 1024 * 1024]; 
static char hostname[256];

// Global cURL handle
static CURL *curl_handle = NULL;

void init_network() {
    curl_global_init(CURL_GLOBAL_ALL);
    curl_handle = curl_easy_init();
    gethostname(hostname, sizeof(hostname));
    
    if (curl_handle) {
        curl_easy_setopt(curl_handle, CURLOPT_URL, DB_URL);
        // Optimize cURL for high-throughput background streaming
        curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 3L); 
        curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPALIVE, 1L);
    }
}

void cleanup_network() {
    if (curl_handle) curl_easy_cleanup(curl_handle);
    curl_global_cleanup();
}

void flush_batch_to_db(telemetry_event_t *batch, size_t count) {
    if (!curl_handle || count == 0) return;

    size_t offset = 0;
    
    for (size_t i = 0; i < count; i++) {
        telemetry_event_t *ev = &batch[i];
        
        // Convert MPI_Wtime (seconds) into rough nanoseconds for the DB
        long long ts_nanos = (long long)(ev->time * 1000000000.0);
        
        // Format: measurement,tags fields timestamp
        if (ev->is_large == 0) {
            offset += snprintf(payload_buffer + offset, sizeof(payload_buffer) - offset,
                "mpi_call,host=%s,type=%d,sender=%d,receiver=%d bytes=%di,count=%di %lld\n",
                hostname, ev->message_type, ev->sender, ev->receiver, 
                ev->bytes, ev->count, ts_nanos);
        } else {
            // Large/Collective event formatting
            offset += snprintf(payload_buffer + offset, sizeof(payload_buffer) - offset,
                "mpi_call,host=%s,type=%d,sender1=%d,receiver1=%d bytes1=%di,count1=%di,bytes2=%di,count2=%di %lld\n",
                hostname, ev->message_type, ev->sender, ev->receiver, 
                ev->bytes, ev->count, ev->bytes2, ev->count2, ts_nanos);
        }

        // If we are nearing the 2MB buffer limit or if this is the final event in the batch:
        // Flush to the network and reset the buffer.
        if (offset >= sizeof(payload_buffer) - 512 || i == count - 1) {
            curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, payload_buffer);
            curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, (long)offset);

            CURLcode res = curl_easy_perform(curl_handle);
            if (res != CURLE_OK) {
                fprintf(stderr, "[Daemon %s] HTTP Post failed: %s\n", hostname, curl_easy_strerror(res));
            }

            // Reset the offset to start filling the buffer from the beginning again
            offset = 0;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <shm_name> <parent_pid>\n", argv[0]);
        return 1;
    }

    const char *shm_name = argv[1];
    pid_t parent_pid = (pid_t)atoi(argv[2]); // Parse the parent PID

    usleep(100000); // Wait for MPI app to finish initialization

    int fd = shm_open(shm_name, O_RDWR, 0666);
    if (fd == -1) return 1;

    // Map memory to get subset bounds
    shared_telemetry_state_t *temp_state = mmap(NULL, sizeof(shared_telemetry_state_t), PROT_READ, MAP_SHARED, fd, 0);
    int num_ranks = temp_state->num_ranks_in_subset;
    munmap(temp_state, sizeof(shared_telemetry_state_t));

    size_t shm_size = sizeof(shared_telemetry_state_t) + (num_ranks * sizeof(spsc_ring_buffer_t));
    shared_telemetry_state_t *shm_state = mmap(NULL, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    init_network();

    struct timespec sleep_time = {0, 100000000}; // 100ms
    telemetry_event_t batch[4096];

    time_t last_heartbeat_check = time(NULL);

    while (atomic_load_explicit(&shm_state->daemon_running, memory_order_acquire)) {
        size_t total_batch_size = 0;

        for (int i = 0; i < num_ranks; i++) {
            spsc_ring_buffer_t *ring = &shm_state->buffers[i];
            size_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
            size_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);

            while (tail != head && total_batch_size < 4096) {
                batch[total_batch_size++] = ring->events[tail & RING_BUFFER_MASK];
                tail++;
            }

            if (total_batch_size > 0) {
                atomic_store_explicit(&ring->tail, tail, memory_order_release);
            }
        }

        // Check the parent process is still alive (every 2 seconds) ---
        time_t now = time(NULL);
        if (now - last_heartbeat_check >= 2) {
            // kill(pid, 0) returns -1 with errno ESRCH if the process no longer exists
            if (kill(parent_pid, 0) == -1 && errno == ESRCH) {
                fprintf(stderr, "[Daemon %s] Parent PID %d died unexpectedly. Initiating emergency telemetry flush.\n", hostname, parent_pid);
                
                // Break out of the while loop. This triggers the final flush 
                // and cleans up the memory, preventing a zombie daemon
                break; 
            }
            last_heartbeat_check = now;
        }

        if (total_batch_size > 0) {
            flush_batch_to_db(batch, total_batch_size);
        } else {
            nanosleep(&sleep_time, NULL);
        }
    }

    // Flush any remaining data on shutdown
    cleanup_network();
    munmap(shm_state, shm_size);
    shm_unlink(shm_name); 
    return 0;
}
