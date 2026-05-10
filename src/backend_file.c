#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>

#include "mpi_communication_tracking.h"

/*
 * Updated .mpic format
 *
 * Header layout:
 *   char     magic[8]              = "MPICv002"
 *   uint32_t format_version        = 2
 *   int      world_size
 *   char     datetime[DATETIME_LENGTH]
 *   char     program[STRING_LENGTH]
 *   process_info_t               processes[world_size]
 *   mpic_process_time_anchor_t   anchors[world_size]
 *
 * Then the same per-rank blocks as before:
 *   int   rank
 *   char  small_label[24]
 *   int   small_count
 *   small_node_no_link_t[small_count]
 *   char  large_label[24]
 *   int   large_count
 *   large_node_no_link_t[large_count]
 */

#define MPIC_MAGIC_SIZE 8
#define MPIC_FORMAT_VERSION 2u

typedef struct {
    int rank;
    double mpi_time_zero;
    int64_t unix_time_zero_ns;
} mpic_process_time_anchor_t;

/* -------------------------------------------------------------------------- */
/* Local State                                                                */
/* -------------------------------------------------------------------------- */

static FILE *small_output_file = NULL;
static FILE *large_output_file = NULL;
static int small_count = 0;
static int large_count = 0;
static pid_t my_pid = -1;

/* Names of this rank's temp files so we can clean them up or preserve them */
static char my_small_temp_name[STRING_LENGTH] = {0};
static char my_large_temp_name[STRING_LENGTH] = {0};

/* Rank 0 global file naming */
static char final_output_name[STRING_LENGTH] = {0};
static char temp_output_name[STRING_LENGTH] = {0};

/* -------------------------------------------------------------------------- */
/* Global State (Rank 0 only)                                                 */
/* -------------------------------------------------------------------------- */

static FILE *global_file = NULL;
static process_info_t *all_processes = NULL;
static mpic_process_time_anchor_t *all_time_anchors = NULL;
static int *all_small_counts = NULL;
static int *all_large_counts = NULL;

/* -------------------------------------------------------------------------- */
/* Helpers                                                                    */
/* -------------------------------------------------------------------------- */

static int build_temp_rank_filename(char *filename,
                                    size_t filename_len,
                                    const char *host,
                                    int pid,
                                    const char *suffix) {
    int rc;

    if (filename == NULL || filename_len == 0 || suffix == NULL) {
        return -1;
    }

    rc = snprintf(filename, filename_len, ".%.450s-%.450s-%d_%s",
                  (tracking_programname[0] != '\0') ? tracking_programname : "unknown-program",
                  (host != NULL && host[0] != '\0') ? host : "unknown-host",
                  pid,
                  suffix);

    if (rc < 0 || (size_t)rc >= filename_len) {
        return -1;
    }

    return 0;
}

static int build_global_output_names(char *final_name,
                                     size_t final_name_len,
                                     char *temp_name,
                                     size_t temp_name_len,
                                     const char *program,
                                     const char *stamp,
                                     int pid) {
    int rc1, rc2;

    if (final_name == NULL || final_name_len == 0 ||
        temp_name == NULL || temp_name_len == 0 ||
        stamp == NULL) {
        return -1;
    }

    rc1 = snprintf(final_name, final_name_len, "%.995s-%s.mpic",
                   (program != NULL && program[0] != '\0') ? program : "unknown-program",
                   stamp);

    rc2 = snprintf(temp_name, temp_name_len, "%.970s.%d.tmp",
                   final_name,
                   pid);

    if (rc1 < 0 || (size_t)rc1 >= final_name_len) {
        return -1;
    }

    if (rc2 < 0 || (size_t)rc2 >= temp_name_len) {
        return -1;
    }

    return 0;
}

static void close_local_temp_files(void) {
    if (small_output_file != NULL) {
        fclose(small_output_file);
        small_output_file = NULL;
    }

    if (large_output_file != NULL) {
        fclose(large_output_file);
        large_output_file = NULL;
    }
}

static void unlink_local_temp_files(void) {
    if (my_small_temp_name[0] != '\0') {
        unlink(my_small_temp_name);
        my_small_temp_name[0] = '\0';
    }

    if (my_large_temp_name[0] != '\0') {
        unlink(my_large_temp_name);
        my_large_temp_name[0] = '\0';
    }
}

static void cleanup_global_header_state(void) {
    if (global_file != NULL) {
        fclose(global_file);
        global_file = NULL;
    }

    free(all_processes);
    all_processes = NULL;

    free(all_time_anchors);
    all_time_anchors = NULL;
}

static void cleanup_global_finalize_state(void) {
    free(all_small_counts);
    all_small_counts = NULL;

    free(all_large_counts);
    all_large_counts = NULL;
}

static void unlink_temp_global_file(void) {
    if (temp_output_name[0] != '\0') {
        unlink(temp_output_name);
        temp_output_name[0] = '\0';
    }
}

static int write_checked(const void *ptr, size_t size, size_t nmemb, FILE *out) {
    if (out == NULL) {
        return MPI_ERR_ARG;
    }

    if (nmemb == 0) {
        return MPI_SUCCESS;
    }

    if (fwrite(ptr, size, nmemb, out) != nmemb) {
        return MPI_ERR_IO;
    }

    return MPI_SUCCESS;
}

static int write_global_header(FILE *out,
                               int size,
                               const process_info_t *processes,
                               const mpic_process_time_anchor_t *anchors) {
    static const char magic[MPIC_MAGIC_SIZE] = { 'M', 'P', 'I', 'C', 'v', '0', '0', '2' };
    uint32_t format_version = MPIC_FORMAT_VERSION;
    char fixed_datetime[DATETIME_LENGTH] = {0};
    char fixed_program[STRING_LENGTH] = {0};
    int i;
    int rc;

    if (out == NULL || size < 0 || processes == NULL || anchors == NULL) {
        return MPI_ERR_ARG;
    }

    strncpy(fixed_datetime, tracking_datetime, DATETIME_LENGTH - 1);
    fixed_datetime[DATETIME_LENGTH - 1] = '\0';

    strncpy(fixed_program, tracking_programname, STRING_LENGTH - 1);
    fixed_program[STRING_LENGTH - 1] = '\0';

    rc = write_checked(magic, sizeof(char), MPIC_MAGIC_SIZE, out);
    if (rc != MPI_SUCCESS) return rc;

    rc = write_checked(&format_version, sizeof(format_version), 1, out);
    if (rc != MPI_SUCCESS) return rc;

    rc = write_checked(&size, sizeof(int), 1, out);
    if (rc != MPI_SUCCESS) return rc;

    rc = write_checked(fixed_datetime, sizeof(char), DATETIME_LENGTH, out);
    if (rc != MPI_SUCCESS) return rc;

    rc = write_checked(fixed_program, sizeof(char), STRING_LENGTH, out);
    if (rc != MPI_SUCCESS) return rc;

    for (i = 0; i < size; i++) {
        rc = write_checked(&processes[i], sizeof(process_info_t), 1, out);
        if (rc != MPI_SUCCESS) return rc;
    }

    for (i = 0; i < size; i++) {
        rc = write_checked(&anchors[i], sizeof(mpic_process_time_anchor_t), 1, out);
        if (rc != MPI_SUCCESS) return rc;
    }

    return MPI_SUCCESS;
}

static int copy_exact_records(FILE *out, const char *path, size_t record_size, int expected_count) {
    FILE *in = NULL;
    unsigned char buffer[8192];
    size_t total_bytes;
    size_t copied = 0;
    int rc = MPI_SUCCESS;

    if (out == NULL || path == NULL || record_size == 0 || expected_count < 0) {
        return MPI_ERR_ARG;
    }

    if ((size_t)expected_count > ((size_t)-1) / record_size) {
        return MPI_ERR_ARG;
    }

    total_bytes = (size_t)expected_count * record_size;

    in = fopen(path, "rb");
    if (in == NULL) {
        return MPI_ERR_IO;
    }

    while (copied < total_bytes) {
        size_t remaining = total_bytes - copied;
        size_t chunk = (remaining < sizeof(buffer)) ? remaining : sizeof(buffer);
        size_t nread = fread(buffer, 1, chunk, in);

        if (nread == 0) {
            rc = MPI_ERR_IO;
            break;
        }

        if (write_checked(buffer, 1, nread, out) != MPI_SUCCESS) {
            rc = MPI_ERR_IO;
            break;
        }

        copied += nread;
    }

    if (rc == MPI_SUCCESS) {
        unsigned char extra;
        size_t extra_read = fread(&extra, 1, 1, in);

        if (extra_read != 0) {
            rc = MPI_ERR_IO;
        } else if (ferror(in)) {
            rc = MPI_ERR_IO;
        }
    }

    fclose(in);
    return rc;
}

/* -------------------------------------------------------------------------- */
/* Backend Implementation                                                     */
/* -------------------------------------------------------------------------- */

static int file_init(int rank, int size) {
    int core = -1;
    int chip = -1;
    int root_ok = 1;
    int local_ok = 1;
    int all_ok = 1;
    process_info_t my_process;
    mpic_process_time_anchor_t my_anchor;
    char s_name[STRING_LENGTH];
    char l_name[STRING_LENGTH];

    my_pid = getpid();
    get_processor_and_core(&chip, &core);

    memset(&my_process, 0, sizeof(my_process));
    my_process.rank = rank;
    my_process.process_id = (int)my_pid;
    my_process.core = core;
    my_process.chip = chip;
    strncpy(my_process.hostname, tracking_hostname, sizeof(my_process.hostname) - 1);
    my_process.hostname[sizeof(my_process.hostname) - 1] = '\0';

    memset(&my_anchor, 0, sizeof(my_anchor));
    my_anchor.rank = rank;
    my_anchor.mpi_time_zero = tracking_start_time;
    my_anchor.unix_time_zero_ns = tracking_start_unix_ns;

    /*
     * Phase 1: rank 0 allocates global metadata buffers.
     * This must be communicated before any Gather calls.
     */
    if (rank == 0) {
        all_processes = (process_info_t *)calloc((size_t)size, sizeof(process_info_t));
        all_time_anchors = (mpic_process_time_anchor_t *)calloc((size_t)size, sizeof(mpic_process_time_anchor_t));

        if (all_processes == NULL || all_time_anchors == NULL) {
            root_ok = 0;
        }
    }

    PMPI_Bcast(&root_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!root_ok) {
        cleanup_global_header_state();
        return MPI_ERR_NO_MEM;
    }

    PMPI_Gather(&my_process, sizeof(process_info_t), MPI_BYTE,
                all_processes, sizeof(process_info_t), MPI_BYTE,
                0, MPI_COMM_WORLD);

    PMPI_Gather(&my_anchor, sizeof(mpic_process_time_anchor_t), MPI_BYTE,
                all_time_anchors, sizeof(mpic_process_time_anchor_t), MPI_BYTE,
                0, MPI_COMM_WORLD);

    /*
     * Phase 2: every rank opens its local temp files; rank 0 also opens the
     * temporary global .mpic and writes the header.
     */
    if (build_temp_rank_filename(s_name, sizeof(s_name), tracking_hostname, (int)my_pid, "small") != 0 ||
        build_temp_rank_filename(l_name, sizeof(l_name), tracking_hostname, (int)my_pid, "large") != 0) {
        local_ok = 0;
    } else {
        strncpy(my_small_temp_name, s_name, sizeof(my_small_temp_name) - 1);
        my_small_temp_name[sizeof(my_small_temp_name) - 1] = '\0';

        strncpy(my_large_temp_name, l_name, sizeof(my_large_temp_name) - 1);
        my_large_temp_name[sizeof(my_large_temp_name) - 1] = '\0';

        small_output_file = fopen(s_name, "wb");
        large_output_file = fopen(l_name, "wb");

        if (small_output_file == NULL || large_output_file == NULL) {
            local_ok = 0;
        }
    }

    if (rank == 0) {
        char stamp[DATETIME_LENGTH];
        int rc;

        get_date_time_string(stamp);

        if (build_global_output_names(final_output_name, sizeof(final_output_name),
                                      temp_output_name, sizeof(temp_output_name),
                                      tracking_programname, stamp, (int)getpid()) != 0) {
            local_ok = 0;
        } else {
            global_file = fopen(temp_output_name, "wb");
            if (global_file == NULL) {
                local_ok = 0;
            } else {
                rc = write_global_header(global_file, size, all_processes, all_time_anchors);
                if (rc != MPI_SUCCESS) {
                    local_ok = 0;
                }
            }
        }
    }

    PMPI_Allreduce(&local_ok, &all_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (!all_ok) {
        close_local_temp_files();
        unlink_local_temp_files();

        cleanup_global_header_state();

        if (rank == 0) {
            unlink_temp_global_file();
            final_output_name[0] = '\0';
        }

        return MPI_ERR_IO;
    }

    return MPI_SUCCESS;
}

static void file_record_event(telemetry_event_t *event) {
    if (event == NULL) {
        return;
    }

    if (event->is_large == 0) {
        small_node_no_link_t s;
        memset(&s, 0, sizeof(s));

        if (small_output_file == NULL) {
            return;
        }

        s.time = event->time;
        s.id = event->id;
        s.message_type = event->message_type;
        s.comm = event->comm;
        s.tag = event->tag;
        s.sender = event->sender;
        s.receiver = event->receiver;
        s.count = event->count;
        s.bytes = event->bytes;

        if (fwrite(&s, sizeof(s), 1, small_output_file) == 1) {
            small_count++;
        }
    } else {
        large_node_no_link_t l;
        memset(&l, 0, sizeof(l));

        if (large_output_file == NULL) {
            return;
        }

        l.time = event->time;
        l.id = event->id;
        l.message_type = event->message_type;
        l.comm = event->comm;
        l.sender1 = event->sender;
        l.receiver1 = event->receiver;
        l.count1 = event->count;
        l.bytes1 = event->bytes;
        l.tag1 = event->tag;
        l.sender2 = event->sender2;
        l.receiver2 = event->receiver2;
        l.count2 = event->count2;
        l.bytes2 = event->bytes2;
        l.tag2 = event->tag2;

        if (fwrite(&l, sizeof(l), 1, large_output_file) == 1) {
            large_count++;
        }
    }
}

static void file_finalize(void) {
    int counts_ok = 1;
    int merge_ok = 1;

    close_local_temp_files();

    /*
     * Rank 0 allocates merge-count arrays.
     */
    if (tracking_my_rank == 0) {
        if (global_file == NULL || all_processes == NULL || all_time_anchors == NULL) {
            counts_ok = 0;
        } else {
            all_small_counts = (int *)calloc((size_t)tracking_my_size, sizeof(int));
            all_large_counts = (int *)calloc((size_t)tracking_my_size, sizeof(int));

            if (all_small_counts == NULL || all_large_counts == NULL) {
                counts_ok = 0;
            }
        }
    }

    PMPI_Bcast(&counts_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!counts_ok) {
        cleanup_global_finalize_state();
        cleanup_global_header_state();

        /*
         * Preserve local temp files on finalize failure so data can be recovered.
         */
        if (tracking_my_rank == 0) {
            unlink_temp_global_file();
            final_output_name[0] = '\0';
        }
        return;
    }

    PMPI_Gather(&small_count, 1, MPI_INT,
                all_small_counts, 1, MPI_INT,
                0, MPI_COMM_WORLD);

    PMPI_Gather(&large_count, 1, MPI_INT,
                all_large_counts, 1, MPI_INT,
                0, MPI_COMM_WORLD);

    /*
     * Rank 0 performs the merge while all other ranks wait for the broadcast
     * below. This avoids the race where non-root ranks unlink temp files too
     * early.
     */
    if (tracking_my_rank == 0) {
        static const char small_label[24] = "P2P Small Type Messages";
        static const char large_label[24] = "P2P Large Type Messages";
        int i;

        for (i = 0; i < tracking_my_size && merge_ok; i++) {
            char s_name[STRING_LENGTH];
            char l_name[STRING_LENGTH];
            int rc;

            if (build_temp_rank_filename(s_name, sizeof(s_name),
                                         all_processes[i].hostname,
                                         all_processes[i].process_id,
                                         "small") != 0 ||
                build_temp_rank_filename(l_name, sizeof(l_name),
                                         all_processes[i].hostname,
                                         all_processes[i].process_id,
                                         "large") != 0) {
                merge_ok = 0;
                break;
            }

            /* Small block header */
            rc = write_checked(&all_processes[i].rank, sizeof(int), 1, global_file);
            if (rc != MPI_SUCCESS) { merge_ok = 0; break; }

            rc = write_checked(small_label, sizeof(char), 24, global_file);
            if (rc != MPI_SUCCESS) { merge_ok = 0; break; }

            rc = write_checked(&all_small_counts[i], sizeof(int), 1, global_file);
            if (rc != MPI_SUCCESS) { merge_ok = 0; break; }

            rc = copy_exact_records(global_file, s_name,
                                    sizeof(small_node_no_link_t),
                                    all_small_counts[i]);
            if (rc != MPI_SUCCESS) { merge_ok = 0; break; }

            /* Large block header */
            rc = write_checked(large_label, sizeof(char), 24, global_file);
            if (rc != MPI_SUCCESS) { merge_ok = 0; break; }

            rc = write_checked(&all_large_counts[i], sizeof(int), 1, global_file);
            if (rc != MPI_SUCCESS) { merge_ok = 0; break; }

            rc = copy_exact_records(global_file, l_name,
                                    sizeof(large_node_no_link_t),
                                    all_large_counts[i]);
            if (rc != MPI_SUCCESS) { merge_ok = 0; break; }
        }

        if (merge_ok) {
            if (fflush(global_file) != 0) {
                merge_ok = 0;
            }
        }

        if (global_file != NULL) {
            if (fclose(global_file) != 0) {
                merge_ok = 0;
            }
            global_file = NULL;
        }

        if (merge_ok) {
            if (rename(temp_output_name, final_output_name) != 0) {
                merge_ok = 0;
            } else {
                temp_output_name[0] = '\0';
            }
        }

        if (!merge_ok) {
            unlink_temp_global_file();
            final_output_name[0] = '\0';
        }
    }

    PMPI_Bcast(&merge_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /*
     * Only after rank 0 is done merging is it safe for every rank to unlink its
     * local temp files.
     *
     * On merge failure, preserve local temp files for debugging/recovery.
     */
    if (merge_ok) {
        unlink_local_temp_files();
    }

    cleanup_global_finalize_state();
    cleanup_global_header_state();
}

static tracking_backend_t file_backend = {
    .init_backend = file_init,
    .record_event = file_record_event,
    .finalize_backend = file_finalize
};

__attribute__((constructor))
static void register_file_backend(void) {
    register_tracking_backend(&file_backend);
}

