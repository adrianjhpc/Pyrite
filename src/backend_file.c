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
 *   process_info_t      processes[world_size]
 *   process_time_anchor_t anchors[world_size]
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
} process_time_anchor_t;

/* Local State */
static FILE *small_output_file = NULL;
static FILE *large_output_file = NULL;
static int small_count = 0;
static int large_count = 0;
static pid_t my_pid = -1;

/* Names of this rank's temp files so we can clean them up on failure */
static char my_small_temp_name[STRING_LENGTH] = {0};
static char my_large_temp_name[STRING_LENGTH] = {0};

/* Global State (Rank 0 only) */
static FILE *global_file = NULL;
static process_info_t *all_processes = NULL;
static process_time_anchor_t *all_time_anchors = NULL;
static int *all_small_counts = NULL;
static int *all_large_counts = NULL;

static void get_temp_filename(char *filename, const char *host, int pid, const char *suffix) {
    snprintf(filename, STRING_LENGTH, ".%.450s-%.450s-%d_%s",
             (tracking_programname[0] != '\0') ? tracking_programname : "unknown-program",
             (host != NULL && host[0] != '\0') ? host : "unknown-host",
             pid, suffix);
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

static int write_global_header(FILE *out,
                               int size,
                               const process_info_t *processes,
                               const process_time_anchor_t *anchors) {
    static const char magic[MPIC_MAGIC_SIZE] = { 'M', 'P', 'I', 'C', 'v', '0', '0', '2' };
    uint32_t format_version = MPIC_FORMAT_VERSION;
    char fixed_datetime[DATETIME_LENGTH] = {0};
    char fixed_program[STRING_LENGTH] = {0};
    int i;

    if (out == NULL || size < 0 || processes == NULL || anchors == NULL) {
        return MPI_ERR_ARG;
    }

    strncpy(fixed_datetime, tracking_datetime, DATETIME_LENGTH - 1);
    strncpy(fixed_program, tracking_programname, STRING_LENGTH - 1);

    if (fwrite(magic, sizeof(char), MPIC_MAGIC_SIZE, out) != MPIC_MAGIC_SIZE) {
        return MPI_ERR_IO;
    }

    if (fwrite(&format_version, sizeof(format_version), 1, out) != 1) {
        return MPI_ERR_IO;
    }

    if (fwrite(&size, sizeof(int), 1, out) != 1) {
        return MPI_ERR_IO;
    }

    if (fwrite(fixed_datetime, sizeof(char), DATETIME_LENGTH, out) != DATETIME_LENGTH) {
        return MPI_ERR_IO;
    }

    if (fwrite(fixed_program, sizeof(char), STRING_LENGTH, out) != STRING_LENGTH) {
        return MPI_ERR_IO;
    }

    for (i = 0; i < size; i++) {
        if (fwrite(&processes[i], sizeof(process_info_t), 1, out) != 1) {
            return MPI_ERR_IO;
        }
    }

    for (i = 0; i < size; i++) {
        if (fwrite(&anchors[i], sizeof(process_time_anchor_t), 1, out) != 1) {
            return MPI_ERR_IO;
        }
    }

    return MPI_SUCCESS;
}

static int file_init(int rank, int size) {
    int core = -1, chip = -1;
    int init_ok = 1;
    process_info_t my_process;
    process_time_anchor_t my_anchor;
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

    memset(&my_anchor, 0, sizeof(my_anchor));
    my_anchor.rank = rank;
    my_anchor.mpi_time_zero = tracking_start_time;
    my_anchor.unix_time_zero_ns = tracking_start_unix_ns;

    if (rank == 0) {
        all_processes = (process_info_t *)calloc((size_t)size, sizeof(process_info_t));
        all_time_anchors = (process_time_anchor_t *)calloc((size_t)size, sizeof(process_time_anchor_t));

        if (all_processes == NULL || all_time_anchors == NULL) {
            init_ok = 0;
        }
    }

    PMPI_Bcast(&init_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!init_ok) {
        cleanup_global_header_state();
        return MPI_ERR_NO_MEM;
    }

    PMPI_Gather(&my_process, sizeof(process_info_t), MPI_BYTE,
                all_processes, sizeof(process_info_t), MPI_BYTE,
                0, MPI_COMM_WORLD);

    PMPI_Gather(&my_anchor, sizeof(process_time_anchor_t), MPI_BYTE,
                all_time_anchors, sizeof(process_time_anchor_t), MPI_BYTE,
                0, MPI_COMM_WORLD);

    get_temp_filename(s_name, tracking_hostname, my_pid, "small");
    get_temp_filename(l_name, tracking_hostname, my_pid, "large");

    strncpy(my_small_temp_name, s_name, sizeof(my_small_temp_name) - 1);
    strncpy(my_large_temp_name, l_name, sizeof(my_large_temp_name) - 1);

    small_output_file = fopen(s_name, "wb");
    large_output_file = fopen(l_name, "wb");

    if (small_output_file == NULL || large_output_file == NULL) {
        init_ok = 0;
    }

    if (rank == 0) {
        char stamp[DATETIME_LENGTH];
        char final_name[STRING_LENGTH];
        int rc;

        get_date_time_string(stamp);
        snprintf(final_name, sizeof(final_name), "%.995s-%s.mpic", tracking_programname, stamp);

        global_file = fopen(final_name, "wb");
        if (global_file == NULL) {
            init_ok = 0;
        } else {
            rc = write_global_header(global_file, size, all_processes, all_time_anchors);
            if (rc != MPI_SUCCESS) {
                init_ok = 0;
            }
        }
    }

    PMPI_Bcast(&init_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!init_ok) {
        close_local_temp_files();
        unlink_local_temp_files();
        cleanup_global_header_state();
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

        if (small_output_file == NULL) {
            return;
        }

        s.time = event->time;
        s.id = event->id;
        s.message_type = event->message_type;
        s.sender = event->sender;
        s.receiver = event->receiver;
        s.count = event->count;
        s.bytes = event->bytes;

        if (fwrite(&s, sizeof(s), 1, small_output_file) == 1) {
            small_count++;
        }
    } else {
        large_node_no_link_t l;

        if (large_output_file == NULL) {
            return;
        }

        l.time = event->time;
        l.id = event->id;
        l.message_type = event->message_type;
        l.sender1 = event->sender;
        l.receiver1 = event->receiver;
        l.count1 = event->count;
        l.bytes1 = event->bytes;
        l.sender2 = event->sender2;
        l.receiver2 = event->receiver2;
        l.count2 = event->count2;
        l.bytes2 = event->bytes2;

        if (fwrite(&l, sizeof(l), 1, large_output_file) == 1) {
            large_count++;
        }
    }
}

static void file_finalize(void) {
    int counts_ok = 1;

    close_local_temp_files();

    if (tracking_my_rank == 0) {
        all_small_counts = (int *)calloc((size_t)tracking_my_size, sizeof(int));
        all_large_counts = (int *)calloc((size_t)tracking_my_size, sizeof(int));

        if (all_small_counts == NULL || all_large_counts == NULL) {
            counts_ok = 0;
        }
    }

    PMPI_Bcast(&counts_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!counts_ok) {
        free(all_small_counts);
        all_small_counts = NULL;

        free(all_large_counts);
        all_large_counts = NULL;

        cleanup_global_header_state();
        unlink_local_temp_files();
        return;
    }

    PMPI_Gather(&small_count, 1, MPI_INT, all_small_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);
    PMPI_Gather(&large_count, 1, MPI_INT, all_large_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (tracking_my_rank == 0 && global_file != NULL && all_processes != NULL) {
        static const char small_label[24] = "P2P Small Type Messages";
        static const char large_label[24] = "P2P Large Type Messages";
        int i;

        for (i = 0; i < tracking_my_size; i++) {
            char s_name[STRING_LENGTH];
            char l_name[STRING_LENGTH];
            FILE *s_in = NULL;
            FILE *l_in = NULL;

            get_temp_filename(s_name, all_processes[i].hostname, all_processes[i].process_id, "small");
            get_temp_filename(l_name, all_processes[i].hostname, all_processes[i].process_id, "large");

            /* Small block */
            fwrite(&all_processes[i].rank, sizeof(int), 1, global_file);
            fwrite(small_label, sizeof(char), 24, global_file);
            fwrite(&all_small_counts[i], sizeof(int), 1, global_file);

            s_in = fopen(s_name, "rb");
            if (s_in != NULL) {
                small_node_no_link_t temp_s;
                while (fread(&temp_s, sizeof(temp_s), 1, s_in) == 1) {
                    fwrite(&temp_s, sizeof(temp_s), 1, global_file);
                }
                fclose(s_in);
            }
            unlink(s_name);

            /* Large block */
            fwrite(large_label, sizeof(char), 24, global_file);
            fwrite(&all_large_counts[i], sizeof(int), 1, global_file);

            l_in = fopen(l_name, "rb");
            if (l_in != NULL) {
                large_node_no_link_t temp_l;
                while (fread(&temp_l, sizeof(temp_l), 1, l_in) == 1) {
                    fwrite(&temp_l, sizeof(temp_l), 1, global_file);
                }
                fclose(l_in);
            }
            unlink(l_name);
        }

        fclose(global_file);
        global_file = NULL;
    } else {
        unlink_local_temp_files();
    }

    free(all_processes);
    all_processes = NULL;

    free(all_time_anchors);
    all_time_anchors = NULL;

    free(all_small_counts);
    all_small_counts = NULL;

    free(all_large_counts);
    all_large_counts = NULL;
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

