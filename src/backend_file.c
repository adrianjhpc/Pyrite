#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mpi_communication_tracking.h"

// Local State
static FILE *small_output_file = NULL;
static FILE *large_output_file = NULL;
static int small_count = 0;
static int large_count = 0;
static pid_t my_pid = -1;

// Global State (Rank 0 only)
static FILE *global_file = NULL;
static process_info_t *all_processes = NULL;
static int *all_small_counts = NULL;
static int *all_large_counts = NULL;

static void get_temp_filename(char *filename, const char *host, int pid, const char *suffix) {
    snprintf(filename, STRING_LENGTH, ".%.450s-%.450s-%d_%s", 
             (tracking_programname[0] != '\0') ? tracking_programname : "unknown-program",
             (host != NULL && host[0] != '\0') ? host : "unknown-host",
             pid, suffix);
}

static int file_init(int rank, int size) {
    int core = -1, chip = -1;
    my_pid = getpid();
    get_processor_and_core(&chip, &core);

    // Gather hardware topology to Rank 0
    process_info_t my_process;
    memset(&my_process, 0, sizeof(my_process));
    my_process.rank = rank;
    my_process.process_id = (int)my_pid;
    my_process.core = core;
    my_process.chip = chip;
    strncpy(my_process.hostname, tracking_hostname, sizeof(my_process.hostname) - 1);

    if (rank == 0) {
        all_processes = (process_info_t *)calloc((size_t)size, sizeof(process_info_t));
        if (!all_processes) return MPI_ERR_NO_MEM;
    }

    PMPI_Gather(&my_process, sizeof(process_info_t), MPI_BYTE,
                all_processes, sizeof(process_info_t), MPI_BYTE,
                0, MPI_COMM_WORLD);

    // Open local temporary files for writing
    char s_name[STRING_LENGTH], l_name[STRING_LENGTH];
    get_temp_filename(s_name, tracking_hostname, my_pid, "small");
    get_temp_filename(l_name, tracking_hostname, my_pid, "large");

    small_output_file = fopen(s_name, "wb");
    large_output_file = fopen(l_name, "wb");

    // Rank 0 opens the final global .mpic file and writes the header
    if (rank == 0) {
        char final_name[STRING_LENGTH];
        snprintf(final_name, sizeof(final_name), "%.995s-%s.mpic", tracking_programname, tracking_datetime);
        global_file = fopen(final_name, "wb");
        
        if (global_file) {
            char fixed_datetime[DATETIME_LENGTH] = {0};
            char fixed_program[STRING_LENGTH] = {0};
            strncpy(fixed_datetime, tracking_datetime, DATETIME_LENGTH - 1);
            strncpy(fixed_program, tracking_programname, STRING_LENGTH - 1);

            fwrite(&size, sizeof(int), 1, global_file);
            fwrite(fixed_datetime, sizeof(char), DATETIME_LENGTH, global_file);
            fwrite(fixed_program, sizeof(char), STRING_LENGTH, global_file);
            
            for (int i = 0; i < size; i++) {
                fwrite(&all_processes[i], sizeof(process_info_t), 1, global_file);
            }
        }
    }

    return MPI_SUCCESS;
}

static void file_record_event(telemetry_event_t *event) {
    if (event->is_large == 0) {
        if (!small_output_file) return;
        small_node_no_link_t s = {
            .time = event->time, .id = event->id, .message_type = event->message_type,
            .sender = event->sender, .receiver = event->receiver, 
            .count = event->count, .bytes = event->bytes
        };
        fwrite(&s, sizeof(small_node_no_link_t), 1, small_output_file);
        small_count++;
    } else {
        if (!large_output_file) return;
        large_node_no_link_t l = {
            .time = event->time, .id = event->id, .message_type = event->message_type,
            .sender1 = event->sender, .receiver1 = event->receiver, .count1 = event->count, .bytes1 = event->bytes,
            .sender2 = event->sender2, .receiver2 = event->receiver2, .count2 = event->count2, .bytes2 = event->bytes2
        };
        fwrite(&l, sizeof(large_node_no_link_t), 1, large_output_file);
        large_count++;
    }
}

static void file_finalize(void) {
    // Close local temporary files
    if (small_output_file) fclose(small_output_file);
    if (large_output_file) fclose(large_output_file);

    int counts_ok = 1;
    // Gather total message counts to Rank 0
    if (tracking_my_rank == 0) {
        all_small_counts = (int *)calloc((size_t)tracking_my_size, sizeof(int));
        all_large_counts = (int *)calloc((size_t)tracking_my_size, sizeof(int));
        if (!all_small_counts || !all_large_counts) counts_ok = 0;
    }

    // Abort if rank 0 didn't manage to allocate required storage
    PMPI_Bcast(&counts_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!counts_ok) {
        free(all_small_counts);
        free(all_large_counts);
        return;
    }

    PMPI_Gather(&small_count, 1, MPI_INT, all_small_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);
    PMPI_Gather(&large_count, 1, MPI_INT, all_large_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // Rank 0 concatenates all temp files into the global .mpic file
    if (tracking_my_rank == 0 && global_file != NULL && all_processes != NULL) {
        static const char small_label[24] = "P2P Small Type Messages";
        static const char large_label[24] = "P2P Large Type Messages";

        for (int i = 0; i < tracking_my_size; i++) {
            char s_name[STRING_LENGTH], l_name[STRING_LENGTH];
            get_temp_filename(s_name, all_processes[i].hostname, all_processes[i].process_id, "small");
            get_temp_filename(l_name, all_processes[i].hostname, all_processes[i].process_id, "large");

            // Write small messages block
            fwrite(&all_processes[i].rank, sizeof(int), 1, global_file);
            fwrite(small_label, sizeof(char), 24, global_file);
            fwrite(&all_small_counts[i], sizeof(int), 1, global_file);

            FILE *s_in = fopen(s_name, "rb");
            if (s_in) {
                small_node_no_link_t temp_s;
                while (fread(&temp_s, sizeof(temp_s), 1, s_in) == 1) {
                    fwrite(&temp_s, sizeof(temp_s), 1, global_file);
                }
                fclose(s_in);
            }
            unlink(s_name); // Delete temp file

            // Write large messages block
            fwrite(large_label, sizeof(char), 24, global_file);
            fwrite(&all_large_counts[i], sizeof(int), 1, global_file);

            FILE *l_in = fopen(l_name, "rb");
            if (l_in) {
                large_node_no_link_t temp_l;
                while (fread(&temp_l, sizeof(temp_l), 1, l_in) == 1) {
                    fwrite(&temp_l, sizeof(temp_l), 1, global_file);
                }
                fclose(l_in);
            }
            unlink(l_name); // Delete temp file
        }

        fclose(global_file);
    }

    // Cleanup
    free(all_processes);
    free(all_small_counts);
    free(all_large_counts);
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
