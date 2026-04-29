#include <sys/types.h>
#include <unistd.h>
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <assert.h>

#if defined(__aarch64__)
#include <sys/syscall.h>
#endif

#include "mpi_communication_tracking.h"

/* -------------------------------------------------------------------------- */
/* Global state                                                               */
/* -------------------------------------------------------------------------- */

static double my_time = 0.0;
static int my_rank = -1;
static int my_size = 0;

static small_node_t *small_head = NULL;
static small_node_t *small_current = NULL;
static int small_current_length = 0;
static int small_total_length = 0;
static int *number_of_small_messages = NULL;

static large_node_t *large_head = NULL;
static large_node_t *large_current = NULL;
static int large_current_length = 0;
static int large_total_length = 0;
static int *number_of_large_messages = NULL;

static int current_id = 0;

static pid_t process_id = (pid_t)-1;
static char hostname[STRING_LENGTH] = {0};
static char programname[STRING_LENGTH] = {0};
static char datetime[DATETIME_LENGTH] = {0};

static FILE *global_file = NULL;
static FILE *small_output_file = NULL;
static FILE *large_output_file = NULL;

static process_info_t *processes = NULL;
static MPI_Group world_group = MPI_GROUP_NULL;

static int tracking_initialized = 0;

/* -------------------------------------------------------------------------- */
/* Internal helpers                                                           */
/* -------------------------------------------------------------------------- */

static int c_status_is_ignore(MPI_Status *status) {
    return (status == MPI_STATUS_IGNORE || status == NULL);
}

static int safe_mul_to_int(int a, int b) {
    long long v;

    if (a <= 0 || b <= 0) {
        return 0;
    }

    v = (long long)a * (long long)b;
    if (v > INT_MAX) {
        return INT_MAX;
    }

    return (int)v;
}

static int datatype_nbytes(int count, MPI_Datatype datatype) {
    int type_size = 0;

    if (count <= 0 || datatype == MPI_DATATYPE_NULL) {
        return 0;
    }

    if (PMPI_Type_size(datatype, &type_size) != MPI_SUCCESS || type_size <= 0) {
        return 0;
    }

    return safe_mul_to_int(count, type_size);
}

static double trace_timestamp(void) {
    return PMPI_Wtime() - my_time;
}

static int check_data_limit(void) {
    int limit = get_data_limit();
    return (small_current_length >= limit || large_current_length >= limit);
}

static void free_small_list_nodes(void) {
    small_node_t *cur, *next;

    if (small_head == NULL) {
        return;
    }

    cur = small_head->next;
    while (cur != NULL) {
        next = cur->next;
        free(cur);
        cur = next;
    }

    small_head->next = NULL;
    small_current = small_head;
    small_current_length = 0;
}

static void free_large_list_nodes(void) {
    large_node_t *cur, *next;

    if (large_head == NULL) {
        return;
    }

    cur = large_head->next;
    while (cur != NULL) {
        next = cur->next;
        free(cur);
        cur = next;
    }

    large_head->next = NULL;
    large_current = large_head;
    large_current_length = 0;
}

static void destroy_trace_buffers(void) {
    free_small_list_nodes();
    free_large_list_nodes();

    free(small_head);
    free(large_head);

    small_head = NULL;
    small_current = NULL;
    large_head = NULL;
    large_current = NULL;
}

static int init_trace_buffers(void) {
    small_head = (small_node_t *)calloc(1, sizeof(small_node_t));
    if (small_head == NULL) {
        return MPI_ERR_NO_MEM;
    }

    small_head->time = 0.0;
    small_head->id = -1;
    small_head->message_type = -1;
    small_head->sender = -1;
    small_head->receiver = -1;
    small_head->count = -1;
    small_head->bytes = -1;
    small_head->next = NULL;

    small_current = small_head;
    small_current_length = 0;
    small_total_length = 0;

    large_head = (large_node_t *)calloc(1, sizeof(large_node_t));
    if (large_head == NULL) {
        free(small_head);
        small_head = NULL;
        small_current = NULL;
        return MPI_ERR_NO_MEM;
    }

    large_head->time = 0.0;
    large_head->id = -1;
    large_head->message_type = -1;
    large_head->sender1 = -1;
    large_head->receiver1 = -1;
    large_head->count1 = -1;
    large_head->bytes1 = -1;
    large_head->sender2 = -1;
    large_head->receiver2 = -1;
    large_head->count2 = -1;
    large_head->bytes2 = -1;
    large_head->next = NULL;

    large_current = large_head;
    large_current_length = 0;
    large_total_length = 0;

    current_id = 0;

    return MPI_SUCCESS;
}

static void cleanup_failed_init(void) {
    if (world_group != MPI_GROUP_NULL) {
        PMPI_Group_free(&world_group);
        world_group = MPI_GROUP_NULL;
    }

    if (global_file != NULL) {
        close_global_file();
    }

    if (small_output_file != NULL || large_output_file != NULL) {
        close_data_file();
    }

    free(processes);
    processes = NULL;

    destroy_trace_buffers();

    tracking_initialized = 0;
}

static int begin_tracking_runtime(void) {
    int err = MPI_SUCCESS;

    err = PMPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    if (err != MPI_SUCCESS) {
        cleanup_failed_init();
        return err;
    }

    err = PMPI_Comm_size(MPI_COMM_WORLD, &my_size);
    if (err != MPI_SUCCESS) {
        cleanup_failed_init();
        return err;
    }

    /* Establish a roughly common time origin */
    err = PMPI_Barrier(MPI_COMM_WORLD);
    if (err != MPI_SUCCESS) {
        cleanup_failed_init();
        return err;
    }

    my_time = PMPI_Wtime();

    get_program_name();
    get_process_id();

    if (gethostname(hostname, sizeof(hostname)) != 0) {
        strncpy(hostname, "unknown-host", sizeof(hostname) - 1);
    }
    hostname[sizeof(hostname) - 1] = '\0';

    get_datetime();

    err = init_trace_buffers();
    if (err != MPI_SUCCESS) {
        cleanup_failed_init();
        return err;
    }

    if (open_data_files() != 0) {
        cleanup_failed_init();
        return MPI_ERR_IO;
    }

    if (my_rank == 0) {
        if (open_global_file() != 0) {
            cleanup_failed_init();
            return MPI_ERR_IO;
        }
    }

    err = PMPI_Comm_group(MPI_COMM_WORLD, &world_group);
    if (err != MPI_SUCCESS) {
        cleanup_failed_init();
        return err;
    }

    if (gather_process_information() != 0) {
        cleanup_failed_init();
        return MPI_ERR_OTHER;
    }

    if (my_rank == 0) {
        if (write_global_information() != 0) {
            cleanup_failed_init();
            return MPI_ERR_IO;
        }
    }

    tracking_initialized = 1;
    return MPI_SUCCESS;
}

static int translate_comm_rank_to_world(MPI_Comm comm,
                                        int comm_rank,
                                        int use_remote_group,
                                        int *world_rank_out) {
    int rc;
    int translated = MPI_UNDEFINED;
    int input_rank = comm_rank;
    int is_inter = 0;
    MPI_Group from_group = MPI_GROUP_NULL;

    if (world_rank_out == NULL) {
        return MPI_ERR_ARG;
    }

    if (comm_rank == MPI_PROC_NULL || comm_rank == MPI_ANY_SOURCE || comm_rank == MPI_ROOT) {
        *world_rank_out = comm_rank;
        return MPI_SUCCESS;
    }

    if (comm == MPI_COMM_WORLD) {
        *world_rank_out = comm_rank;
        return MPI_SUCCESS;
    }

    if (comm == MPI_COMM_NULL || world_group == MPI_GROUP_NULL) {
        *world_rank_out = comm_rank;
        return MPI_SUCCESS;
    }

    rc = PMPI_Comm_test_inter(comm, &is_inter);
    if (rc != MPI_SUCCESS) {
        *world_rank_out = comm_rank;
        return rc;
    }

    if (is_inter && use_remote_group) {
        rc = PMPI_Comm_remote_group(comm, &from_group);
    } else {
        rc = PMPI_Comm_group(comm, &from_group);
    }

    if (rc != MPI_SUCCESS || from_group == MPI_GROUP_NULL) {
        *world_rank_out = comm_rank;
        return rc;
    }

    rc = PMPI_Group_translate_ranks(from_group, 1, &input_rank, world_group, &translated);
    PMPI_Group_free(&from_group);

    if (rc != MPI_SUCCESS || translated == MPI_UNDEFINED) {
        *world_rank_out = comm_rank;
    } else {
        *world_rank_out = translated;
    }

    return rc;
}

static int current_world_rank_in_comm(MPI_Comm comm, int *world_rank_out) {
    int local_rank = 0;
    int rc;

    if (world_rank_out == NULL) {
        return MPI_ERR_ARG;
    }

    if (comm == MPI_COMM_WORLD || comm == MPI_COMM_NULL) {
        *world_rank_out = my_rank;
        return MPI_SUCCESS;
    }

    rc = PMPI_Comm_rank(comm, &local_rank);
    if (rc != MPI_SUCCESS) {
        *world_rank_out = my_rank;
        return rc;
    }

    return translate_comm_rank_to_world(comm, local_rank, 0, world_rank_out);
}

static void add_small_data_at(double temp_time,
                              int message_type,
                              int sender,
                              int receiver,
                              int count,
                              MPI_Datatype datatype) {
    small_node_t *node;

    if (!tracking_initialized || small_current == NULL) {
        return;
    }

    node = (small_node_t *)malloc(sizeof(small_node_t));
    if (node == NULL) {
        return;
    }

    node->time = temp_time;
    node->id = current_id;
    node->message_type = message_type;
    node->sender = sender;
    node->receiver = receiver;
    node->count = count;
    node->bytes = datatype_nbytes(count, datatype);
    node->next = NULL;

    small_current->next = node;
    small_current = node;
    small_current_length++;
    current_id++;

    if (check_data_limit()) {
        write_data_output();
    }
}

static void add_large_data_at(double temp_time,
                              int message_type,
                              int sender1,
                              int receiver1,
                              int count1,
                              MPI_Datatype datatype1,
                              int sender2,
                              int receiver2,
                              int count2,
                              MPI_Datatype datatype2) {
    large_node_t *node;

    if (!tracking_initialized || large_current == NULL) {
        return;
    }

    node = (large_node_t *)malloc(sizeof(large_node_t));
    if (node == NULL) {
        return;
    }

    node->time = temp_time;
    node->id = current_id;
    node->message_type = message_type;

    node->sender1 = sender1;
    node->receiver1 = receiver1;
    node->count1 = count1;
    node->bytes1 = datatype_nbytes(count1, datatype1);

    node->sender2 = sender2;
    node->receiver2 = receiver2;
    node->count2 = count2;
    node->bytes2 = datatype_nbytes(count2, datatype2);

    node->next = NULL;

    large_current->next = node;
    large_current = node;
    large_current_length++;
    current_id++;

    if (check_data_limit()) {
        write_data_output();
    }
}

static int open_proc_data_files(FILE **small_input_file,
                                FILE **large_input_file,
                                int proc_id,
                                const char *local_hostname) {
    char filename[STRING_LENGTH];
    char tempfilename[STRING_LENGTH + 32];

    if (small_input_file == NULL || large_input_file == NULL) {
        return -1;
    }

    *small_input_file = NULL;
    *large_input_file = NULL;

    get_local_filename(filename, local_hostname, proc_id);

    snprintf(tempfilename, sizeof(tempfilename), "%s_small", filename);
    *small_input_file = fopen(tempfilename, "rb");

    snprintf(tempfilename, sizeof(tempfilename), "%s_large", filename);
    *large_input_file = fopen(tempfilename, "rb");

    return 0;
}

static int remove_proc_data_files(int proc_id, const char *local_hostname) {
    char filename[STRING_LENGTH];
    char tempfilename[STRING_LENGTH + 32];

    get_local_filename(filename, local_hostname, proc_id);

    snprintf(tempfilename, sizeof(tempfilename), "%s_small", filename);
    unlink(tempfilename);

    snprintf(tempfilename, sizeof(tempfilename), "%s_large", filename);
    unlink(tempfilename);

    return 0;
}

static int read_memory_hwm_kb(int *memory_used_kb) {
    char statusfile_path[STRING_LENGTH];
    char line[STRING_LENGTH];
    FILE *statusfile;
    pid_t pid;

    if (memory_used_kb == NULL) {
        return -1;
    }

    *memory_used_kb = 0;
    pid = getpid();

    snprintf(statusfile_path, sizeof(statusfile_path), "/proc/%d/status", (int)pid);

    statusfile = fopen(statusfile_path, "r");
    if (statusfile == NULL) {
        return -1;
    }

    while (fgets(line, sizeof(line), statusfile) != NULL) {
        int value = 0;
        if (sscanf(line, "VmHWM:%d", &value) == 1) {
            *memory_used_kb = value;
            fclose(statusfile);
            return 0;
        }
    }

    fclose(statusfile);
    return -1;
}

static int fortran_status_is_ignore(MPI_Fint *status) {
#ifdef MPI_F_STATUS_IGNORE
    return (status == MPI_F_STATUS_IGNORE);
#else
    (void)status;
    return 0;
#endif
}

/* -------------------------------------------------------------------------- */
/* Fortran entry points                                                       */
/* -------------------------------------------------------------------------- */

void mpi_finalize_(int *ierr) {
    *ierr = MPI_Finalize();
}

void mpi_finalize__(int *ierr) {
    mpi_finalize_(ierr);
}

void mpi_init_(int *ierr) {
    int argc = 0;
    char **argv = NULL;
    *ierr = MPI_Init(&argc, &argv);
}

void mpi_init__(int *ierr) {
    mpi_init_(ierr);
}

void mpi_finalize_f08_(int *ierr) {
    *ierr = MPI_Finalize();
}

void mpi_init_f08_(int *ierr) {
    int argc = 0;
    char **argv = NULL;
    *ierr = MPI_Init(&argc, &argv);
}

void MPI_INIT(int *ierr) {
    mpi_init_(ierr);
}

void MPI_FINALIZE(int *ierr) {
    mpi_finalize_(ierr);
}

void mpi_send_(const void *buf,
               MPI_Fint *count,
               MPI_Fint *datatype,
               MPI_Fint *dest,
               MPI_Fint *tag,
               MPI_Fint *comm,
               MPI_Fint *ierr) {
    MPI_Datatype c_datatype = PMPI_Type_f2c(*datatype);
    MPI_Comm c_comm = PMPI_Comm_f2c(*comm);

    *ierr = (MPI_Fint)MPI_Send(buf,
                               (int)*count,
                               c_datatype,
                               (int)*dest,
                               (int)*tag,
                               c_comm);
}

void mpi_send__(const void *buf,
                MPI_Fint *count,
                MPI_Fint *datatype,
                MPI_Fint *dest,
                MPI_Fint *tag,
                MPI_Fint *comm,
                MPI_Fint *ierr) {
    mpi_send_(buf, count, datatype, dest, tag, comm, ierr);
}

void MPI_SEND(const void *buf,
              MPI_Fint *count,
              MPI_Fint *datatype,
              MPI_Fint *dest,
              MPI_Fint *tag,
              MPI_Fint *comm,
              MPI_Fint *ierr) {
    mpi_send_(buf, count, datatype, dest, tag, comm, ierr);
}

void mpi_recv_(void *buf,
               MPI_Fint *count,
               MPI_Fint *datatype,
               MPI_Fint *source,
               MPI_Fint *tag,
               MPI_Fint *comm,
               MPI_Fint *status,
               MPI_Fint *ierr) {
    MPI_Datatype c_datatype = PMPI_Type_f2c(*datatype);
    MPI_Comm c_comm = PMPI_Comm_f2c(*comm);
    MPI_Status c_status;
    MPI_Status *c_status_ptr = MPI_STATUS_IGNORE;

    if (status != NULL && !fortran_status_is_ignore(status)) {
        c_status_ptr = &c_status;
    }

    *ierr = (MPI_Fint)MPI_Recv(buf,
                               (int)*count,
                               c_datatype,
                               (int)*source,
                               (int)*tag,
                               c_comm,
                               c_status_ptr);

    if (c_status_ptr != MPI_STATUS_IGNORE) {
        PMPI_Status_c2f(&c_status, status);
    }
}

void mpi_recv__(void *buf,
                MPI_Fint *count,
                MPI_Fint *datatype,
                MPI_Fint *source,
                MPI_Fint *tag,
                MPI_Fint *comm,
                MPI_Fint *status,
                MPI_Fint *ierr) {
    mpi_recv_(buf, count, datatype, source, tag, comm, status, ierr);
}

void MPI_RECV(void *buf,
              MPI_Fint *count,
              MPI_Fint *datatype,
              MPI_Fint *source,
              MPI_Fint *tag,
              MPI_Fint *comm,
              MPI_Fint *status,
              MPI_Fint *ierr) {
    mpi_recv_(buf, count, datatype, source, tag, comm, status, ierr);
}

/* -------------------------------------------------------------------------- */
/* Init / Finalize                                                            */
/* -------------------------------------------------------------------------- */

int MPI_Init_thread(int *argc, char ***argv, int required, int *provided) {
    int err = PMPI_Init_thread(argc, argv, required, provided);
    if (err != MPI_SUCCESS) {
        return err;
    }
    return begin_tracking_runtime();
}

int MPI_Init(int *argc, char ***argv) {
    int err = PMPI_Init(argc, argv);
    if (err != MPI_SUCCESS) {
        return err;
    }
    return begin_tracking_runtime();
}

int MPI_Finalize(void) {
    MPI_Comm node_comm = MPI_COMM_NULL;
    MPI_Comm root_comm = MPI_COMM_NULL;
    int node_rank = -1;
    int root_rank = -1;
    int root_size = 0;
    int node_key = 0;

    int memory_used = 0;
    int have_mem = 0;
    int all_have_mem = 0;

    int node_max = 0;
    int node_min = 0;
    int node_total = 0;

    int root_indivi_max = 0;
    int root_indivi_min = 0;
    int root_node_max = 0;
    int root_node_min = 0;
    int root_node_sum = 0;
    int root_node_av = 0;

    if (!tracking_initialized) {
        return PMPI_Finalize();
    }

    node_key = mpi_high_water_get_key();

    PMPI_Comm_split(MPI_COMM_WORLD, node_key, 0, &node_comm);
    PMPI_Comm_rank(node_comm, &node_rank);

    PMPI_Comm_split(MPI_COMM_WORLD, node_rank, 0, &root_comm);

    if (node_rank == 0) {
        PMPI_Comm_rank(root_comm, &root_rank);
        PMPI_Comm_size(root_comm, &root_size);
    }

    have_mem = (read_memory_hwm_kb(&memory_used) == 0) ? 1 : 0;
    PMPI_Allreduce(&have_mem, &all_have_mem, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

    if (all_have_mem) {
        PMPI_Reduce(&memory_used, &node_max,   1, MPI_INT, MPI_MAX, 0, node_comm);
        PMPI_Reduce(&memory_used, &node_min,   1, MPI_INT, MPI_MIN, 0, node_comm);
        PMPI_Reduce(&memory_used, &node_total, 1, MPI_INT, MPI_SUM, 0, node_comm);

        if (node_rank == 0) {
            PMPI_Reduce(&node_max,   &root_indivi_max, 1, MPI_INT, MPI_MAX, 0, root_comm);
            PMPI_Reduce(&node_min,   &root_indivi_min, 1, MPI_INT, MPI_MIN, 0, root_comm);
            PMPI_Reduce(&node_total, &root_node_max,   1, MPI_INT, MPI_MAX, 0, root_comm);
            PMPI_Reduce(&node_total, &root_node_min,   1, MPI_INT, MPI_MIN, 0, root_comm);
            PMPI_Reduce(&node_total, &root_node_sum,   1, MPI_INT, MPI_SUM, 0, root_comm);

            if (root_rank == 0 && root_size > 0) {
                root_node_av = root_node_sum / root_size;
            }
        }

        if (my_rank == 0) {
            printf("process max %dMB min %dMB\n",
                   root_indivi_max / 1024,
                   root_indivi_min / 1024);
            printf("node max %dMB min %dMB avg %dMB\n",
                   root_node_max / 1024,
                   root_node_min / 1024,
                   root_node_av / 1024);
        }
    } else {
        if (my_rank == 0) {
            fprintf(stderr, "Warning: unable to read VmHWM on all ranks; skipping memory summary.\n");
        }
    }

    write_data_output();
    close_data_file();

    communicate_total_message_numbers();

    if (my_rank == 0) {
        process_data_files();
        close_global_file();
    }

    if (node_comm != MPI_COMM_NULL) {
        PMPI_Comm_free(&node_comm);
    }

    if (root_comm != MPI_COMM_NULL) {
        PMPI_Comm_free(&root_comm);
    }

    if (world_group != MPI_GROUP_NULL) {
        PMPI_Group_free(&world_group);
        world_group = MPI_GROUP_NULL;
    }

    destroy_trace_buffers();

    free(number_of_small_messages);
    free(number_of_large_messages);
    free(processes);

    number_of_small_messages = NULL;
    number_of_large_messages = NULL;
    processes = NULL;

    small_output_file = NULL;
    large_output_file = NULL;
    global_file = NULL;

    tracking_initialized = 0;

    return PMPI_Finalize();
}

/* -------------------------------------------------------------------------- */
/* Utility functions                                                          */
/* -------------------------------------------------------------------------- */

int mpi_high_water_name_to_colour(const char *name) {
    const long long small_multiplier = 31LL;
    const long long large_multiplier = 1000000009LL;
    long long res = 0;
    long long power = 1;
    const char *p;

    if (name == NULL) {
        return 0;
    }

    for (p = name; *p; p++) {
        res = (res + (((long long)(unsigned char)(*p)) + 1LL) * power) % large_multiplier;
        if (res < 0) {
            res = -res;
        }
        power = (power * small_multiplier) % large_multiplier;
    }

    return (int)res;
}

void get_date_time_string(char *out) {
    time_t now;
    struct tm tm_now;

    if (out == NULL) {
        return;
    }

    now = time(NULL);

#if defined(_POSIX_THREAD_SAFE_FUNCTIONS)
    localtime_r(&now, &tm_now);
#else
    {
        struct tm *tmp = localtime(&now);
        if (tmp != NULL) {
            tm_now = *tmp;
        } else {
            memset(&tm_now, 0, sizeof(tm_now));
        }
    }
#endif

    strftime(out, DATETIME_LENGTH, "%Y%m%d%H%M%S", &tm_now);
}

int open_data_files(void) {
    char filename[STRING_LENGTH];
    char tempfilename[STRING_LENGTH + 32];

    if (process_id == (pid_t)-1) {
        return -1;
    }

    get_local_filename(filename, hostname, (int)process_id);

    snprintf(tempfilename, sizeof(tempfilename), "%s_small", filename);
    small_output_file = fopen(tempfilename, "wb");
    if (small_output_file == NULL) {
        return -1;
    }

    snprintf(tempfilename, sizeof(tempfilename), "%s_large", filename);
    large_output_file = fopen(tempfilename, "wb");
    if (large_output_file == NULL) {
        fclose(small_output_file);
        small_output_file = NULL;
        return -1;
    }

    return 0;
}

int get_local_filename(char *filename, const char *local_hostname, int proc_id) {
    if (filename == NULL) {
        return -1;
    }

    snprintf(filename,
             STRING_LENGTH,
             ".%.450s-%.450s-%d",
             (programname[0] != '\0') ? programname : "unknown-program",
             (local_hostname != NULL && local_hostname[0] != '\0') ? local_hostname : "unknown-host",
             proc_id);

    return 0;
}

int close_data_file(void) {
    int err = 0;

    if (small_output_file != NULL) {
        if (fclose(small_output_file) != 0) {
            err = -1;
        }
        small_output_file = NULL;
    }

    if (large_output_file != NULL) {
        if (fclose(large_output_file) != 0) {
            err = -1;
        }
        large_output_file = NULL;
    }

    return err;
}

int open_global_file(void) {
    char filename[STRING_LENGTH];
    char stamp[DATETIME_LENGTH];

    assert(my_rank == 0);

    get_date_time_string(stamp);
    snprintf(filename, sizeof(filename), "%.995s-%s.mpic", programname, stamp);

    global_file = fopen(filename, "wb");
    if (global_file == NULL) {
        return -1;
    }

    return 0;
}

int get_program_name(void) {
    ssize_t len;
    char *base;

    len = readlink("/proc/self/exe", programname, sizeof(programname) - 1);
    if (len == -1) {
        strncpy(programname, "unknown-program", sizeof(programname) - 1);
        programname[sizeof(programname) - 1] = '\0';
        return 0;
    }

    programname[len] = '\0';

    base = strrchr(programname, '/');
    if (base != NULL && *(base + 1) != '\0') {
        memmove(programname, base + 1, strlen(base + 1) + 1);
    }

    return 0;
}

int get_datetime(void) {
    time_t t;
    struct tm tm_now;

    t = time(NULL);

#if defined(_POSIX_THREAD_SAFE_FUNCTIONS)
    localtime_r(&t, &tm_now);
#else
    {
        struct tm *tmp = localtime(&t);
        if (tmp != NULL) {
            tm_now = *tmp;
        } else {
            memset(&tm_now, 0, sizeof(tm_now));
        }
    }
#endif

    strftime(datetime, sizeof(datetime), "%Y-%m-%d %H:%M:%S", &tm_now);
    return 0;
}

int get_process_id(void) {
    process_id = getpid();
    return 0;
}

int close_global_file(void) {
    if (global_file == NULL) {
        return 0;
    }

    if (fclose(global_file) != 0) {
        global_file = NULL;
        return -1;
    }

    global_file = NULL;
    return 0;
}

int gather_process_information(void) {
    process_info_t my_process;
    int core = -1;
    int chip = -1;
    int byte_count = (int)sizeof(process_info_t);

    get_processor_and_core(&chip, &core);

    memset(&my_process, 0, sizeof(my_process));
    my_process.rank = my_rank;
    my_process.process_id = (int)process_id;
    my_process.core = core;
    my_process.chip = chip;
    strncpy(my_process.hostname, hostname, sizeof(my_process.hostname) - 1);

    if (my_rank == 0) {
        processes = (process_info_t *)calloc((size_t)my_size, sizeof(process_info_t));
        if (processes == NULL) {
            return -1;
        }
    }

    PMPI_Gather(&my_process,
                byte_count,
                MPI_BYTE,
                processes,
                byte_count,
                MPI_BYTE,
                0,
                MPI_COMM_WORLD);

    return 0;
}

int write_global_information(void) {
    int i;
    char fixed_datetime[DATETIME_LENGTH] = {0};
    char fixed_programname[STRING_LENGTH] = {0};

    assert(my_rank == 0);

    if (global_file == NULL || processes == NULL) {
        return -1;
    }

    snprintf(fixed_datetime, sizeof(fixed_datetime), "%s", datetime);
    snprintf(fixed_programname, sizeof(fixed_programname), "%s", programname);

    fwrite(&my_size, sizeof(int), 1, global_file);
    fwrite(fixed_datetime, sizeof(char), DATETIME_LENGTH, global_file);
    fwrite(fixed_programname, sizeof(char), STRING_LENGTH, global_file);

    for (i = 0; i < my_size; i++) {
        fwrite(&processes[i], sizeof(process_info_t), 1, global_file);
    }

    return 0;
}

int communicate_total_message_numbers(void) {
    if (my_rank == 0) {
        number_of_small_messages = (int *)calloc((size_t)my_size, sizeof(int));
        number_of_large_messages = (int *)calloc((size_t)my_size, sizeof(int));

        if (number_of_small_messages == NULL || number_of_large_messages == NULL) {
            free(number_of_small_messages);
            free(number_of_large_messages);
            number_of_small_messages = NULL;
            number_of_large_messages = NULL;
            return -1;
        }
    }

    PMPI_Gather(&small_total_length,
                1,
                MPI_INT,
                number_of_small_messages,
                1,
                MPI_INT,
                0,
                MPI_COMM_WORLD);

    PMPI_Gather(&large_total_length,
                1,
                MPI_INT,
                number_of_large_messages,
                1,
                MPI_INT,
                0,
                MPI_COMM_WORLD);

    return 0;
}

int process_data_files(void) {
    int i;
    FILE *small_input_file = NULL;
    FILE *large_input_file = NULL;
    small_node_no_link_t temp_small;
    large_node_no_link_t temp_large;

    static const char small_label[24] = "P2P Small Type Messages";
    static const char large_label[24] = "P2P Large Type Messages";

    assert(my_rank == 0);

    if (global_file == NULL || processes == NULL ||
        number_of_small_messages == NULL || number_of_large_messages == NULL) {
        return -1;
    }

    for (i = 0; i < my_size; i++) {
        open_proc_data_files(&small_input_file,
                             &large_input_file,
                             processes[i].process_id,
                             processes[i].hostname);

        fwrite(&processes[i].rank, sizeof(int), 1, global_file);
        fwrite(small_label, sizeof(char), sizeof(small_label), global_file);
        fwrite(&number_of_small_messages[i], sizeof(int), 1, global_file);

        if (small_input_file != NULL) {
            while (fread(&temp_small, sizeof(temp_small), 1, small_input_file) == 1) {
                fwrite(&temp_small, sizeof(temp_small), 1, global_file);
            }
            fclose(small_input_file);
            small_input_file = NULL;
        }

        fwrite(large_label, sizeof(char), sizeof(large_label), global_file);
        fwrite(&number_of_large_messages[i], sizeof(int), 1, global_file);

        if (large_input_file != NULL) {
            while (fread(&temp_large, sizeof(temp_large), 1, large_input_file) == 1) {
                fwrite(&temp_large, sizeof(temp_large), 1, global_file);
            }
            fclose(large_input_file);
            large_input_file = NULL;
        }

        remove_proc_data_files(processes[i].process_id, processes[i].hostname);
    }

    return 0;
}

int get_data_limit(void) {
    return 2000;
}

int write_data_output(void) {
    small_node_t *cur_small;
    large_node_t *cur_large;

    if (!tracking_initialized) {
        return 0;
    }

    if (small_output_file != NULL) {
        cur_small = (small_head != NULL) ? small_head->next : NULL;
        while (cur_small != NULL) {
            small_node_t *node = cur_small;
            small_node_no_link_t temp;

            temp.time = node->time;
            temp.id = node->id;
            temp.message_type = node->message_type;
            temp.sender = node->sender;
            temp.receiver = node->receiver;
            temp.count = node->count;
            temp.bytes = node->bytes;

            cur_small = cur_small->next;
            fwrite(&temp, sizeof(temp), 1, small_output_file);
            free(node);
        }
    } else {
        free_small_list_nodes();
    }

    if (small_head != NULL) {
        small_head->next = NULL;
        small_current = small_head;
    }
    small_total_length += small_current_length;
    small_current_length = 0;

    if (large_output_file != NULL) {
        cur_large = (large_head != NULL) ? large_head->next : NULL;
        while (cur_large != NULL) {
            large_node_t *node = cur_large;
            large_node_no_link_t temp;

            temp.time = node->time;
            temp.id = node->id;
            temp.message_type = node->message_type;
            temp.sender1 = node->sender1;
            temp.receiver1 = node->receiver1;
            temp.count1 = node->count1;
            temp.bytes1 = node->bytes1;
            temp.sender2 = node->sender2;
            temp.receiver2 = node->receiver2;
            temp.count2 = node->count2;
            temp.bytes2 = node->bytes2;

            cur_large = cur_large->next;
            fwrite(&temp, sizeof(temp), 1, large_output_file);
            free(node);
        }
    } else {
        free_large_list_nodes();
    }

    if (large_head != NULL) {
        large_head->next = NULL;
        large_current = large_head;
    }
    large_total_length += large_current_length;
    large_current_length = 0;

    return 0;
}

int mpi_high_water_get_key(void) {
    char name[MPI_MAX_PROCESSOR_NAME] = {0};
    int len = 0;

    PMPI_Get_processor_name(name, &len);
    return mpi_high_water_name_to_colour(name);
}

#if defined(__aarch64__)
unsigned long get_processor_and_core(int *chip, int *core) {
    return syscall(SYS_getcpu, core, chip, NULL);
}
#elif defined(__x86_64__) || defined(__i386__)
unsigned long get_processor_and_core(int *chip, int *core) {
    unsigned int a, d, c;
    __asm__ volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
    *chip = (int)((c & 0xFFF000U) >> 12);
    *core = (int)(c & 0xFFFU);
    return ((unsigned long)a) | (((unsigned long)d) << 32);
}
#else
unsigned long get_processor_and_core(int *chip, int *core) {
    if (chip) {
        *chip = -1;
    }
    if (core) {
        *core = -1;
    }
    return 0UL;
}
#endif

/* -------------------------------------------------------------------------- */
/* MPI wrappers                                                               */
/* -------------------------------------------------------------------------- */

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
    int rc;
    int sender_world = my_rank;
    int receiver_world = dest;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Send(buf, count, datatype, dest, tag, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &sender_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &receiver_world);
        }

        add_small_data_at(ts, MPI_SEND_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status) {
    int rc;
    int sender_world = source;
    int receiver_world = my_rank;
    int actual_source = source;
    int is_inter = 0;
    double ts = trace_timestamp();
    MPI_Status local_status;
    MPI_Status *call_status = status;

    if (call_status == NULL) {
        call_status = MPI_STATUS_IGNORE;
    }

    if (source == MPI_ANY_SOURCE && c_status_is_ignore(call_status)) {
        call_status = &local_status;
    }

    rc = PMPI_Recv(buf, count, datatype, source, tag, comm, call_status);

    if (rc == MPI_SUCCESS) {
        if (source == MPI_ANY_SOURCE && !c_status_is_ignore(call_status)) {
            actual_source = call_status->MPI_SOURCE;
        }

        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &receiver_world);
            translate_comm_rank_to_world(comm, actual_source, is_inter ? 1 : 0, &sender_world);
        } else {
            sender_world = actual_source;
        }

        add_small_data_at(ts, MPI_RECV_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Bsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
    int rc;
    int sender_world = my_rank;
    int receiver_world = dest;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Bsend(buf, count, datatype, dest, tag, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &sender_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &receiver_world);
        }

        add_small_data_at(ts, MPI_BSEND_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Ssend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
    int rc;
    int sender_world = my_rank;
    int receiver_world = dest;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Ssend(buf, count, datatype, dest, tag, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &sender_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &receiver_world);
        }

        add_small_data_at(ts, MPI_SSEND_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Rsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
    int rc;
    int sender_world = my_rank;
    int receiver_world = dest;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Rsend(buf, count, datatype, dest, tag, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &sender_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &receiver_world);
        }

        add_small_data_at(ts, MPI_RSEND_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request) {
    int rc;
    int sender_world = my_rank;
    int receiver_world = dest;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Isend(buf, count, datatype, dest, tag, comm, request);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &sender_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &receiver_world);
        }

        add_small_data_at(ts, MPI_ISEND_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Ibsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request) {
    int rc;
    int sender_world = my_rank;
    int receiver_world = dest;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Ibsend(buf, count, datatype, dest, tag, comm, request);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &sender_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &receiver_world);
        }

        add_small_data_at(ts, MPI_IBSEND_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Issend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request) {
    int rc;
    int sender_world = my_rank;
    int receiver_world = dest;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Issend(buf, count, datatype, dest, tag, comm, request);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &sender_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &receiver_world);
        }

        add_small_data_at(ts, MPI_ISSEND_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Irsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request) {
    int rc;
    int sender_world = my_rank;
    int receiver_world = dest;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Irsend(buf, count, datatype, dest, tag, comm, request);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &sender_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &receiver_world);
        }

        add_small_data_at(ts, MPI_IRSEND_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request) {
    int rc;
    int sender_world = source;
    int receiver_world = my_rank;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Irecv(buf, count, datatype, source, tag, comm, request);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &receiver_world);
            translate_comm_rank_to_world(comm, source, is_inter ? 1 : 0, &sender_world);
        }

        add_small_data_at(ts, MPI_IRECV_TYPE, sender_world, receiver_world, count, datatype);
    }

    return rc;
}

int MPI_Sendrecv(const void *sendbuf,
                 int sendcount,
                 MPI_Datatype sendtype,
                 int dest,
                 int sendtag,
                 void *recvbuf,
                 int recvcount,
                 MPI_Datatype recvtype,
                 int source,
                 int recvtag,
                 MPI_Comm comm,
                 MPI_Status *status) {
    int rc;
    int local_world = my_rank;
    int dest_world = dest;
    int source_world = source;
    int actual_source = source;
    int is_inter = 0;
    double ts = trace_timestamp();
    MPI_Status local_status;
    MPI_Status *call_status = status;

    if (call_status == NULL) {
        call_status = MPI_STATUS_IGNORE;
    }

    if (source == MPI_ANY_SOURCE && c_status_is_ignore(call_status)) {
        call_status = &local_status;
    }

    rc = PMPI_Sendrecv(sendbuf, sendcount, sendtype,
                       dest, sendtag,
                       recvbuf, recvcount, recvtype,
                       source, recvtag,
                       comm, call_status);

    if (rc == MPI_SUCCESS) {
        if (source == MPI_ANY_SOURCE && !c_status_is_ignore(call_status)) {
            actual_source = call_status->MPI_SOURCE;
        }

        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            PMPI_Comm_test_inter(comm, &is_inter);
            current_world_rank_in_comm(comm, &local_world);
            translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &dest_world);
            translate_comm_rank_to_world(comm, actual_source, is_inter ? 1 : 0, &source_world);
        } else {
            source_world = actual_source;
        }

        add_large_data_at(ts,
                          MPI_SENDRECV_TYPE,
                          local_world, dest_world, sendcount, sendtype,
                          source_world, local_world, recvcount, recvtype);
    }

    return rc;
}

int MPI_Wait(MPI_Request *request, MPI_Status *status) {
    int rc;
    double ts = trace_timestamp();

    rc = PMPI_Wait(request, status);

    if (rc == MPI_SUCCESS) {
        add_small_data_at(ts, MPI_WAIT_TYPE, my_rank, my_rank, 0, MPI_DATATYPE_NULL);
    }

    return rc;
}

int MPI_Waitall(int count, MPI_Request array_of_requests[], MPI_Status array_of_statuses[]) {
    int rc;
    double ts = trace_timestamp();

    rc = PMPI_Waitall(count, array_of_requests, array_of_statuses);

    if (rc == MPI_SUCCESS) {
        add_small_data_at(ts, MPI_WAITALL_TYPE, my_rank, my_rank, count, MPI_DATATYPE_NULL);
    }

    return rc;
}

int MPI_Barrier(MPI_Comm comm) {
    int rc;
    int local_world = my_rank;
    double ts = trace_timestamp();

    rc = PMPI_Barrier(comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            current_world_rank_in_comm(comm, &local_world);
        }

        add_small_data_at(ts, MPI_BARRIER_TYPE, local_world, local_world, 0, MPI_DATATYPE_NULL);
    }

    return rc;
}

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm) {
    int rc;
    int root_world = root;
    int local_world = my_rank;
    double ts = trace_timestamp();

    rc = PMPI_Bcast(buffer, count, datatype, root, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            current_world_rank_in_comm(comm, &local_world);
            translate_comm_rank_to_world(comm, root, 0, &root_world);
        }

        add_small_data_at(ts, MPI_BCAST_TYPE, root_world, local_world, count, datatype);
    }

    return rc;
}

int MPI_Reduce(const void *sendbuf,
               void *recvbuf,
               int count,
               MPI_Datatype datatype,
               MPI_Op op,
               int root,
               MPI_Comm comm) {
    int rc;
    int root_world = root;
    int local_world = my_rank;
    double ts = trace_timestamp();

    rc = PMPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            current_world_rank_in_comm(comm, &local_world);
            translate_comm_rank_to_world(comm, root, 0, &root_world);
        }

        add_small_data_at(ts, MPI_REDUCE_TYPE, local_world, root_world, count, datatype);
    }

    return rc;
}

int MPI_Allreduce(const void *sendbuf,
                  void *recvbuf,
                  int count,
                  MPI_Datatype datatype,
                  MPI_Op op,
                  MPI_Comm comm) {
    int rc;
    int local_world = my_rank;
    double ts = trace_timestamp();

    rc = PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            current_world_rank_in_comm(comm, &local_world);
        }

        add_small_data_at(ts, MPI_ALLREDUCE_TYPE, local_world, local_world, count, datatype);
    }

    return rc;
}

int MPI_Gather(const void *sendbuf,
               int sendcount,
               MPI_Datatype sendtype,
               void *recvbuf,
               int recvcount,
               MPI_Datatype recvtype,
               int root,
               MPI_Comm comm) {
    int rc;
    int root_world = root;
    int local_world = my_rank;
    int comm_size = 1;
    int aggregate_recvcount = 0;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Gather(sendbuf, sendcount, sendtype,
                     recvbuf, recvcount, recvtype,
                     root, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            current_world_rank_in_comm(comm, &local_world);
            translate_comm_rank_to_world(comm, root, 0, &root_world);
            PMPI_Comm_test_inter(comm, &is_inter);
        }

        PMPI_Comm_size(comm, &comm_size);

        if (!is_inter && local_world == root_world) {
            aggregate_recvcount = safe_mul_to_int(recvcount, comm_size);
        }

        add_large_data_at(ts,
                          MPI_GATHER_TYPE,
                          local_world, root_world, sendcount, sendtype,
                          root_world, root_world, aggregate_recvcount,
                          (aggregate_recvcount > 0) ? recvtype : MPI_DATATYPE_NULL);
    }

    return rc;
}

int MPI_Scatter(const void *sendbuf,
                int sendcount,
                MPI_Datatype sendtype,
                void *recvbuf,
                int recvcount,
                MPI_Datatype recvtype,
                int root,
                MPI_Comm comm) {
    int rc;
    int root_world = root;
    int local_world = my_rank;
    int comm_size = 1;
    int aggregate_sendcount = 0;
    int is_inter = 0;
    double ts = trace_timestamp();

    rc = PMPI_Scatter(sendbuf, sendcount, sendtype,
                      recvbuf, recvcount, recvtype,
                      root, comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            current_world_rank_in_comm(comm, &local_world);
            translate_comm_rank_to_world(comm, root, 0, &root_world);
            PMPI_Comm_test_inter(comm, &is_inter);
        }

        PMPI_Comm_size(comm, &comm_size);

        if (!is_inter && local_world == root_world) {
            aggregate_sendcount = safe_mul_to_int(sendcount, comm_size);
        }

        add_large_data_at(ts,
                          MPI_SCATTER_TYPE,
                          root_world, root_world, aggregate_sendcount,
                          (aggregate_sendcount > 0) ? sendtype : MPI_DATATYPE_NULL,
                          root_world, local_world, recvcount, recvtype);
    }

    return rc;
}

int MPI_Allgather(const void *sendbuf,
                  int sendcount,
                  MPI_Datatype sendtype,
                  void *recvbuf,
                  int recvcount,
                  MPI_Datatype recvtype,
                  MPI_Comm comm) {
    int rc;
    int local_world = my_rank;
    int comm_size = 1;
    int aggregate_recvcount = 0;
    double ts = trace_timestamp();

    rc = PMPI_Allgather(sendbuf, sendcount, sendtype,
                        recvbuf, recvcount, recvtype,
                        comm);

    if (rc == MPI_SUCCESS) {
        if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
            current_world_rank_in_comm(comm, &local_world);
        }

        PMPI_Comm_size(comm, &comm_size);
        aggregate_recvcount = safe_mul_to_int(recvcount, comm_size);

        add_large_data_at(ts,
                          MPI_ALLGATHER_TYPE,
                          local_world, local_world, sendcount, sendtype,
                          local_world, local_world, aggregate_recvcount, recvtype);
    }

    return rc;
}

