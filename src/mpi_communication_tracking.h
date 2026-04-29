#ifndef MPI_COMMUNICATION_TRACKING_H
#define MPI_COMMUNICATION_TRACKING_H

#include <mpi.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define STRING_LENGTH 1024
#define DATETIME_LENGTH 64

#define MPI_SEND_TYPE       13
#define MPI_RECV_TYPE       14
#define MPI_BSEND_TYPE      15
#define MPI_SSEND_TYPE      16
#define MPI_RSEND_TYPE      17
#define MPI_ISEND_TYPE      18
#define MPI_IBSEND_TYPE     19
#define MPI_ISSEND_TYPE     20
#define MPI_IRSEND_TYPE     21
#define MPI_IRECV_TYPE      22
#define MPI_SENDRECV_TYPE   23
#define MPI_WAIT_TYPE       24
#define MPI_WAITALL_TYPE    25
#define MPI_BARRIER_TYPE    26
#define MPI_BCAST_TYPE      27
#define MPI_REDUCE_TYPE     28
#define MPI_ALLREDUCE_TYPE  29
#define MPI_GATHER_TYPE     30
#define MPI_SCATTER_TYPE    31
#define MPI_ALLGATHER_TYPE  32

typedef struct small_node {
    double time;
    int id;
    int message_type;
    int sender;
    int receiver;
    int count;
    int bytes;
    struct small_node *next;
} small_node_t;

typedef struct small_node_no_link {
    double time;
    int id;
    int message_type;
    int sender;
    int receiver;
    int count;
    int bytes;
} small_node_no_link_t;

typedef struct large_node {
    double time;
    int id;
    int message_type;
    int sender1;
    int receiver1;
    int count1;
    int bytes1;
    int sender2;
    int receiver2;
    int count2;
    int bytes2;
    struct large_node *next;
} large_node_t;

typedef struct large_node_no_link {
    double time;
    int id;
    int message_type;
    int sender1;
    int receiver1;
    int count1;
    int bytes1;
    int sender2;
    int receiver2;
    int count2;
    int bytes2;
} large_node_no_link_t;

typedef struct process_info {
    int rank;
    int process_id;
    int core;
    int chip;
    char hostname[STRING_LENGTH];
} process_info_t;

/* Utility functions */
int mpi_high_water_name_to_colour(const char *name);
int mpi_high_water_get_key(void);
void get_date_time_string(char *datetime);
unsigned long get_processor_and_core(int *chip, int *core);

int open_data_files(void);
int open_global_file(void);
int gather_process_information(void);
int write_global_information(void);
int write_data_output(void);
int close_data_file(void);
int communicate_total_message_numbers(void);
int process_data_files(void);
int close_global_file(void);
int get_program_name(void);
int get_process_id(void);
int get_datetime(void);
int get_local_filename(char *filename, const char *hostname, int proc_id);
int get_data_limit(void);

#ifdef __cplusplus
}
#endif

#endif /* MPI_COMMUNICATION_TRACKING_H */

