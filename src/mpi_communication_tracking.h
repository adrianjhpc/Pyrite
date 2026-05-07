#ifndef MPI_COMMUNICATION_TRACKING_H
#define MPI_COMMUNICATION_TRACKING_H

#include <mpi.h>
#include <sys/types.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define STRING_LENGTH 1024
#define DATETIME_LENGTH 64

/* MPI Event Types */
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
#define MPI_WAITANY_TYPE    33
#define MPI_WAITSOME_TYPE   34
#define MPI_TEST_TYPE       35
#define MPI_TESTANY_TYPE    36
#define MPI_TESTALL_TYPE    37
#define MPI_TESTSOME_TYPE   38

/* --- LEGACY FORMATS FOR FILE I/O --- */
typedef struct small_node_no_link {
    double time;
    int id;
    int message_type;
    int sender;
    int receiver;
    int count;
    int bytes;
} small_node_no_link_t;

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

/* --- UNIFIED TELEMETRY EVENT --- */
typedef struct telemetry_event {
    double time;
    int id;
    int message_type;
    int is_large; 
    
    int sender;
    int receiver;
    int count;
    int bytes;
    
    int sender2;
    int receiver2;
    int count2;
    int bytes2;
} telemetry_event_t;

/* Backend Interface Definition */
typedef struct {
    int (*init_backend)(int rank, int size);
    void (*record_event)(telemetry_event_t *event);
    void (*finalize_backend)(void);
} tracking_backend_t;

void register_tracking_backend(tracking_backend_t *backend);

/* Common state exposed to backends */
extern double tracking_start_time;
extern int64_t tracking_start_unix_ns;
extern int tracking_my_rank;
extern int tracking_my_size;
extern char tracking_hostname[STRING_LENGTH];
extern char tracking_programname[STRING_LENGTH];
extern char tracking_datetime[DATETIME_LENGTH];

/* Utility functions */
double trace_timestamp(void);
int datatype_nbytes(int count, MPI_Datatype datatype);
unsigned long get_processor_and_core(int *chip, int *core);
void get_date_time_string(char *out);

#ifdef __cplusplus
}
#endif

#endif /* MPI_COMMUNICATION_TRACKING_H */
