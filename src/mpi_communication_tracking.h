#ifndef MPI_COMMUNICATION_TRACKING_H
#define MPI_COMMUNICATION_TRACKING_H

#define STRING_LENGTH 1024
#define DATETIME_LENGTH 64
#define MPI_SEND_TYPE 13
#define MPI_RECV_TYPE 14
#define MPI_BSEND_TYPE 15
#define MPI_SSEND_TYPE 16
#define MPI_RSEND_TYPE 17
#define MPI_ISEND_TYPE 18
#define MPI_IBSEND_TYPE 19
#define MPI_ISSEND_TYPE 20
#define MPI_IRSEND_TYPE 21
#define MPI_IRECV_TYPE 22
#define MPI_SENDRECV_TYPE 23
#define MPI_WAIT_TYPE 24
#define MPI_WAITALL_TYPE 25
#define MPI_BARRIER_TYPE 26
#define MPI_BCAST_TYPE 27
#define MPI_REDUCE_TYPE 28
#define MPI_ALLREDUCE_TYPE 29
#define MPI_GATHER_TYPE 30
#define MPI_SCATTER_TYPE 31
#define MPI_ALLGATHER_TYPE 32

// Forward declarations
int mpi_high_water_name_to_colour(const char *);
int mpi_high_water_get_key();
void get_date_time_string(char *);
unsigned long get_processor_and_core(int *chip, int *core);
int open_data_files();
int open_global_file();
int gather_process_information();
int write_global_information();
int write_data_output();
int close_data_file();
int communicate_total_message_numbers();
int process_data_files();
int close_global_file();
int get_program_name();
int get_process_id();
int get_datetime();
int get_local_filename(char *filename, char *hostname, int proc_id);
int get_data_limit();


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

#endif // MPI_COMMUNICATION_TRACKING_H
