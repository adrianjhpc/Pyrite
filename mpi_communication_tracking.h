#define STRING_LENGTH 1024
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

typedef struct p2p_small_node {
  int time;
  int id;
  int message_type;
  int sender;
  int receiver;
  int count;
  int datatype;
  struct p2p_small_node *next;
} p2p_small_node_t;

typedef struct p2p_small_node_no_link {
  int time;
  int id;
  int message_type;
  int sender;
  int receiver;
  int count;
  int datatype;
} p2p_small_node_no_link_t;


typedef struct p2p_large_node {
  int time;
  int id;
  int message_type;
  int sender1;
  int receiver1;
  int count1;
  int datatype1;
  int sender2;
  int receiver2;
  int count2;
  int datatype2;
  struct p2p_large_node *next;
} p2p_large_node_t;

typedef struct p2p_large_node_no_link {
  int time;
  int id;
  int message_type;
  int sender1;
  int receiver1;
  int count1;
  int datatype1;
  int sender2;
  int receiver2;
  int count2;
  int datatype2;
} p2p_large_node_no_link_t;


typedef struct process_info {
  int rank;
  int process_id;
  int core;
  int chip;
  char hostname[STRING_LENGTH];
} process_info_t;
