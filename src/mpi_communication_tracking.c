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
/* Common Exposed State (Read-Only for Backends)                              */
/* -------------------------------------------------------------------------- */

double tracking_start_time = 0.0;
int64_t tracking_start_unix_ns = 0;
int tracking_my_rank = -1;
int tracking_my_size = 0;
char tracking_hostname[STRING_LENGTH] = {0};
char tracking_programname[STRING_LENGTH] = {0};
char tracking_datetime[DATETIME_LENGTH] = {0};

/* -------------------------------------------------------------------------- */
/* Internal State                                                             */
/* -------------------------------------------------------------------------- */

static int current_id = 0;
static MPI_Group world_group = MPI_GROUP_NULL;
static int tracking_initialized = 0;

/* The active backend */
static tracking_backend_t *current_backend = NULL;

void register_tracking_backend(tracking_backend_t *backend) {
  current_backend = backend;
}

/* -------------------------------------------------------------------------- */
/* Internal helpers                                                           */
/* -------------------------------------------------------------------------- */

static int c_status_is_ignore(MPI_Status *status) {
  return (status == MPI_STATUS_IGNORE || status == NULL);
}

static int c_statuses_are_ignore(MPI_Status *statuses) {
  return (statuses == MPI_STATUSES_IGNORE || statuses == NULL);
}

static int safe_mul_to_int(int a, int b) {
  long long v;
  if (a <= 0 || b <= 0) return 0;
  v = (long long)a * (long long)b;
  if (v > INT_MAX) return INT_MAX;
  return (int)v;
}

int datatype_nbytes(int count, MPI_Datatype datatype) {
  int type_size = 0;
  if (count <= 0 || datatype == MPI_DATATYPE_NULL) return 0;
  if (PMPI_Type_size(datatype, &type_size) != MPI_SUCCESS || type_size <= 0) return 0;
  return safe_mul_to_int(count, type_size);
}

double trace_timestamp(void) {
  return PMPI_Wtime() - tracking_start_time;
}

static void free_pending_request(pending_request_t *req) {
  if (req == NULL) return;
  if (req->have_comm_dup && req->comm_dup != MPI_COMM_NULL && req->comm_dup != MPI_COMM_WORLD) {
    PMPI_Comm_free(&req->comm_dup);
    req->comm_dup = MPI_COMM_WORLD;
  }
  free(req);
}

static void free_pending_request_list(void) {
  pending_request_t *cur = pending_requests;
  pending_request_t *next;
  while (cur != NULL) {
    next = cur->next;
    free_pending_request(cur);
    cur = next;
  }
  pending_requests = NULL;
}

static int translate_comm_rank_to_world(MPI_Comm comm, int comm_rank, int use_remote_group, int *world_rank_out) {
  int rc;
  int translated = MPI_UNDEFINED;
  int input_rank = comm_rank;
  int is_inter = 0;
  MPI_Group from_group = MPI_GROUP_NULL;

  if (world_rank_out == NULL) return MPI_ERR_ARG;
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
  if (world_rank_out == NULL) return MPI_ERR_ARG;
  if (comm == MPI_COMM_WORLD || comm == MPI_COMM_NULL) {
    *world_rank_out = tracking_my_rank;
    return MPI_SUCCESS;
  }
  rc = PMPI_Comm_rank(comm, &local_rank);
  if (rc != MPI_SUCCESS) {
    *world_rank_out = tracking_my_rank;
    return rc;
  }
  return translate_comm_rank_to_world(comm, local_rank, 0, world_rank_out);
}

static int sample_local_time_anchor(double *mpi_zero_out, int64_t *unix_zero_ns_out) {
  double w0, w1;
  struct timespec ts;

  if (mpi_zero_out == NULL || unix_zero_ns_out == NULL) {
    return -1;
  }

  /*
   * Midpoint sampling reduces error from the clock_gettime() syscall latency.
   */
  w0 = PMPI_Wtime();

  if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
    return -1;
  }

  w1 = PMPI_Wtime();

  *mpi_zero_out = 0.5 * (w0 + w1);
  *unix_zero_ns_out = (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;

  return 0;
}

/* -------------------------------------------------------------------------- */
/* Event Recording Routers                                                    */
/* -------------------------------------------------------------------------- */

static void record_small_event(double ts, int message_type, int comm, int tag, int sender, int receiver, int count, MPI_Datatype datatype) {
  if (!tracking_initialized || !current_backend || !current_backend->record_event) return;

  telemetry_event_t ev = {0};
  ev.time = ts;
  ev.id = current_id++;
  ev.message_type = message_type;
  ev.is_large = 0;
  ev.comm = comm;
  ev.tag = tag;  
  ev.sender = sender;
  ev.receiver = receiver;
  ev.count = count;
  ev.bytes = datatype_nbytes(count, datatype);

  current_backend->record_event(&ev);
}

static void record_large_event(double ts, int message_type, int comm, int sender1, int receiver1, int count1, MPI_Datatype datatype1, int tag1, int sender2, int receiver2, int count2, MPI_Datatype datatype2, int tag2) {
  if (!tracking_initialized || !current_backend || !current_backend->record_event) return;

  telemetry_event_t ev = {0};
  ev.time = ts;
  ev.id = current_id++;
  ev.message_type = message_type;
  ev.is_large = 1;
  ev.comm = comm;
  ev.sender = sender1;
  ev.receiver = receiver1;
  ev.count = count1;
  ev.bytes = datatype_nbytes(count1, datatype1);
  ev.tag = tag1;
  ev.sender2 = sender2;
  ev.receiver2 = receiver2;
  ev.count2 = count2;
  ev.bytes2 = datatype_nbytes(count2, datatype2);
  ev.tag2 = tag2;

  current_backend->record_event(&ev);
}

/* -------------------------------------------------------------------------- */
/* Pending Request Logic                                                      */
/* -------------------------------------------------------------------------- */

static pending_request_t *find_pending_request(MPI_Request handle) {
  pending_request_t *cur = pending_requests;
  while (cur != NULL) {
    if (cur->handle == handle) return cur;
    cur = cur->next;
  }
  return NULL;
}

static pending_request_t *detach_pending_request(MPI_Request handle) {
  pending_request_t *cur = pending_requests;
  pending_request_t *prev = NULL;
  while (cur != NULL) {
    if (cur->handle == handle) {
      if (prev == NULL) pending_requests = cur->next;
      else prev->next = cur->next;
      cur->next = NULL;
      return cur;
    }
    prev = cur;
    cur = cur->next;
  }
  return NULL;
}

static void register_pending_request(MPI_Request handle, int message_type, int sender_world, int receiver_world,
                                     int count, MPI_Datatype datatype, int tag, int is_recv, int source_rank_param,
                                     int peer_is_remote_group, MPI_Comm comm) {
  pending_request_t *req;
  if (handle == MPI_REQUEST_NULL) return;

  req = (pending_request_t *)calloc(1, sizeof(pending_request_t));
  if (req == NULL) return;

  req->handle = handle;
  req->message_type = message_type;
  req->sender_world = sender_world;
  req->receiver_world = receiver_world;
  req->count = count;
  req->datatype = datatype;
  req->tag = tag;
  req->is_recv = is_recv;
  req->source_rank_param = source_rank_param;
  req->peer_is_remote_group = peer_is_remote_group;
  req->comm_dup = MPI_COMM_WORLD;
  req->have_comm_dup = 0;
  req->next = pending_requests;

  if (is_recv && comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
    if (PMPI_Comm_dup(comm, &req->comm_dup) == MPI_SUCCESS) {
      req->have_comm_dup = 1;
    }
  }
  pending_requests = req;
}

static int actual_count_from_status(MPI_Status *status, MPI_Datatype datatype, int fallback_count) {
  int actual_count = fallback_count;
  int rc;
  if (status == NULL || datatype == MPI_DATATYPE_NULL) return fallback_count;
  rc = PMPI_Get_count(status, datatype, &actual_count);
  if (rc != MPI_SUCCESS || actual_count == MPI_UNDEFINED || actual_count < 0) return fallback_count;
  return actual_count;
}

static int status_is_cancelled(MPI_Status *status) {
  int cancelled = 0;
  if (status == NULL) return 0;
  if (PMPI_Test_cancelled(status, &cancelled) != MPI_SUCCESS) return 0;
  return cancelled;
}

static void complete_pending_request(pending_request_t *req, MPI_Status *status, int status_valid, double completion_time) {
  int cancelled = 0;
  if (req == NULL) return;
  if (status_valid && status != NULL) cancelled = status_is_cancelled(status);

  if (!cancelled) {
    if (req->is_recv) {
      int actual_sender = req->sender_world;
      int actual_count = req->count;

      if (status_valid && status != NULL) {
	actual_count = actual_count_from_status(status, req->datatype, req->count);
	if (req->source_rank_param == MPI_ANY_SOURCE) {
	  if (req->have_comm_dup && req->comm_dup != MPI_COMM_NULL && req->comm_dup != MPI_COMM_WORLD) {
	    translate_comm_rank_to_world(req->comm_dup, status->MPI_SOURCE, req->peer_is_remote_group, &actual_sender);
	  } else {
	    actual_sender = status->MPI_SOURCE;
	  }
	}
      }
      record_small_event(completion_time, req->message_type, PMPI_Comm_c2f(req->comm_dup), req->tag, actual_sender, req->receiver_world, actual_count, req->datatype);
    } else {
      record_small_event(completion_time, req->message_type, PMPI_Comm_c2f(req->comm_dup), req->tag, req->sender_world, req->receiver_world, req->count, req->datatype);
    }
  }
  free_pending_request(req);
}

/* -------------------------------------------------------------------------- */
/* Environment & OS Setup Helpers                                             */
/* -------------------------------------------------------------------------- */

void get_date_time_string(char *out) {
  time_t now;
  struct tm tm_now;
  if (out == NULL) return;
  now = time(NULL);
#if defined(_POSIX_THREAD_SAFE_FUNCTIONS)
  localtime_r(&now, &tm_now);
#else
  {
    struct tm *tmp = localtime(&now);
    if (tmp != NULL) tm_now = *tmp;
    else memset(&tm_now, 0, sizeof(tm_now));
  }
#endif
  strftime(out, DATETIME_LENGTH, "%Y%m%d%H%M%S", &tm_now);
}

int get_program_name(void) {
  ssize_t len;
  char *base;
  len = readlink("/proc/self/exe", tracking_programname, sizeof(tracking_programname) - 1);
  if (len == -1) {
    strncpy(tracking_programname, "unknown-program", sizeof(tracking_programname) - 1);
    tracking_programname[sizeof(tracking_programname) - 1] = '\0';
    return 0;
  }
  tracking_programname[len] = '\0';
  base = strrchr(tracking_programname, '/');
  if (base != NULL && *(base + 1) != '\0') {
    memmove(tracking_programname, base + 1, strlen(base + 1) + 1);
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
    if (tmp != NULL) tm_now = *tmp;
    else memset(&tm_now, 0, sizeof(tm_now));
  }
#endif
  strftime(tracking_datetime, sizeof(tracking_datetime), "%Y-%m-%d %H:%M:%S", &tm_now);
  return 0;
}


int mpi_high_water_name_to_colour(const char *name) {
  const long long small_multiplier = 31LL;
  const long long large_multiplier = 1000000009LL;
  long long res = 0;
  long long power = 1;
  const char *p;
  if (name == NULL) return 0;
  for (p = name; *p; p++) {
    res = (res + (((long long)(unsigned char)(*p)) + 1LL) * power) % large_multiplier;
    if (res < 0) res = -res;
    power = (power * small_multiplier) % large_multiplier;
  }
  return (int)res;
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
  if (chip) *chip = -1;
  if (core) *core = -1;
  return 0UL;
}
#endif

/* -------------------------------------------------------------------------- */
/* Init / Finalize                                                            */
/* -------------------------------------------------------------------------- */

static void cleanup_failed_init(void) {
  if (world_group != MPI_GROUP_NULL) {
    PMPI_Group_free(&world_group);
    world_group = MPI_GROUP_NULL;
  }
  free_pending_request_list();
  tracking_initialized = 0;
}

static int begin_tracking_runtime(void) {
  int err = MPI_SUCCESS;

  err = PMPI_Comm_rank(MPI_COMM_WORLD, &tracking_my_rank);
  if (err != MPI_SUCCESS) { cleanup_failed_init(); return err; }

  err = PMPI_Comm_size(MPI_COMM_WORLD, &tracking_my_size);
  if (err != MPI_SUCCESS) { cleanup_failed_init(); return err; }

  err = PMPI_Barrier(MPI_COMM_WORLD);
  if (err != MPI_SUCCESS) { cleanup_failed_init(); return err; }

  /*
   * Barrier gives rough startup alignment only.
   * Precise DB timestamp registration is done per rank using the local
   * MPI<->Unix anchor pair below.
   */
  if (sample_local_time_anchor(&tracking_start_time, &tracking_start_unix_ns) != 0) {
    cleanup_failed_init();
    return MPI_ERR_INTERN;
  }

  get_program_name();

  if (gethostname(tracking_hostname, sizeof(tracking_hostname)) != 0) {
    strncpy(tracking_hostname, "unknown-host", sizeof(tracking_hostname) - 1);
  }
  tracking_hostname[sizeof(tracking_hostname) - 1] = '\0';

  get_datetime();

  err = PMPI_Comm_group(MPI_COMM_WORLD, &world_group);
  if (err != MPI_SUCCESS) { cleanup_failed_init(); return err; }

  // Initialize backend
  if (current_backend && current_backend->init_backend) {
    if (current_backend->init_backend(tracking_my_rank, tracking_my_size) != MPI_SUCCESS) {
      cleanup_failed_init();
      return MPI_ERR_INTERN;
    }
  }

  tracking_initialized = 1;
  return MPI_SUCCESS;
}

int MPI_Init_thread(int *argc, char ***argv, int required, int *provided) {
  int err = PMPI_Init_thread(argc, argv, required, provided);
  if (err != MPI_SUCCESS) return err;
  return begin_tracking_runtime();
}

int MPI_Init(int *argc, char ***argv) {
  int err = PMPI_Init(argc, argv);
  if (err != MPI_SUCCESS) return err;
  return begin_tracking_runtime();
}

int MPI_Finalize(void) {
  if (!tracking_initialized) return PMPI_Finalize();

  if (current_backend && current_backend->finalize_backend) {
    current_backend->finalize_backend();
  }

  if (world_group != MPI_GROUP_NULL) {
    PMPI_Group_free(&world_group);
    world_group = MPI_GROUP_NULL;
  }

  free_pending_request_list();
  tracking_initialized = 0;

  return PMPI_Finalize();
}

/* -------------------------------------------------------------------------- */
/* MPI Wrappers (C Interface)                                                 */
/* -------------------------------------------------------------------------- */

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
  int rc;
  int sender_world = tracking_my_rank;
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
    record_small_event(ts, MPI_SEND_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
  }
  return rc;
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status) {
  int rc;
  int sender_world = source;
  int receiver_world = tracking_my_rank;
  int actual_source = source;
  int actual_count = count;
  int is_inter = 0;
  double ts = trace_timestamp();
  MPI_Status local_status;
  MPI_Status *call_status = status;

  if (c_status_is_ignore(call_status)) call_status = &local_status;

  rc = PMPI_Recv(buf, count, datatype, source, tag, comm, call_status);

  if (rc == MPI_SUCCESS) {
    actual_count = actual_count_from_status(call_status, datatype, count);
    if (source == MPI_ANY_SOURCE) actual_source = call_status->MPI_SOURCE;

    if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
      PMPI_Comm_test_inter(comm, &is_inter);
      current_world_rank_in_comm(comm, &receiver_world);
      translate_comm_rank_to_world(comm, actual_source, is_inter ? 1 : 0, &sender_world);
    } else {
      sender_world = actual_source;
    }
    record_small_event(ts, MPI_RECV_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, actual_count, datatype);
  }

  if (!c_status_is_ignore(status) && call_status == &local_status) *status = local_status;
  return rc;
}

int MPI_Bsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
  int rc;
  int sender_world = tracking_my_rank;
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
    record_small_event(ts, MPI_BSEND_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
  }
  return rc;
}

int MPI_Ssend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
  int rc;
  int sender_world = tracking_my_rank;
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
    record_small_event(ts, MPI_SSEND_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
  }
  return rc;
}

int MPI_Rsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
  int rc;
  int sender_world = tracking_my_rank;
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
    record_small_event(ts, MPI_RSEND_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
  }
  return rc;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request) {
  int rc;
  int sender_world = tracking_my_rank;
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

    if (request != NULL && *request != MPI_REQUEST_NULL) {
      register_pending_request(*request, MPI_ISEND_TYPE, sender_world, receiver_world,
			       count, datatype, tag, 0, dest, is_inter ? 1 : 0, comm);
    } else {
      record_small_event(ts, MPI_ISEND_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
    }
  }
  return rc;
}

int MPI_Ibsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request) {
  int rc;
  int sender_world = tracking_my_rank;
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

    if (request != NULL && *request != MPI_REQUEST_NULL) {
      register_pending_request(*request, MPI_IBSEND_TYPE, sender_world, receiver_world,
			       count, datatype, tag, 0, dest, is_inter ? 1 : 0, comm);
    } else {
      record_small_event(ts, MPI_IBSEND_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
    }
  }
  return rc;
}

int MPI_Issend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request) {
  int rc;
  int sender_world = tracking_my_rank;
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

    if (request != NULL && *request != MPI_REQUEST_NULL) {
      register_pending_request(*request, MPI_ISSEND_TYPE, sender_world, receiver_world,
			       count, datatype, tag, 0, dest, is_inter ? 1 : 0, comm);
    } else {
      record_small_event(ts, MPI_ISSEND_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
    }
  }
  return rc;
}

int MPI_Irsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request) {
  int rc;
  int sender_world = tracking_my_rank;
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

    if (request != NULL && *request != MPI_REQUEST_NULL) {
      register_pending_request(*request, MPI_IRSEND_TYPE, sender_world, receiver_world,
			       count, datatype, tag, 0, dest, is_inter ? 1 : 0, comm);
    } else {
      record_small_event(ts, MPI_IRSEND_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
    }
  }
  return rc;
}

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request) {
  int rc;
  int sender_world = source;
  int receiver_world = tracking_my_rank;
  int is_inter = 0;
  double ts = trace_timestamp();

  rc = PMPI_Irecv(buf, count, datatype, source, tag, comm, request);

  if (rc == MPI_SUCCESS) {
    if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
      PMPI_Comm_test_inter(comm, &is_inter);
      current_world_rank_in_comm(comm, &receiver_world);
      if (source != MPI_ANY_SOURCE) {
	translate_comm_rank_to_world(comm, source, is_inter ? 1 : 0, &sender_world);
      }
    }

    if (request != NULL && *request != MPI_REQUEST_NULL) {
      register_pending_request(*request, MPI_IRECV_TYPE, sender_world, receiver_world,
			       count, datatype, tag, 1, source, is_inter ? 1 : 0, comm);
    } else {
      record_small_event(ts, MPI_IRECV_TYPE, PMPI_Comm_c2f(comm), tag, sender_world, receiver_world, count, datatype);
    }
  }
  return rc;
}

int MPI_Sendrecv(const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype, int source, int recvtag,
                 MPI_Comm comm, MPI_Status *status) {
  int rc;
  int local_world = tracking_my_rank;
  int dest_world = dest;
  int source_world = source;
  int actual_source = source;
  int actual_recvcount = recvcount;
  int is_inter = 0;
  double ts = trace_timestamp();
  MPI_Status local_status;
  MPI_Status *call_status = status;

  if (c_status_is_ignore(call_status)) call_status = &local_status;

  rc = PMPI_Sendrecv(sendbuf, sendcount, sendtype, dest, sendtag,
		     recvbuf, recvcount, recvtype, source, recvtag, comm, call_status);

  if (rc == MPI_SUCCESS) {
    actual_recvcount = actual_count_from_status(call_status, recvtype, recvcount);
    if (source == MPI_ANY_SOURCE) actual_source = call_status->MPI_SOURCE;

    if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
      PMPI_Comm_test_inter(comm, &is_inter);
      current_world_rank_in_comm(comm, &local_world);
      translate_comm_rank_to_world(comm, dest, is_inter ? 1 : 0, &dest_world);
      translate_comm_rank_to_world(comm, actual_source, is_inter ? 1 : 0, &source_world);
    } else {
      source_world = actual_source;
    }

    record_large_event(ts, MPI_SENDRECV_TYPE, PMPI_Comm_c2f(comm), local_world, dest_world, sendcount, sendtype, sendtag, source_world, local_world, actual_recvcount, recvtype, recvtag);
  }

  if (!c_status_is_ignore(status) && call_status == &local_status) *status = local_status;
  return rc;
}

int MPI_Wait(MPI_Request *request, MPI_Status *status) {
  int rc;
  int completed = 0;
  MPI_Request req_before = MPI_REQUEST_NULL;
  pending_request_t *tracked = NULL;
  double ts = trace_timestamp();
  MPI_Status local_status;
  MPI_Status *call_status = status;

  if (request != NULL) req_before = *request;

  tracked = find_pending_request(req_before);
  if (tracked != NULL && c_status_is_ignore(call_status)) call_status = &local_status;
  else if (call_status == NULL) call_status = MPI_STATUS_IGNORE;

  rc = PMPI_Wait(request, call_status);

  if (rc == MPI_SUCCESS) {
    if (tracked != NULL) {
      tracked = detach_pending_request(req_before);
      complete_pending_request(tracked, call_status, !c_status_is_ignore(call_status), ts);
      completed = 1;
    }
    record_small_event(ts, MPI_WAIT_TYPE, PMPI_Comm_c2f(tracked->comm_dup), tracked->tag, tracking_my_rank, tracking_my_rank, completed, MPI_DATATYPE_NULL);
  }
  return rc;
}

int MPI_Waitall(int count, MPI_Request array_of_requests[], MPI_Status array_of_statuses[]) {
  int rc;
  int i;
  int completed = 0;
  MPI_Request *pre_handles = NULL;
  MPI_Status *temp_statuses = NULL;
  MPI_Status *call_statuses = array_of_statuses;
  double t_start = trace_timestamp();

  if (count > 0) {
    pre_handles = (MPI_Request *)malloc((size_t)count * sizeof(MPI_Request));
    if (pre_handles != NULL) {
      for (i = 0; i < count; i++) pre_handles[i] = array_of_requests[i];
    }
  }

  if (count > 0 && c_statuses_are_ignore(array_of_statuses)) {
    temp_statuses = (MPI_Status *)malloc((size_t)count * sizeof(MPI_Status));
    if (temp_statuses != NULL) call_statuses = temp_statuses;
  }

  rc = PMPI_Waitall(count, array_of_requests, call_statuses);

  double t_end = trace_timestamp();
  int duration_us = (int)((t_end - t_start) * 1000000.0);

  if (rc == MPI_SUCCESS) {
    for (i = 0; i < count; i++) {
      pending_request_t *tracked = detach_pending_request(pre_handles[i]);
      if (tracked != NULL) {
	MPI_Status *st = NULL;
	if (!c_statuses_are_ignore(call_statuses)) st = &call_statuses[i];
	complete_pending_request(tracked, st, (st != NULL), t_end);
	completed++;
      }
    }
    record_small_event(t_start, MPI_WAITALL_TYPE, 0, 0, tracking_my_rank, tracking_my_rank, completed, duration_us);
  }

  free(pre_handles);
  free(temp_statuses);
  return rc;
}

int MPI_Waitany(int count, MPI_Request array_of_requests[], int *index, MPI_Status *status) {
  int rc;
  int completed = 0;
  int i;
  MPI_Request *pre_handles = NULL;
  MPI_Status local_status;
  MPI_Status *call_status = status;
  double t_start = trace_timestamp();

  if (count > 0) {
    pre_handles = (MPI_Request *)malloc((size_t)count * sizeof(MPI_Request));
    if (pre_handles != NULL) {
      for (i = 0; i < count; i++) pre_handles[i] = array_of_requests[i];
    }
  }

  if (c_status_is_ignore(call_status)) call_status = &local_status;

  rc = PMPI_Waitany(count, array_of_requests, index, call_status);

  double t_end = trace_timestamp();
  int duration_us = (int)((t_end - t_start) * 1000000.0);

  if (rc == MPI_SUCCESS) {
    if (index != NULL && *index != MPI_UNDEFINED && pre_handles != NULL) {
      pending_request_t *tracked = detach_pending_request(pre_handles[*index]);
      if (tracked != NULL) {
	complete_pending_request(tracked, call_status, 1, t_start);
	completed = 1;
      }
    }
    record_small_event(t_start, MPI_WAITANY_TYPE, 0, 0, tracking_my_rank, tracking_my_rank, completed, duration_us);
  }

  free(pre_handles);
  return rc;
}

int MPI_Waitsome(int incount, MPI_Request array_of_requests[], int *outcount, int array_of_indices[], MPI_Status array_of_statuses[]) {
  int rc;
  int i;
  int completed = 0;
  MPI_Request *pre_handles = NULL;
  MPI_Status *temp_statuses = NULL;
  MPI_Status *call_statuses = array_of_statuses;
  double t_start = trace_timestamp();

  if (incount > 0) {
    pre_handles = (MPI_Request *)malloc((size_t)incount * sizeof(MPI_Request));
    if (pre_handles != NULL) {
      for (i = 0; i < incount; i++) pre_handles[i] = array_of_requests[i];
    }
  }

  if (incount > 0 && c_statuses_are_ignore(array_of_statuses)) {
    temp_statuses = (MPI_Status *)malloc((size_t)incount * sizeof(MPI_Status));
    if (temp_statuses != NULL) call_statuses = temp_statuses;
  }

  rc = PMPI_Waitsome(incount, array_of_requests, outcount, array_of_indices, call_statuses);

  double t_end = trace_timestamp();
  int duration_us = (int)((t_end - t_start) * 1000000.0);

  if (rc == MPI_SUCCESS && outcount != NULL && *outcount != MPI_UNDEFINED) {
    for (i = 0; i < *outcount; i++) {
      int idx = array_of_indices[i];
      pending_request_t *tracked = detach_pending_request(pre_handles[idx]);
      if (tracked != NULL) {
	MPI_Status *st = NULL;
	if (!c_statuses_are_ignore(call_statuses)) st = &call_statuses[i];
	complete_pending_request(tracked, st, (st != NULL), t_start);
	completed++;
      }
    }
    record_small_event(t_start, MPI_WAITSOME_TYPE, 0, 0, tracking_my_rank, tracking_my_rank, completed, duration_us);
  }

  free(pre_handles);
  free(temp_statuses);
  return rc;
}

int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status) {
  int rc;
  int completed = 0;
  MPI_Request req_before = MPI_REQUEST_NULL;
  pending_request_t *tracked = NULL;
  double t_start = trace_timestamp();
  MPI_Status local_status;
  MPI_Status *call_status = status;

  if (request != NULL) req_before = *request;

  tracked = find_pending_request(req_before);
  if (tracked != NULL && c_status_is_ignore(call_status)) call_status = &local_status;
  else if (call_status == NULL) call_status = MPI_STATUS_IGNORE;

  rc = PMPI_Test(request, flag, call_status);

  double t_end = trace_timestamp();
  int duration_us = (int)((t_end - t_start) * 1000000.0);

  if (rc == MPI_SUCCESS && flag != NULL && *flag) {
    if (tracked != NULL) {
      tracked = detach_pending_request(req_before);
      complete_pending_request(tracked, call_status, !c_status_is_ignore(call_status), t_start);
      completed = 1;
    }
    record_small_event(t_start, MPI_TEST_TYPE, MPI_Comm_c2f(tracked->comm_dup), tracked->tag, tracking_my_rank, tracking_my_rank, completed, duration_us);
  }
  return rc;
}

int MPI_Testall(int count, MPI_Request array_of_requests[], int *flag, MPI_Status array_of_statuses[]) {
  int rc;
  int i;
  int completed = 0;
  MPI_Request *pre_handles = NULL;
  MPI_Status *temp_statuses = NULL;
  MPI_Status *call_statuses = array_of_statuses;
  double t_start = trace_timestamp();

  if (count > 0) {
    pre_handles = (MPI_Request *)malloc((size_t)count * sizeof(MPI_Request));
    if (pre_handles != NULL) {
      for (i = 0; i < count; i++) pre_handles[i] = array_of_requests[i];
    }
  }

  if (count > 0 && c_statuses_are_ignore(array_of_statuses)) {
    temp_statuses = (MPI_Status *)malloc((size_t)count * sizeof(MPI_Status));
    if (temp_statuses != NULL) call_statuses = temp_statuses;
  }

  rc = PMPI_Testall(count, array_of_requests, flag, call_statuses);

  double t_end = trace_timestamp();
  int duration_us = (int)((t_end - t_start) * 1000000.0);

  if (rc == MPI_SUCCESS && flag != NULL && *flag) {
    for (i = 0; i < count; i++) {
      pending_request_t *tracked = detach_pending_request(pre_handles[i]);
      if (tracked != NULL) {
	MPI_Status *st = NULL;
	if (!c_statuses_are_ignore(call_statuses)) st = &call_statuses[i];
	complete_pending_request(tracked, st, (st != NULL), t_start);
	completed++;
      }
    }
    record_small_event(t_start, MPI_TESTALL_TYPE, 0, 0, tracking_my_rank, tracking_my_rank, completed, duration_us);
  }

  free(pre_handles);
  free(temp_statuses);
  return rc;
}

int MPI_Testany(int count, MPI_Request array_of_requests[], int *index, int *flag, MPI_Status *status) {
  int rc;
  int completed = 0;
  int i;
  MPI_Request *pre_handles = NULL;
  MPI_Status local_status;
  MPI_Status *call_status = status;
  double t_start = trace_timestamp();

  if (count > 0) {
    pre_handles = (MPI_Request *)malloc((size_t)count * sizeof(MPI_Request));
    if (pre_handles != NULL) {
      for (i = 0; i < count; i++) pre_handles[i] = array_of_requests[i];
    }
  }

  if (c_status_is_ignore(call_status)) call_status = &local_status;

  rc = PMPI_Testany(count, array_of_requests, index, flag, call_status);

  double t_end = trace_timestamp();
  int duration_us = (int)((t_end - t_start) * 1000000.0);

  if (rc == MPI_SUCCESS && flag != NULL && *flag) {
    if (index != NULL && *index != MPI_UNDEFINED && pre_handles != NULL) {
      pending_request_t *tracked = detach_pending_request(pre_handles[*index]);
      if (tracked != NULL) {
	complete_pending_request(tracked, call_status, 1, t_start);
	completed = 1;
      }
    }
    record_small_event(t_start, MPI_TESTANY_TYPE, 0, 0, tracking_my_rank, tracking_my_rank, completed, duration_us);
  }

  free(pre_handles);
  return rc;
}

int MPI_Testsome(int incount, MPI_Request array_of_requests[], int *outcount, int array_of_indices[], MPI_Status array_of_statuses[]) {
  int rc;
  int i;
  int completed = 0;
  MPI_Request *pre_handles = NULL;
  MPI_Status *temp_statuses = NULL;
  MPI_Status *call_statuses = array_of_statuses;
  double t_start = trace_timestamp();

  if (incount > 0) {
    pre_handles = (MPI_Request *)malloc((size_t)incount * sizeof(MPI_Request));
    if (pre_handles != NULL) {
      for (i = 0; i < incount; i++) pre_handles[i] = array_of_requests[i];
    }
  }

  if (incount > 0 && c_statuses_are_ignore(array_of_statuses)) {
    temp_statuses = (MPI_Status *)malloc((size_t)incount * sizeof(MPI_Status));
    if (temp_statuses != NULL) call_statuses = temp_statuses;
  }

  rc = PMPI_Testsome(incount, array_of_requests, outcount, array_of_indices, call_statuses);

  double t_end = trace_timestamp();
  int duration_us = (int)((t_end - t_start) * 1000000.0);

  if (rc == MPI_SUCCESS && outcount != NULL && *outcount != MPI_UNDEFINED && *outcount > 0) {
    for (i = 0; i < *outcount; i++) {
      int idx = array_of_indices[i];
      pending_request_t *tracked = detach_pending_request(pre_handles[idx]);
      if (tracked != NULL) {
	MPI_Status *st = NULL;
	if (!c_statuses_are_ignore(call_statuses)) st = &call_statuses[i];
	complete_pending_request(tracked, st, (st != NULL), t_start);
	completed++;
      }
    }
    record_small_event(t_start, MPI_TESTSOME_TYPE, 0, 0, tracking_my_rank, tracking_my_rank, completed, duration_us);
  }

  free(pre_handles);
  free(temp_statuses);
  return rc;
}

int MPI_Cancel(MPI_Request *request) {
  return PMPI_Cancel(request);
}

int MPI_Barrier(MPI_Comm comm) {
  int rc;
  int local_world = tracking_my_rank;
  double t_start = trace_timestamp();

  rc = PMPI_Barrier(comm);

  double t_end = trace_timestamp();
  int duration_us = (int)((t_end - t_start) * 1000000.0);

  if (rc == MPI_SUCCESS) {
    if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
      current_world_rank_in_comm(comm, &local_world);
    }
    record_small_event(t_start, MPI_BARRIER_TYPE, PMPI_Comm_c2f(comm), 0, local_world, local_world, 0, duration_us);
  }
  return rc;
}

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm) {
  int rc;
  int root_world = root;
  int local_world = tracking_my_rank;
  double ts = trace_timestamp();

  rc = PMPI_Bcast(buffer, count, datatype, root, comm);

  if (rc == MPI_SUCCESS) {
    if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
      current_world_rank_in_comm(comm, &local_world);
      translate_comm_rank_to_world(comm, root, 0, &root_world);
    }
    record_small_event(ts, MPI_BCAST_TYPE, PMPI_Comm_c2f(comm), 0, root_world, local_world, count, datatype);
  }
  return rc;
}

int MPI_Reduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm) {
  int rc;
  int root_world = root;
  int local_world = tracking_my_rank;
  double ts = trace_timestamp();

  rc = PMPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, comm);

  if (rc == MPI_SUCCESS) {
    if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
      current_world_rank_in_comm(comm, &local_world);
      translate_comm_rank_to_world(comm, root, 0, &root_world);
    }
    record_small_event(ts, MPI_REDUCE_TYPE, PMPI_Comm_c2f(comm), 0, local_world, root_world, count, datatype);
  }
  return rc;
}

int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm) {
  int rc;
  int local_world = tracking_my_rank;
  double ts = trace_timestamp();

  rc = PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, comm);

  if (rc == MPI_SUCCESS) {
    if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
      current_world_rank_in_comm(comm, &local_world);
    }
    record_small_event(ts, MPI_ALLREDUCE_TYPE, PMPI_Comm_c2f(comm), 0, local_world, local_world, count, datatype);
  }
  return rc;
}

int MPI_Gather(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm) {
  int rc;
  int root_world = root;
  int local_world = tracking_my_rank;
  int comm_size = 1;
  int aggregate_recvcount = 0;
  int is_inter = 0;
  double ts = trace_timestamp();

  rc = PMPI_Gather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm);

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

    record_large_event(ts, MPI_GATHER_TYPE, PMPI_Comm_c2f(comm), local_world, root_world, sendcount, sendtype, 0, root_world, root_world, aggregate_recvcount, (aggregate_recvcount > 0) ? recvtype : MPI_DATATYPE_NULL, 0);
  }
  return rc;
}

int MPI_Scatter(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm) {
  int rc;
  int root_world = root;
  int local_world = tracking_my_rank;
  int comm_size = 1;
  int aggregate_sendcount = 0;
  int is_inter = 0;
  double ts = trace_timestamp();

  rc = PMPI_Scatter(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm);

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

    record_large_event(ts, MPI_SCATTER_TYPE, PMPI_Comm_c2f(comm), root_world, root_world, aggregate_sendcount, (aggregate_sendcount > 0) ? sendtype : MPI_DATATYPE_NULL, 0, root_world, local_world, recvcount, recvtype, 0);
  }
  return rc;
}

int MPI_Allgather(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm) {
  int rc;
  int local_world = tracking_my_rank;
  int comm_size = 1;
  int aggregate_recvcount = 0;
  double ts = trace_timestamp();

  rc = PMPI_Allgather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);

  if (rc == MPI_SUCCESS) {
    if (comm != MPI_COMM_WORLD && comm != MPI_COMM_NULL) {
      current_world_rank_in_comm(comm, &local_world);
    }

    PMPI_Comm_size(comm, &comm_size);
    aggregate_recvcount = safe_mul_to_int(recvcount, comm_size);

    record_large_event(ts, MPI_ALLGATHER_TYPE, PMPI_Comm_c2f(comm), local_world, local_world, sendcount, sendtype, 0, local_world, local_world, aggregate_recvcount, recvtype, 0);
  }
  return rc;
}

#ifdef MPI_TRACE_ENABLE_FORTRAN_SUPPORT
/* -------------------------------------------------------------------------- */
/* Fortran Helpers                                                            */
/* -------------------------------------------------------------------------- */
static int fortran_status_is_ignore(MPI_Fint *status) {
#ifdef MPI_F_STATUS_IGNORE
  return (status == MPI_F_STATUS_IGNORE);
#else
  (void)status;
  return 0;
#endif
}

static int fortran_statuses_are_ignore(MPI_Fint *statuses) {
#ifdef MPI_F_STATUSES_IGNORE
  return (statuses == MPI_F_STATUSES_IGNORE);
#else
  (void)statuses;
  return 0;
#endif
}

extern int mpi_tracking_get_status_size(void);

static int get_fortran_status_size(void) {
  static int cached = 0;

  if (cached > 0) {
    return cached;
  }

#ifdef MPI_F_STATUS_SIZE
  cached = MPI_F_STATUS_SIZE;
#elif defined(MPI_STATUS_SIZE)
  cached = MPI_STATUS_SIZE;
#else
  cached = mpi_tracking_get_status_size();
#endif

  return cached;
}

static void report_fortran_status_size_error(const char *wrapper_name, int f_status_size) {
  static int already_reported = 0;

  /*
    Avoid spamming stderr from repeated calls. We still fail each wrapper
    call, but only print the full diagnostic once per process.
  */
  if (already_reported) {
    return;
  }
  already_reported = 1;

  fprintf(stderr,
	  "[mpi-trace] ERROR: invalid Fortran MPI status size in %s on rank %d (%s).\n"
	  "[mpi-trace]        get_fortran_status_size() returned %d.\n"
	  "[mpi-trace]        The tracer cannot safely convert C MPI_Status values\n"
	  "[mpi-trace]        back into a Fortran status array without a valid stride.\n"
	  "[mpi-trace]        Likely causes:\n"
	  "[mpi-trace]          - the Fortran helper object was not linked\n"
	  "[mpi-trace]          - MPI Fortran support was enabled inconsistently\n"
	  "[mpi-trace]          - the MPI implementation does not expose a usable\n"
	  "[mpi-trace]            Fortran status size to this build\n",
	  wrapper_name,
	  tracking_my_rank,
	  (tracking_hostname[0] != '\0') ? tracking_hostname : "unknown-host",
	  f_status_size);

  fflush(stderr);
}

static int validate_fortran_status_size(const char *wrapper_name,
                                        int *f_status_size_out,
                                        MPI_Fint *ierr) {
  int f_status_size;

  if (f_status_size_out == NULL) {
    if (ierr != NULL) {
      *ierr = (MPI_Fint)MPI_ERR_ARG;
    }
    fprintf(stderr,
	    "[mpi-trace] ERROR: validate_fortran_status_size called with NULL output pointer in %s.\n",
	    wrapper_name);
    fflush(stderr);
    return 0;
  }

  f_status_size = get_fortran_status_size();

  if (f_status_size <= 0) {
    report_fortran_status_size_error(wrapper_name, f_status_size);
    if (ierr != NULL) {
      *ierr = (MPI_Fint)MPI_ERR_INTERN;
    }
    return 0;
  }

  *f_status_size_out = f_status_size;
  return 1;
}

/* -------------------------------------------------------------------------- */
/* Fortran Wrappers                                                           */
/* -------------------------------------------------------------------------- */

void mpi_init_(int *ierr) { int argc = 0; char **argv = NULL; *ierr = MPI_Init(&argc, &argv); }
void mpi_init__(int *ierr) { mpi_init_(ierr); }
void mpi_init_f08_(int *ierr) { int argc = 0; char **argv = NULL; *ierr = MPI_Init(&argc, &argv); }
void MPI_INIT(int *ierr) { mpi_init_(ierr); }

void mpi_finalize_(int *ierr) { *ierr = MPI_Finalize(); }
void mpi_finalize__(int *ierr) { mpi_finalize_(ierr); }
void mpi_finalize_f08_(int *ierr) { *ierr = MPI_Finalize(); }
void MPI_FINALIZE(int *ierr) { mpi_finalize_(ierr); }

void mpi_send_(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Send(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*dest, (int)*tag, PMPI_Comm_f2c(*comm));
}
void mpi_send__(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) {
  mpi_send_(buf, count, datatype, dest, tag, comm, ierr);
}
void MPI_SEND(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) {
  mpi_send_(buf, count, datatype, dest, tag, comm, ierr);
}

void mpi_recv_(void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *source, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *status, MPI_Fint *ierr) {
  MPI_Status c_status;
#ifdef MPI_F_STATUS_IGNORE
  MPI_Status *c_status_ptr = (status == MPI_F_STATUS_IGNORE) ? MPI_STATUS_IGNORE : &c_status;
#else
  MPI_Status *c_status_ptr = &c_status;
#endif
  *ierr = (MPI_Fint)MPI_Recv(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*source, (int)*tag, PMPI_Comm_f2c(*comm), c_status_ptr);
  if (c_status_ptr != MPI_STATUS_IGNORE) PMPI_Status_c2f(&c_status, status);
}
void mpi_recv__(void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *source, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *status, MPI_Fint *ierr) {
  mpi_recv_(buf, count, datatype, source, tag, comm, status, ierr);
}
void MPI_RECV(void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *source, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *status, MPI_Fint *ierr) {
  mpi_recv_(buf, count, datatype, source, tag, comm, status, ierr);
}

void mpi_isend_(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  MPI_Request c_req = MPI_REQUEST_NULL;
  *ierr = (MPI_Fint)MPI_Isend(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*dest, (int)*tag, PMPI_Comm_f2c(*comm), &c_req);
  if (request != NULL) *request = PMPI_Request_c2f(c_req);
}
void mpi_isend__(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_isend_(buf, count, datatype, dest, tag, comm, request, ierr);
}
void MPI_ISEND(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_isend_(buf, count, datatype, dest, tag, comm, request, ierr);
}

void mpi_ibsend_(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  MPI_Request c_req = MPI_REQUEST_NULL;
  *ierr = (MPI_Fint)MPI_Ibsend(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*dest, (int)*tag, PMPI_Comm_f2c(*comm), &c_req);
  if (request != NULL) *request = PMPI_Request_c2f(c_req);
}
void mpi_ibsend__(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_ibsend_(buf, count, datatype, dest, tag, comm, request, ierr);
}
void MPI_IBSEND(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_ibsend_(buf, count, datatype, dest, tag, comm, request, ierr);
}

void mpi_issend_(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  MPI_Request c_req = MPI_REQUEST_NULL;
  *ierr = (MPI_Fint)MPI_Issend(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*dest, (int)*tag, PMPI_Comm_f2c(*comm), &c_req);
  if (request != NULL) *request = PMPI_Request_c2f(c_req);
}
void mpi_issend__(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_issend_(buf, count, datatype, dest, tag, comm, request, ierr);
}
void MPI_ISSEND(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_issend_(buf, count, datatype, dest, tag, comm, request, ierr);
}

void mpi_irsend_(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  MPI_Request c_req = MPI_REQUEST_NULL;
  *ierr = (MPI_Fint)MPI_Irsend(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*dest, (int)*tag, PMPI_Comm_f2c(*comm), &c_req);
  if (request != NULL) *request = PMPI_Request_c2f(c_req);
}
void mpi_irsend__(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_irsend_(buf, count, datatype, dest, tag, comm, request, ierr);
}
void MPI_IRSEND(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_irsend_(buf, count, datatype, dest, tag, comm, request, ierr);
}

void mpi_irecv_(void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *source, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  MPI_Request c_req = MPI_REQUEST_NULL;
  *ierr = (MPI_Fint)MPI_Irecv(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*source, (int)*tag, PMPI_Comm_f2c(*comm), &c_req);
  if (request != NULL) *request = PMPI_Request_c2f(c_req);
}
void mpi_irecv__(void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *source, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_irecv_(buf, count, datatype, source, tag, comm, request, ierr);
}
void MPI_IRECV(void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *source, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *request, MPI_Fint *ierr) {
  mpi_irecv_(buf, count, datatype, source, tag, comm, request, ierr);
}

void mpi_wait_(MPI_Fint *request, MPI_Fint *status, MPI_Fint *ierr) {
  MPI_Request c_req = (request != NULL) ? PMPI_Request_f2c(*request) : MPI_REQUEST_NULL;
  MPI_Status c_status;
#ifdef MPI_F_STATUS_IGNORE
  MPI_Status *c_status_ptr = (status == MPI_F_STATUS_IGNORE) ? MPI_STATUS_IGNORE : &c_status;
#else
  MPI_Status *c_status_ptr = &c_status;
#endif
  *ierr = (MPI_Fint)MPI_Wait(&c_req, c_status_ptr);
  if (request != NULL) *request = PMPI_Request_c2f(c_req);
  if (c_status_ptr != MPI_STATUS_IGNORE && status != NULL) PMPI_Status_c2f(&c_status, status);
}
void mpi_wait__(MPI_Fint *request, MPI_Fint *status, MPI_Fint *ierr) { mpi_wait_(request, status, ierr); }
void MPI_WAIT(MPI_Fint *request, MPI_Fint *status, MPI_Fint *ierr) { mpi_wait_(request, status, ierr); }

void mpi_test_(MPI_Fint *request, int *flag, MPI_Fint *status, MPI_Fint *ierr) {
  MPI_Request c_req = (request != NULL) ? PMPI_Request_f2c(*request) : MPI_REQUEST_NULL;
  MPI_Status c_status;
#ifdef MPI_F_STATUS_IGNORE
  MPI_Status *c_status_ptr = (status == MPI_F_STATUS_IGNORE) ? MPI_STATUS_IGNORE : &c_status;
#else
  MPI_Status *c_status_ptr = &c_status;
#endif
  int c_flag = 0;
  *ierr = (MPI_Fint)MPI_Test(&c_req, &c_flag, c_status_ptr);
  if (request != NULL) *request = PMPI_Request_c2f(c_req);
  if (flag != NULL) *flag = c_flag;
  if (c_flag && c_status_ptr != MPI_STATUS_IGNORE && status != NULL) PMPI_Status_c2f(&c_status, status);
}
void mpi_test__(MPI_Fint *request, int *flag, MPI_Fint *status, MPI_Fint *ierr) { mpi_test_(request, flag, status, ierr); }
void MPI_TEST(MPI_Fint *request, int *flag, MPI_Fint *status, MPI_Fint *ierr) { mpi_test_(request, flag, status, ierr); }

void mpi_waitall_(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  int i;
  int n = (int)*count;
  MPI_Request *c_requests = NULL;
  MPI_Status *c_statuses = NULL;
  MPI_Status *c_status_ptr = MPI_STATUSES_IGNORE;

  if (n > 0) {
    c_requests = (MPI_Request *)malloc((size_t)n * sizeof(MPI_Request));
    if (c_requests == NULL) {
      *ierr = (MPI_Fint)MPI_ERR_NO_MEM;
      return;
    }

    for (i = 0; i < n; i++) {
      c_requests[i] = PMPI_Request_f2c(array_of_requests[i]);
    }

    if (array_of_statuses != NULL && !fortran_statuses_are_ignore(array_of_statuses)) {
      c_statuses = (MPI_Status *)malloc((size_t)n * sizeof(MPI_Status));
      if (c_statuses == NULL) {
	free(c_requests);
	*ierr = (MPI_Fint)MPI_ERR_NO_MEM;
	return;
      }
      c_status_ptr = c_statuses;
    }
  }

  /* Call the C wrapper so telemetry logic is triggered */
  *ierr = (MPI_Fint)MPI_Waitall(n, c_requests, c_status_ptr);

  if (n > 0) {
    for (i = 0; i < n; i++) {
      array_of_requests[i] = PMPI_Request_c2f(c_requests[i]);
    }

    if (c_status_ptr != MPI_STATUSES_IGNORE && array_of_statuses != NULL) {
      int f_status_size = 0;

      if (!validate_fortran_status_size("mpi_waitall_", &f_status_size, ierr)) {
	free(c_requests);
	free(c_statuses);
	return;
      } 
      for (i = 0; i < n; i++) {
	PMPI_Status_c2f(&c_statuses[i], &array_of_statuses[i * f_status_size]);
      }
    }
  }

  free(c_requests);
  free(c_statuses);
}

void mpi_waitall__(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  mpi_waitall_(count, array_of_requests, array_of_statuses, ierr);
}

void MPI_WAITALL(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  mpi_waitall_(count, array_of_requests, array_of_statuses, ierr);
}


void mpi_waitany_(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint *index, MPI_Fint *status, MPI_Fint *ierr) {
  int i;
  int n = (int)*count;
  int c_index = MPI_UNDEFINED;
  MPI_Request *c_requests = NULL;
  MPI_Status c_status;
  MPI_Status *c_status_ptr = MPI_STATUS_IGNORE;

  if (n > 0) {
    c_requests = (MPI_Request *)malloc((size_t)n * sizeof(MPI_Request));
    if (c_requests == NULL) {
      *ierr = (MPI_Fint)MPI_ERR_NO_MEM;
      return;
    }

    for (i = 0; i < n; i++) {
      c_requests[i] = PMPI_Request_f2c(array_of_requests[i]);
    }
  }

  if (status != NULL && !fortran_status_is_ignore(status)) {
    c_status_ptr = &c_status;
  }

  *ierr = (MPI_Fint)MPI_Waitany(n, c_requests, &c_index, c_status_ptr);

  if (n > 0) {
    for (i = 0; i < n; i++) {
      array_of_requests[i] = PMPI_Request_c2f(c_requests[i]);
    }
  }

  if (index != NULL) {
    if (c_index == MPI_UNDEFINED) {
      *index = (MPI_Fint)MPI_UNDEFINED;
    } else {
      *index = (MPI_Fint)(c_index + 1);
    }
  }

  if (c_status_ptr != MPI_STATUS_IGNORE && status != NULL && c_index != MPI_UNDEFINED) {
    PMPI_Status_c2f(&c_status, status);
  }

  free(c_requests);
}

void mpi_waitany__(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint *index, MPI_Fint *status, MPI_Fint *ierr) {
  mpi_waitany_(count, array_of_requests, index, status, ierr);
}

void MPI_WAITANY(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint *index, MPI_Fint *status, MPI_Fint *ierr) {
  mpi_waitany_(count, array_of_requests, index, status, ierr);
}


void mpi_waitsome_(MPI_Fint *incount, MPI_Fint array_of_requests[], MPI_Fint *outcount, MPI_Fint array_of_indices[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  int i;
  int n = (int)*incount;
  int c_outcount = MPI_UNDEFINED;
  MPI_Request *c_requests = NULL;
  int *c_indices = NULL;
  MPI_Status *c_statuses = NULL;
  MPI_Status *c_status_ptr = MPI_STATUSES_IGNORE;

  if (n > 0) {
    c_requests = (MPI_Request *)malloc((size_t)n * sizeof(MPI_Request));
    c_indices = (int *)malloc((size_t)n * sizeof(int));

    if (c_requests == NULL || c_indices == NULL) {
      free(c_requests);
      free(c_indices);
      *ierr = (MPI_Fint)MPI_ERR_NO_MEM;
      return;
    }

    for (i = 0; i < n; i++) {
      c_requests[i] = PMPI_Request_f2c(array_of_requests[i]);
    }

    if (array_of_statuses != NULL && !fortran_statuses_are_ignore(array_of_statuses)) {
      c_statuses = (MPI_Status *)malloc((size_t)n * sizeof(MPI_Status));
      if (c_statuses == NULL) {
	free(c_requests);
	free(c_indices);
	*ierr = (MPI_Fint)MPI_ERR_NO_MEM;
	return;
      }
      c_status_ptr = c_statuses;
    }
  }

  *ierr = (MPI_Fint)MPI_Waitsome(n, c_requests, &c_outcount, c_indices, c_status_ptr);

  if (n > 0) {
    for (i = 0; i < n; i++) {
      array_of_requests[i] = PMPI_Request_c2f(c_requests[i]);
    }
  }

  if (outcount != NULL) {
    *outcount = (MPI_Fint)c_outcount;
  }

  if (c_outcount != MPI_UNDEFINED && c_outcount > 0) {
    int f_status_size = 0;

    if (c_status_ptr != MPI_STATUSES_IGNORE && array_of_statuses != NULL) {
      if (!validate_fortran_status_size("mpi_waitsome_", &f_status_size, ierr)) {
	free(c_requests);
	free(c_indices);
	free(c_statuses);
	return;
      }
    }
    for (i = 0; i < c_outcount; i++) {
      if (array_of_indices != NULL) {
	array_of_indices[i] = (MPI_Fint)(c_indices[i] + 1);
      }

      if (c_status_ptr != MPI_STATUSES_IGNORE && array_of_statuses != NULL) {
	PMPI_Status_c2f(&c_statuses[i], &array_of_statuses[i * f_status_size]);
      }
    }
  }

  free(c_requests);
  free(c_indices);
  free(c_statuses);
}

void mpi_waitsome__(MPI_Fint *incount, MPI_Fint array_of_requests[], MPI_Fint *outcount, MPI_Fint array_of_indices[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  mpi_waitsome_(incount, array_of_requests, outcount, array_of_indices, array_of_statuses, ierr);
}

void MPI_WAITSOME(MPI_Fint *incount, MPI_Fint array_of_requests[], MPI_Fint *outcount, MPI_Fint array_of_indices[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  mpi_waitsome_(incount, array_of_requests, outcount, array_of_indices, array_of_statuses, ierr);
}


void mpi_testall_(MPI_Fint *count, MPI_Fint array_of_requests[], int *flag, MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  int i;
  int n = (int)*count;
  int c_flag = 0;
  MPI_Request *c_requests = NULL;
  MPI_Status *c_statuses = NULL;
  MPI_Status *c_status_ptr = MPI_STATUSES_IGNORE;

  if (n > 0) {
    c_requests = (MPI_Request *)malloc((size_t)n * sizeof(MPI_Request));
    if (c_requests == NULL) {
      *ierr = (MPI_Fint)MPI_ERR_NO_MEM;
      return;
    }

    for (i = 0; i < n; i++) {
      c_requests[i] = PMPI_Request_f2c(array_of_requests[i]);
    }

    if (array_of_statuses != NULL && !fortran_statuses_are_ignore(array_of_statuses)) {
      c_statuses = (MPI_Status *)malloc((size_t)n * sizeof(MPI_Status));
      if (c_statuses == NULL) {
	free(c_requests);
	*ierr = (MPI_Fint)MPI_ERR_NO_MEM;
	return;
      }
      c_status_ptr = c_statuses;
    }
  }

  *ierr = (MPI_Fint)MPI_Testall(n, c_requests, &c_flag, c_status_ptr);

  if (n > 0) {
    for (i = 0; i < n; i++) {
      array_of_requests[i] = PMPI_Request_c2f(c_requests[i]);
    }
  }

  if (flag != NULL) {
    *flag = c_flag;
  }

  if (c_flag && c_status_ptr != MPI_STATUSES_IGNORE && array_of_statuses != NULL) {
    int f_status_size = 0;

    if (!validate_fortran_status_size("mpi_testall_", &f_status_size, ierr)) {
      free(c_requests);
      free(c_statuses);
      return;
    }

    for (i = 0; i < n; i++) {
      PMPI_Status_c2f(&c_statuses[i], &array_of_statuses[i * f_status_size]);
    }
  }

  free(c_requests);
  free(c_statuses);
}

void mpi_testall__(MPI_Fint *count, MPI_Fint array_of_requests[], int *flag, MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  mpi_testall_(count, array_of_requests, flag, array_of_statuses, ierr);
}

void MPI_TESTALL(MPI_Fint *count, MPI_Fint array_of_requests[], int *flag, MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  mpi_testall_(count, array_of_requests, flag, array_of_statuses, ierr);
}


void mpi_testany_(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint *index, int *flag, MPI_Fint *status, MPI_Fint *ierr) {
  int i;
  int n = (int)*count;
  int c_index = MPI_UNDEFINED;
  int c_flag = 0;
  MPI_Request *c_requests = NULL;
  MPI_Status c_status;
  MPI_Status *c_status_ptr = MPI_STATUS_IGNORE;

  if (n > 0) {
    c_requests = (MPI_Request *)malloc((size_t)n * sizeof(MPI_Request));
    if (c_requests == NULL) {
      *ierr = (MPI_Fint)MPI_ERR_NO_MEM;
      return;
    }

    for (i = 0; i < n; i++) {
      c_requests[i] = PMPI_Request_f2c(array_of_requests[i]);
    }
  }

  if (status != NULL && !fortran_status_is_ignore(status)) {
    c_status_ptr = &c_status;
  }

  *ierr = (MPI_Fint)MPI_Testany(n, c_requests, &c_index, &c_flag, c_status_ptr);

  if (n > 0) {
    for (i = 0; i < n; i++) {
      array_of_requests[i] = PMPI_Request_c2f(c_requests[i]);
    }
  }

  if (flag != NULL) {
    *flag = c_flag;
  }

  if (index != NULL) {
    if (c_index == MPI_UNDEFINED) {
      *index = (MPI_Fint)MPI_UNDEFINED;
    } else {
      *index = (MPI_Fint)(c_index + 1);
    }
  }

  if (c_flag && c_status_ptr != MPI_STATUS_IGNORE && status != NULL && c_index != MPI_UNDEFINED) {
    PMPI_Status_c2f(&c_status, status);
  }

  free(c_requests);
}

void mpi_testany__(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint *index, int *flag, MPI_Fint *status, MPI_Fint *ierr) {
  mpi_testany_(count, array_of_requests, index, flag, status, ierr);
}

void MPI_TESTANY(MPI_Fint *count, MPI_Fint array_of_requests[], MPI_Fint *index, int *flag, MPI_Fint *status, MPI_Fint *ierr) {
  mpi_testany_(count, array_of_requests, index, flag, status, ierr);
}


void mpi_testsome_(MPI_Fint *incount, MPI_Fint array_of_requests[], MPI_Fint *outcount, MPI_Fint array_of_indices[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  int i;
  int n = (int)*incount;
  int c_outcount = MPI_UNDEFINED;
  MPI_Request *c_requests = NULL;
  int *c_indices = NULL;
  MPI_Status *c_statuses = NULL;
  MPI_Status *c_status_ptr = MPI_STATUSES_IGNORE;

  if (n > 0) {
    c_requests = (MPI_Request *)malloc((size_t)n * sizeof(MPI_Request));
    c_indices = (int *)malloc((size_t)n * sizeof(int));

    if (c_requests == NULL || c_indices == NULL) {
      free(c_requests);
      free(c_indices);
      *ierr = (MPI_Fint)MPI_ERR_NO_MEM;
      return;
    }

    for (i = 0; i < n; i++) {
      c_requests[i] = PMPI_Request_f2c(array_of_requests[i]);
    }

    if (array_of_statuses != NULL && !fortran_statuses_are_ignore(array_of_statuses)) {
      c_statuses = (MPI_Status *)malloc((size_t)n * sizeof(MPI_Status));
      if (c_statuses == NULL) {
	free(c_requests);
	free(c_indices);
	*ierr = (MPI_Fint)MPI_ERR_NO_MEM;
	return;
      }
      c_status_ptr = c_statuses;
    }
  }

  *ierr = (MPI_Fint)MPI_Testsome(n, c_requests, &c_outcount, c_indices, c_status_ptr);

  if (n > 0) {
    for (i = 0; i < n; i++) {
      array_of_requests[i] = PMPI_Request_c2f(c_requests[i]);
    }
  }

  if (outcount != NULL) {
    *outcount = (MPI_Fint)c_outcount;
  }

  if (c_outcount != MPI_UNDEFINED && c_outcount > 0) {
    int f_status_size = 0;

    if (c_status_ptr != MPI_STATUSES_IGNORE && array_of_statuses != NULL) {
      if (!validate_fortran_status_size("mpi_testsome_", &f_status_size, ierr)) {
	free(c_requests);
	free(c_indices);
	free(c_statuses);
	return;
      }
    } 
    for (i = 0; i < c_outcount; i++) {
      if (array_of_indices != NULL) {
	array_of_indices[i] = (MPI_Fint)(c_indices[i] + 1);
      }

      if (c_status_ptr != MPI_STATUSES_IGNORE && array_of_statuses != NULL) {
	PMPI_Status_c2f(&c_statuses[i], &array_of_statuses[i * f_status_size]);
      }
    }
  }

  free(c_requests);
  free(c_indices);
  free(c_statuses);
}

void mpi_testsome__(MPI_Fint *incount, MPI_Fint array_of_requests[], MPI_Fint *outcount, MPI_Fint array_of_indices[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  mpi_testsome_(incount, array_of_requests, outcount, array_of_indices, array_of_statuses, ierr);
}

void MPI_TESTSOME(MPI_Fint *incount, MPI_Fint array_of_requests[], MPI_Fint *outcount, MPI_Fint array_of_indices[], MPI_Fint array_of_statuses[], MPI_Fint *ierr) {
  mpi_testsome_(incount, array_of_requests, outcount, array_of_indices, array_of_statuses, ierr);
}

void mpi_bsend_(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Bsend(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*dest, (int)*tag, PMPI_Comm_f2c(*comm));
}
void mpi_bsend__(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) { mpi_bsend_(buf, count, datatype, dest, tag, comm, ierr); }
void MPI_BSEND(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) { mpi_bsend_(buf, count, datatype, dest, tag, comm, ierr); }

void mpi_ssend_(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Ssend(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*dest, (int)*tag, PMPI_Comm_f2c(*comm));
}
void mpi_ssend__(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) { mpi_ssend_(buf, count, datatype, dest, tag, comm, ierr); }
void MPI_SSEND(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) { mpi_ssend_(buf, count, datatype, dest, tag, comm, ierr); }

void mpi_rsend_(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Rsend(buf, (int)*count, PMPI_Type_f2c(*datatype), (int)*dest, (int)*tag, PMPI_Comm_f2c(*comm));
}
void mpi_rsend__(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) { mpi_rsend_(buf, count, datatype, dest, tag, comm, ierr); }
void MPI_RSEND(const void *buf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *dest, MPI_Fint *tag, MPI_Fint *comm, MPI_Fint *ierr) { mpi_rsend_(buf, count, datatype, dest, tag, comm, ierr); }

void mpi_sendrecv_(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, MPI_Fint *dest, MPI_Fint *sendtag, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *source, MPI_Fint *recvtag, MPI_Fint *comm, MPI_Fint *status, MPI_Fint *ierr) {
  MPI_Status c_status;
#ifdef MPI_F_STATUS_IGNORE
  MPI_Status *c_status_ptr = (status == MPI_F_STATUS_IGNORE) ? MPI_STATUS_IGNORE : &c_status;
#else
  MPI_Status *c_status_ptr = &c_status;
#endif
  *ierr = (MPI_Fint)MPI_Sendrecv(sendbuf, (int)*sendcount, PMPI_Type_f2c(*sendtype), (int)*dest, (int)*sendtag, recvbuf, (int)*recvcount, PMPI_Type_f2c(*recvtype), (int)*source, (int)*recvtag, PMPI_Comm_f2c(*comm), c_status_ptr);
  if (c_status_ptr != MPI_STATUS_IGNORE && status != NULL) PMPI_Status_c2f(&c_status, status);
}
void mpi_sendrecv__(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, MPI_Fint *dest, MPI_Fint *sendtag, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *source, MPI_Fint *recvtag, MPI_Fint *comm, MPI_Fint *status, MPI_Fint *ierr) { mpi_sendrecv_(sendbuf, sendcount, sendtype, dest, sendtag, recvbuf, recvcount, recvtype, source, recvtag, comm, status, ierr); }
void MPI_SENDRECV(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, MPI_Fint *dest, MPI_Fint *sendtag, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *source, MPI_Fint *recvtag, MPI_Fint *comm, MPI_Fint *status, MPI_Fint *ierr) { mpi_sendrecv_(sendbuf, sendcount, sendtype, dest, sendtag, recvbuf, recvcount, recvtype, source, recvtag, comm, status, ierr); }

void mpi_barrier_(MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Barrier(PMPI_Comm_f2c(*comm));
}
void mpi_barrier__(MPI_Fint *comm, MPI_Fint *ierr) { mpi_barrier_(comm, ierr); }
void MPI_BARRIER(MPI_Fint *comm, MPI_Fint *ierr) { mpi_barrier_(comm, ierr); }

void mpi_bcast_(void *buffer, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Bcast(buffer, (int)*count, PMPI_Type_f2c(*datatype), (int)*root, PMPI_Comm_f2c(*comm));
}
void mpi_bcast__(void *buffer, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) { mpi_bcast_(buffer, count, datatype, root, comm, ierr); }
void MPI_BCAST(void *buffer, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) { mpi_bcast_(buffer, count, datatype, root, comm, ierr); }

void mpi_reduce_(const void *sendbuf, void *recvbuf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *op, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Reduce(sendbuf, recvbuf, (int)*count, PMPI_Type_f2c(*datatype), PMPI_Op_f2c(*op), (int)*root, PMPI_Comm_f2c(*comm));
}
void mpi_reduce__(const void *sendbuf, void *recvbuf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *op, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) { mpi_reduce_(sendbuf, recvbuf, count, datatype, op, root, comm, ierr); }
void MPI_REDUCE(const void *sendbuf, void *recvbuf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *op, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) { mpi_reduce_(sendbuf, recvbuf, count, datatype, op, root, comm, ierr); }

void mpi_allreduce_(const void *sendbuf, void *recvbuf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *op, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Allreduce(sendbuf, recvbuf, (int)*count, PMPI_Type_f2c(*datatype), PMPI_Op_f2c(*op), PMPI_Comm_f2c(*comm));
}
void mpi_allreduce__(const void *sendbuf, void *recvbuf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *op, MPI_Fint *comm, MPI_Fint *ierr) { mpi_allreduce_(sendbuf, recvbuf, count, datatype, op, comm, ierr); }
void MPI_ALLREDUCE(const void *sendbuf, void *recvbuf, MPI_Fint *count, MPI_Fint *datatype, MPI_Fint *op, MPI_Fint *comm, MPI_Fint *ierr) { mpi_allreduce_(sendbuf, recvbuf, count, datatype, op, comm, ierr); }

void mpi_gather_(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Gather(sendbuf, (int)*sendcount, PMPI_Type_f2c(*sendtype), recvbuf, (int)*recvcount, PMPI_Type_f2c(*recvtype), (int)*root, PMPI_Comm_f2c(*comm));
}
void mpi_gather__(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) { mpi_gather_(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm, ierr); }
void MPI_GATHER(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) { mpi_gather_(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm, ierr); }

void mpi_scatter_(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Scatter(sendbuf, (int)*sendcount, PMPI_Type_f2c(*sendtype), recvbuf, (int)*recvcount, PMPI_Type_f2c(*recvtype), (int)*root, PMPI_Comm_f2c(*comm));
}
void mpi_scatter__(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) { mpi_scatter_(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm, ierr); }
void MPI_SCATTER(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *root, MPI_Fint *comm, MPI_Fint *ierr) { mpi_scatter_(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm, ierr); }

void mpi_allgather_(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *comm, MPI_Fint *ierr) {
  *ierr = (MPI_Fint)MPI_Allgather(sendbuf, (int)*sendcount, PMPI_Type_f2c(*sendtype), recvbuf, (int)*recvcount, PMPI_Type_f2c(*recvtype), PMPI_Comm_f2c(*comm));
}
void mpi_allgather__(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *comm, MPI_Fint *ierr) { mpi_allgather_(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm, ierr); }
void MPI_ALLGATHER(const void *sendbuf, MPI_Fint *sendcount, MPI_Fint *sendtype, void *recvbuf, MPI_Fint *recvcount, MPI_Fint *recvtype, MPI_Fint *comm, MPI_Fint *ierr) { mpi_allgather_(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm, ierr); }

void mpi_cancel_(MPI_Fint *request, MPI_Fint *ierr) {
  MPI_Request c_req = (request != NULL) ? PMPI_Request_f2c(*request) : MPI_REQUEST_NULL;
  *ierr = (MPI_Fint)MPI_Cancel(&c_req);
  if (request != NULL) *request = PMPI_Request_c2f(c_req);
}
void mpi_cancel__(MPI_Fint *request, MPI_Fint *ierr) { mpi_cancel_(request, ierr); }
void MPI_CANCEL(MPI_Fint *request, MPI_Fint *ierr) { mpi_cancel_(request, ierr); }

#endif /* MPI_TRACE_ENABLE_FORTRAN_SUPPORT */
