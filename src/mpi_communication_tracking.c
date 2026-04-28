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

double my_time; 
int my_rank;
int my_size;

small_node_t *small_head = NULL;
small_node_t *small_current = NULL;
int small_current_length;
int small_total_length;
int *number_of_small_messages;

large_node_t *large_head = NULL;
large_node_t *large_current = NULL;
int large_current_length;
int large_total_length;
int *number_of_large_messages;

int current_id;

pid_t process_id = -1;
char hostname[STRING_LENGTH];
char programname[STRING_LENGTH];
char datetime[DATETIME_LENGTH];
FILE *global_file;
FILE *small_output_file;
FILE *large_output_file;
process_info_t *processes;


// Function to intercept Fortran MPI call to MPI_FINALIZE. Passes through to the
// C version of MPI_Finalize following this routine.
void mpi_finalize_(int *ierr){
  *ierr = MPI_Finalize();
}

// Function to intercept Fortran MPI call to MPI_INIT. Passes through to the
// C version of MPI_Init following this routine.
void mpi_init_(int *ierr) {
    int argc = 0; char **argv = NULL;
    *ierr = MPI_Init(&argc, &argv);
}

// Fortran 2008 (f08) Bindings
void mpi_finalize_f08_(int *ierr) {
    *ierr = MPI_Finalize();
}

// Fortran 2008 (f08) Bindings
void mpi_init_f08_(int *ierr) {
    int argc = 0; char **argv = NULL;
    *ierr = MPI_Init(&argc, &argv);
}

// Catch uppercase function names produced by compilers just in case 
void MPI_INIT(int *ierr) { mpi_init_(ierr); }
void MPI_FINALIZE(int *ierr) { mpi_finalize_(ierr); }


int MPI_Init_thread(int *argc, char ***argv, int required, int *provided) {

    int err;

    err = PMPI_Init_thread(argc, argv, required, provided);

    PMPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    PMPI_Comm_size(MPI_COMM_WORLD, &my_size);
    PMPI_Barrier(MPI_COMM_WORLD);
    my_time = PMPI_Wtime();

  my_time = MPI_Wtime();

  get_program_name();

  get_process_id();

  gethostname(hostname, STRING_LENGTH);

  get_datetime();
  
  small_head = (small_node_t *) malloc(sizeof(small_node_t));
  if(small_head == NULL){
    printf("Error creating the data structure for storing message data\n");
    return 1;
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

  large_head = (large_node_t *) malloc(sizeof(large_node_t));
  if(large_head == NULL){
    printf("Error creating the data structure for storing message data\n");
    return 1;
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

  open_data_files();

  if(my_rank == 0){
    open_global_file();
  }

  gather_process_information();

  if(my_rank == 0){
    write_global_information();
  }

  return err;

}

int MPI_Init(int *argc, char ***argv){

  int err;
  
  err = PMPI_Init(argc, argv);

  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
  MPI_Comm_size(MPI_COMM_WORLD,&my_size);

  PMPI_Barrier(MPI_COMM_WORLD);  

  my_time = MPI_Wtime();
  
  get_program_name();

  get_process_id();

  gethostname(hostname, STRING_LENGTH);

  get_datetime();

  small_head = (small_node_t *) malloc(sizeof(small_node_t));
  if(small_head == NULL){
    printf("Error creating the data structure for storing message data\n");
    return 1;
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

  large_head = (large_node_t *) malloc(sizeof(large_node_t));
  if(large_head == NULL){
    printf("Error creating the data structure for storing message data\n");
    return 1;
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

  open_data_files();

  if(my_rank == 0){
    open_global_file();
  }

  gather_process_information();

  if(my_rank == 0){
    write_global_information();
  }

  return err;
}


int MPI_Finalize(){
  int iter = 0;
  char statusfile_path[STRING_LENGTH];
  char line[STRING_LENGTH];
  pid_t pid;
  FILE *statusfile;
  char *tokens;
  int node_key;
  int temp_size;
  int temp_rank;
  MPI_Comm temp_comm;
  typedef struct communicator {
    MPI_Comm comm;
    int rank;
    int size;
  } communicator;
  communicator node_comm, world_comm, root_comm;
  
  int memory_used = 0, node_max, node_min, node_total, root_indivi_max, root_indivi_min, root_node_av, root_node_min, root_node_max;  
  
  double min_time, max_time;

  // Get the rank and size of the node communicator this process is involved in.
  PMPI_Comm_size(MPI_COMM_WORLD, &temp_size);
  PMPI_Comm_rank(MPI_COMM_WORLD, &temp_rank);
  world_comm.size = temp_size;
  world_comm.rank = temp_rank;  
  world_comm.comm = MPI_COMM_WORLD;
  
  // Get a integer key for this process that is different for every node
  node_key = mpi_high_water_get_key();
  
  // Use the node key to split the MPI_COMM_WORLD communicator
  PMPI_Comm_split(world_comm.comm, node_key, 0, &temp_comm);
  
  PMPI_Comm_size(temp_comm, &temp_size);
  PMPI_Comm_rank(temp_comm, &temp_rank);
  
  node_comm.comm = temp_comm;
  node_comm.rank = temp_rank;
  node_comm.size = temp_size;
  
  // Now create a communicator that goes across nodes. 
  PMPI_Comm_split(world_comm.comm, node_comm.rank, 0, &temp_comm);
  
  PMPI_Comm_size(temp_comm, &temp_size);
  PMPI_Comm_rank(temp_comm, &temp_rank);
  
  root_comm.comm = temp_comm;
  root_comm.rank = temp_rank;
  root_comm.size = temp_size;
  
  pid = getpid();
  snprintf(statusfile_path, STRING_LENGTH, "/proc/%d/status", pid);
  
  if((statusfile = fopen(statusfile_path, "r"))){
    
    while (fgets(line, STRING_LENGTH, statusfile)) {
      if(strstr(line,"VmPeak")){
         // Ignore
      }else if(strstr(line,"VmHWM")){
        tokens = strtok(line, " ");
        while (tokens != NULL){
          if(iter == 1){
            memory_used = atoi(tokens);
            break;
          }
          tokens = strtok (NULL, " ");
          iter++;
        }
      }
    }

    PMPI_Reduce(&memory_used, &node_max, 1, MPI_INT, MPI_MAX, 0, node_comm.comm);
    PMPI_Reduce(&memory_used, &node_min, 1, MPI_INT, MPI_MIN, 0, node_comm.comm);
    PMPI_Reduce(&memory_used, &node_total, 1, MPI_INT, MPI_SUM, 0, node_comm.comm);
    if(node_comm.rank == 0){
      PMPI_Reduce(&node_max, &root_indivi_max, 1, MPI_INT, MPI_MAX, 0, root_comm.comm);
      PMPI_Reduce(&node_min, &root_indivi_min, 1, MPI_INT, MPI_MIN, 0, root_comm.comm);
      PMPI_Reduce(&node_total, &root_node_max, 1, MPI_INT, MPI_MAX, 0, root_comm.comm);
      PMPI_Reduce(&node_total, &root_node_min, 1, MPI_INT, MPI_MIN, 0, root_comm.comm);      
      PMPI_Reduce(&node_total, &root_node_av, 1, MPI_INT, MPI_SUM, 0, root_comm.comm);
      root_node_av = root_node_av/root_comm.size;
    }
    if(world_comm.rank == 0){
      printf("process max %dMB min %dMB\n",root_indivi_max/1024, root_indivi_min/1024);
      printf("node max %dMB min %dMB avg %dMB\n",root_node_max/1024, root_node_min/1024, root_node_av/1024);
    }
    
  }else{
    printf("%s:%d problem opening /proc/pid/status file\n",hostname,pid);
  }

  PMPI_Allreduce(&my_time, &max_time, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
  PMPI_Allreduce(&my_time, &min_time, 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);

  write_data_output();

  free(small_head);
  free(large_head);

  close_data_file();

  communicate_total_message_numbers();

  if(my_rank == 0){
    process_data_files();
    close_global_file();
  }

  free(number_of_small_messages);
  free(number_of_large_messages);
 
  PMPI_Comm_free(&(node_comm.comm));
  PMPI_Comm_free(&(root_comm.comm));
 
  return PMPI_Finalize();
}

int mpi_high_water_name_to_colour(const char *name){
  int small_multiplier = 31;
  int large_multiplier = 1e9 + 9;
  int res = 0;
  int power = 1;
  const char *p;
  for(p=name; *p ; p++){
    res = (res + (*p + 1) * power) % large_multiplier;
    if(res < 0){
      res = - res;
    }
    power = (power * small_multiplier) % large_multiplier;
  }
  return res;
}

void get_date_time_string(char *datetime){
    time_t now;
    struct tm *local;
    int seconds, minutes, hours, days, months, years;
    time(&now);
    local = localtime(&now);

    seconds = local->tm_sec;
    minutes = local->tm_min;
    hours = local->tm_hour; 
    days = local->tm_mday;  
    months = local->tm_mon + 1; 
    years = local->tm_year + 1900; 

    sprintf(datetime+strlen(datetime), "%d", years);
    sprintf(datetime+strlen(datetime), "%d", months);
    sprintf(datetime+strlen(datetime), "%d", days);
    sprintf(datetime+strlen(datetime), "%d", hours);
    sprintf(datetime+strlen(datetime), "%d", minutes);
    sprintf(datetime+strlen(datetime), "%d", seconds);
}

int open_data_files(){
  char filename[STRING_LENGTH];
  char tempfilename[STRING_LENGTH];

  assert(process_id != -1);

  get_local_filename(filename, hostname, process_id);
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_small");
  small_output_file = fopen(tempfilename,"wb");
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_large");
  large_output_file = fopen(tempfilename,"wb");

  return 0;
}

int open_proc_data_files(FILE **small_input_file, FILE **large_input_file, int proc_id, char *local_hostname){
  char filename[STRING_LENGTH];
  char tempfilename[STRING_LENGTH];

  get_local_filename(filename, local_hostname, proc_id);
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_small");
  *small_input_file = fopen(tempfilename,"rb");
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_large");
  *large_input_file = fopen(tempfilename,"rb");
  
  return 0;
}

int remove_proc_data_files(int proc_id, char *local_hostname){
  char filename[STRING_LENGTH];
  char tempfilename[STRING_LENGTH];

  get_local_filename(filename, local_hostname, proc_id);
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_small");
  unlink(tempfilename);
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_large");
  unlink(tempfilename);

  return 0;
}

int get_local_filename(char *filename, char *local_hostname, int proc_id){
  assert(process_id != -1);

  snprintf(filename, STRING_LENGTH, ".%.256s-%.256s-%d", programname, local_hostname, proc_id);

  return 0;
}

int close_data_file(){
  int err;

  err = fclose(small_output_file);
  err += fclose(large_output_file);

  return err;
}

int open_global_file(){
  char *global_file_name;

  assert(my_rank == 0);

  global_file_name = malloc(sizeof(char)*STRING_LENGTH);
  strcpy(global_file_name, programname);
  strcat(global_file_name, "-");
  get_date_time_string(global_file_name);
  strcat(global_file_name, ".mpic");
  global_file = fopen(global_file_name,"wb");

  free(global_file_name);

  return 0;
}

int get_program_name(){
  char *oldname = NULL, *name, *ptrprogramname;
  ssize_t len; 

  len = readlink("/proc/self/exe", programname, sizeof(programname)-1);
  if (len != -1) {
    programname[len] = '\0';
    ptrprogramname = programname;
    while ((name = strsep(&ptrprogramname, "/"))){
      oldname = name;
    }
    if (oldname) {
      strcpy(programname, oldname);
    }
  } else {
    strcpy(programname, "unknown program\0");
  }

  return 0;
}

int get_datetime(){

  time_t t = time(NULL);
  struct tm *tm = localtime(&t);
  size_t ret = strftime(datetime, sizeof(datetime), "%c", tm);

  return 0;
}

int get_process_id(){
  process_id = getpid();
  return 0;
}

int close_global_file(){
  return fclose(global_file);
}

int gather_process_information(){
  int i;
  int core, chip;
  process_info_t my_process;
  MPI_Status status;
  MPI_Datatype process_type;
  int struct_length = 5;
  int block_lengths[5]; 
  MPI_Datatype block_types[5];
  MPI_Aint block_displacements[5];
  time_t t = time(NULL);

  get_processor_and_core(&chip, &core);

  my_process.rank = my_rank;
  my_process.process_id = process_id;
  my_process.core = core;
  my_process.chip = chip;

  memset(my_process.hostname,0,sizeof(my_process.hostname));
  strcpy(my_process.hostname, hostname);

  block_lengths[0] = 1; 
  block_types[0] = MPI_INT;
  block_displacements[0] = (size_t)&(my_process.rank) - (size_t)&my_process;

  block_lengths[1] = 1; 
  block_types[1] = MPI_INT;
  block_displacements[1] = (size_t)&(my_process.process_id) - (size_t)&my_process;

  block_lengths[2] = 1; 
  block_types[2] = MPI_INT;
  block_displacements[2] = (size_t)&(my_process.core) - (size_t)&my_process;

  block_lengths[3] = 1; 
  block_types[3] = MPI_INT;
  block_displacements[3] = (size_t)&(my_process.chip) - (size_t)&my_process;

  block_lengths[4] = STRING_LENGTH; 
  block_types[4] = MPI_CHAR;
  block_displacements[4] = (size_t)&(my_process.hostname) - (size_t)&my_process;

  MPI_Type_create_struct(struct_length, block_lengths, block_displacements, block_types, &process_type);
  MPI_Type_commit(&process_type);

  if(my_rank == 0){
    processes = malloc(sizeof(process_info_t)*my_size);

    processes[0].rank = my_process.rank;
    processes[0].process_id = my_process.process_id;
    processes[0].core = my_process.core;
    processes[0].chip = my_process.chip;
    strcpy(processes[0].hostname, my_process.hostname);

    for(i=1; i<my_size; i++){
      PMPI_Recv(&processes[i], 1, process_type, i, 0, MPI_COMM_WORLD, &status);
    }
  }else{
    PMPI_Send(&my_process, 1, process_type, 0, 0, MPI_COMM_WORLD);
  }

  MPI_Type_free(&process_type);

  return 0;
}

int write_global_information(){
  int i;
  assert(my_rank == 0);
 
  // Use fixed-size buffers padded with null-terminators
  char fixed_datetime[64] = {0};
  char fixed_programname[256] = {0};
  
  snprintf(fixed_datetime, sizeof(fixed_datetime), "%.63s", datetime);
  snprintf(fixed_programname, sizeof(fixed_programname), "%.255s", programname); 
 
  fwrite(&my_size, sizeof(int), 1, global_file);
  fwrite(fixed_datetime, sizeof(char), 64, global_file);
  fwrite(fixed_programname, sizeof(char), 256, global_file);

  for(i=0; i<my_size; i++){
    fwrite(&processes[i], sizeof(struct process_info), 1, global_file);
  }
  
  return 0;
}

int communicate_total_message_numbers(){
  int i;
  MPI_Status status;

  if(my_rank == 0){
    number_of_small_messages = malloc(sizeof(int)*my_size);
    number_of_small_messages[0] = small_total_length;

    for(i=1; i<my_size; i++){
      PMPI_Recv(&number_of_small_messages[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
    }

    number_of_large_messages = malloc(sizeof(int)*my_size);
    number_of_large_messages[0] = large_total_length;

    for(i=1; i<my_size; i++){
      PMPI_Recv(&number_of_large_messages[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
    }

  }else{
    PMPI_Send(&small_total_length, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    PMPI_Send(&large_total_length, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
  }
  
  return 0;
}

int process_data_files(){
  int i;
  FILE *small_input_file;
  FILE *large_input_file;
  char *line = NULL;
  small_node_no_link_t temp_small;
  large_node_no_link_t temp_large;

  assert(my_rank == 0);

  for(i=0; i<my_size; i++){
    open_proc_data_files(&small_input_file, &large_input_file, processes[i].process_id, processes[i].hostname);
    
    fwrite(&processes[i].rank, sizeof(int), 1, global_file);
    line = malloc(sizeof(char)*24);
    strcpy(line, "P2P Small Type Messages");
    fwrite(line, sizeof(char), 24, global_file);
    free(line);
    
    fwrite(&number_of_small_messages[i], sizeof(int), 1, global_file);
    
    if (small_input_file) {
      while(fread(&temp_small, sizeof(small_node_no_link_t), 1, small_input_file) == 1){
        fwrite(&temp_small, sizeof(small_node_no_link_t), 1, global_file);
      }
      fclose(small_input_file);
    }

    line = malloc(sizeof(char)*24);
    strcpy(line, "P2P Large Type Messages");
    fwrite(line, sizeof(char), 24, global_file);
    free(line);
    
    fwrite(&number_of_large_messages[i], sizeof(int), 1, global_file);
    
    if (large_input_file) {
      while(fread(&temp_large, sizeof(large_node_no_link_t), 1, large_input_file) == 1){
        fwrite(&temp_large, sizeof(large_node_no_link_t), 1, global_file);
      }
      fclose(large_input_file);
    }
    
    remove_proc_data_files(processes[i].process_id, processes[i].hostname);
  }
  return 0;
}

int check_data_limit(){
  int limit = get_data_limit();
  return (small_current_length > limit || large_current_length > limit);
}

int get_data_limit(){
  return 2000;
}

int write_data_output(){
  int i;
  small_node_t *current_small_node;
  large_node_t *current_large_node;
  small_node_no_link_t temp_small_node;
  large_node_no_link_t temp_large_node;

  current_small_node = small_head->next;
  
  for(i=0; i<small_current_length; i++){
    small_node_t *node = current_small_node;
    temp_small_node.time = current_small_node->time;
    temp_small_node.id = current_small_node->id;
    temp_small_node.message_type = current_small_node->message_type;
    temp_small_node.sender = current_small_node->sender;
    temp_small_node.receiver = current_small_node->receiver;
    temp_small_node.count = current_small_node->count;
    temp_small_node.bytes = current_small_node->bytes;

    current_small_node = current_small_node->next;
    free(node);
 
    fwrite(&temp_small_node, sizeof(small_node_no_link_t), 1, small_output_file);
  }

  small_current = small_head;
  small_total_length += small_current_length;
  small_current_length = 0;

  current_large_node = large_head->next;

  for(i=0; i<large_current_length; i++){
    large_node_t *node = current_large_node;
    temp_large_node.time = current_large_node->time;
    temp_large_node.id = current_large_node->id;
    temp_large_node.message_type = current_large_node->message_type;
    temp_large_node.sender1 = current_large_node->sender1;
    temp_large_node.receiver1 = current_large_node->receiver1;
    temp_large_node.count1 = current_large_node->count1;
    temp_large_node.bytes1 = current_large_node->bytes1;
    temp_large_node.sender2 = current_large_node->sender2;
    temp_large_node.receiver2 = current_large_node->receiver2;
    temp_large_node.count2 = current_large_node->count2;
    temp_large_node.bytes2 = current_large_node->bytes2; 
    
    current_large_node = current_large_node->next;
    free(node);
    fwrite(&temp_large_node, sizeof(large_node_no_link_t), 1, large_output_file);
  }

  large_current = large_head;
  large_total_length += large_current_length;
  large_current_length = 0;

  return 0;
}

int mpi_high_water_get_key(){
  char name[MPI_MAX_PROCESSOR_NAME];
  int len;
  int lpar_key;
  
  MPI_Get_processor_name(name, &len);
  lpar_key = mpi_high_water_name_to_colour(name);
  
  return lpar_key;
}

#if defined(__aarch64__)
unsigned long get_processor_and_core(int *chip, int *core){
  return syscall(SYS_getcpu, core, chip, NULL);
}
#else
unsigned long get_processor_and_core(int *chip, int *core){
  unsigned long a,d,c;
  __asm__ volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  *chip = (c & 0xFFF000)>>12;
  *core = c & 0xFFF;
  return ((unsigned long)a) | (((unsigned long)d) << 32);
}
#endif

void add_small_data(int message_type, int sender, int receiver, int count, MPI_Datatype datatype){
  // Calculate delta time using MPI_Wtime
  double temp_time = MPI_Wtime() - my_time;
  
  // Calculate exact byte volume
  int type_size = 0;
  // Some interceptors (like Wait/Barrier) will pass a dummy value. 
  // We only query the size if it's a real communication.
  if (count > 0 &&  datatype != MPI_DATATYPE_NULL) {
      MPI_Type_size(datatype, &type_size);
  }
  int total_bytes = count * type_size;

  small_current->next = (small_node_t *) malloc(sizeof(small_node_t));
  small_current->next->time = temp_time;
  small_current->next->id =  current_id;
  small_current->next->message_type = message_type;
  small_current->next->sender = sender;
  small_current->next->receiver = receiver;
  small_current->next->count = count;
  small_current->next->bytes = total_bytes; 
  small_current->next->next = NULL;
  
  small_current = small_current->next;
  small_current_length++;
  current_id++;

  if(check_data_limit()){
    write_data_output();
  }
}

void add_large_data(int message_type, int sender1, int receiver1, int count1, MPI_Datatype datatype1, int sender2, int receiver2, int count2, MPI_Datatype datatype2){
  // Calculate delta time using MPI_Wtime
  double temp_time = MPI_Wtime() - my_time;

  // Calculate exact byte volume for the first payload (e.g., Send portion)
  int type_size1 = 0;
  if (count1 > 0 && datatype1 != MPI_DATATYPE_NULL) {
      MPI_Type_size(datatype1, &type_size1);
  }
  int total_bytes1 = count1 * type_size1;

  // Calculate exact byte volume for the second payload (e.g., Receive portion)
  int type_size2 = 0;
  if (count2 > 0 && datatype2 != MPI_DATATYPE_NULL) {
      MPI_Type_size(datatype2, &type_size2);
  }
  int total_bytes2 = count2 * type_size2;

  large_current->next = (large_node_t *) malloc(sizeof(large_node_t));
  large_current->next->time = temp_time;
  large_current->next->id = current_id;
  large_current->next->message_type = message_type;
  
  // First data movement
  large_current->next->sender1 = sender1;
  large_current->next->receiver1 = receiver1;
  large_current->next->count1 = count1;
  large_current->next->bytes1 = total_bytes1; 
  
  // Second data movement
  large_current->next->sender2 = sender2;
  large_current->next->receiver2 = receiver2;
  large_current->next->count2 = count2;
  large_current->next->bytes2 = total_bytes2; 
  
  large_current->next->next = NULL;
  
  large_current = large_current->next;
  large_current_length++;
  current_id++;

  if(check_data_limit()){
    write_data_output();
  }
}

void mpi_send_(const void *buf, int *count, MPI_Datatype *datatype, int *dest, int *tag, MPI_Comm *comm, int *ierr){
  // Dereference the pointers to get the actual integers before passing to C
  *ierr = MPI_Send(buf, *count, *datatype, *dest, *tag, *comm);
}

void mpi_recv_(void *buf, int *count, MPI_Datatype *datatype, int *source, int *tag, MPI_Comm *comm, int *status, int *ierr){
  *ierr = MPI_Recv(buf, *count, *datatype, *source, *tag, *comm, (MPI_Status *)status);
}

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm){
  add_small_data(MPI_SEND_TYPE, my_rank, dest, count, datatype);
  return PMPI_Send(buf, count, datatype, dest, tag, comm);
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status){
  add_small_data(MPI_RECV_TYPE, source, my_rank, count, datatype);
  return PMPI_Recv(buf, count, datatype, source, tag, comm, status);
}

int MPI_Bsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm){
  add_small_data(MPI_BSEND_TYPE, my_rank, dest, count, datatype);
  return PMPI_Bsend(buf, count, datatype, dest, tag, comm);
}

int MPI_Ssend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm){
  add_small_data(MPI_SSEND_TYPE, my_rank, dest, count, datatype);
  return PMPI_Ssend(buf, count, datatype, dest, tag, comm);
}

int MPI_Rsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm){
  add_small_data(MPI_RSEND_TYPE, my_rank, dest, count, datatype);
  return PMPI_Rsend(buf, count, datatype, dest, tag, comm);
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){
  add_small_data(MPI_ISEND_TYPE, my_rank, dest, count, datatype);
  return PMPI_Isend(buf, count, datatype, dest, tag, comm, request);
}

int MPI_Ibsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){
  add_small_data(MPI_IBSEND_TYPE, my_rank, dest, count, datatype);
  return PMPI_Ibsend(buf, count, datatype, dest, tag, comm, request);
}

int MPI_Issend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){
  add_small_data(MPI_ISSEND_TYPE, my_rank, dest, count, datatype);
  return PMPI_Issend(buf, count, datatype, dest, tag, comm, request);
}

int MPI_Irsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){
  add_small_data(MPI_IRSEND_TYPE, my_rank, dest, count, datatype);
  return PMPI_Irsend(buf, count, datatype, dest, tag, comm, request);
}

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request){
  add_small_data(MPI_IRECV_TYPE, source, my_rank, count, datatype);
  return PMPI_Irecv(buf, count, datatype, source, tag, comm, request);
}

int MPI_Sendrecv(const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag, void *recvbuf, int recvcount, MPI_Datatype recvtype, int source, int recvtag, MPI_Comm comm, MPI_Status *status){
  add_large_data(MPI_SENDRECV_TYPE, dest, my_rank, sendcount, sendtype, my_rank, dest, recvcount, recvtype);
  return PMPI_Sendrecv(sendbuf, sendcount, sendtype, dest, sendtag, recvbuf, recvcount, recvtype, source, recvtag, comm, status);
}

int MPI_Wait(MPI_Request *request, MPI_Status *status){
  // For a Wait, we track that the local rank completed a wait event.
  // We set sender and receiver to my_rank, and count to 0.
  add_small_data(MPI_WAIT_TYPE, my_rank, my_rank, 0, MPI_DATATYPE_NULL);
  return PMPI_Wait(request, status);
}

int MPI_Waitall(int count, MPI_Request array_of_requests[], MPI_Status array_of_statuses[]){
  // We use the count variable here to track how many requests were waited on.
  // Passing MPI_DATATYPE_NULL prevents MPI_Type_size from attempting to read a dummy datatype.
  add_small_data(MPI_WAITALL_TYPE, my_rank, my_rank, count, MPI_DATATYPE_NULL);
  return PMPI_Waitall(count, array_of_requests, array_of_statuses);
}

int MPI_Barrier(MPI_Comm comm){
  add_small_data(MPI_BARRIER_TYPE, my_rank, my_rank, 0, MPI_DATATYPE_NULL);
  return PMPI_Barrier(comm);
}

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm){
  // Bcast has a clear sender (root) and receiver (everyone else).
  add_small_data(MPI_BCAST_TYPE, root, my_rank, count, datatype);
  return PMPI_Bcast(buffer, count, datatype, root, comm);
}

int MPI_Reduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm){
  // Reduce flows from the local rank to the root rank.
  add_small_data(MPI_REDUCE_TYPE, my_rank, root, count, datatype);
  return PMPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, comm);
}

int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm){
  // Allreduce sends data everywhere, so we log my_rank as both.
  add_small_data(MPI_ALLREDUCE_TYPE, my_rank, my_rank, count, datatype);
  return PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, comm);
}

int MPI_Gather(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm){
  // Gather involves sending (sendcount) to the root, and the root receiving (recvcount).
  // We use the large data struct to capture both data types and counts.
  // If we are not the root, force the receive arguments to 0 and NULL so add_large_data safely ignores them.
  int safe_recvcount = (my_rank == root) ? recvcount : 0;
  MPI_Datatype safe_recvtype = (my_rank == root) ? recvtype : MPI_DATATYPE_NULL;

  add_large_data(MPI_GATHER_TYPE, my_rank, root, sendcount, sendtype, root, my_rank, safe_recvcount, safe_recvtype);
  
  return PMPI_Gather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm);
}

int MPI_Scatter(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm){
  // Scatter involves root sending to the local rank.
  // If we are not the root, force the send arguments to 0 and NULL so add_large_data safely ignores them.
  int safe_sendcount = (my_rank == root) ? sendcount : 0;
  MPI_Datatype safe_sendtype = (my_rank == root) ? sendtype : MPI_DATATYPE_NULL;

  add_large_data(MPI_SCATTER_TYPE, root, my_rank, safe_sendcount, safe_sendtype, my_rank, root, recvcount, recvtype);
  
  return PMPI_Scatter(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm);
}

int MPI_Allgather(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm){
  add_large_data(MPI_ALLGATHER_TYPE, my_rank, my_rank, sendcount, sendtype, my_rank, my_rank, recvcount, recvtype);
  return PMPI_Allgather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
}
