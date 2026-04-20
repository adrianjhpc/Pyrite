#include <sys/types.h>
#include <unistd.h>
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <assert.h>

#include "mpi_communication_tracking.h"

int mpi_high_water_name_to_colour(const char *);
int mpi_high_water_get_key();
void get_date_time_string(char *);
unsigned long get_processor_and_core(int *chip, int *core);

long int my_time;
int my_rank;
int my_size;

p2p_small_node_t *p2p_small_head = NULL;
p2p_small_node_t *p2p_small_current = NULL;
int p2p_small_current_length;
int p2p_small_total_length;
int *number_of_small_messages;

p2p_large_node_t *p2p_large_head = NULL;
p2p_large_node_t *p2p_large_current = NULL;
int p2p_large_current_length;
int p2p_large_total_length;
int *number_of_large_messages;

int current_id;

pid_t process_id = -1;
char hostname[STRING_LENGTH];
char programname[STRING_LENGTH];
FILE *global_file;
FILE *p2p_small_output_file;
FILE *p2p_large_output_file;
process_info_t *processes;




// Function to intercept Fortran MPI call to MPI_FINALIZE. Passes through to the
// C version of MPI_Finalize following this routine.
void mpi_finalize_(int *ierr){
  *ierr = MPI_Finalize();
}


// Function to intercept Fortran MPI call to MPI_INIT. Passes through to the
// C version of MPI_Init following this routine.
void mpi_init_(int *ierr){
  *ierr = MPI_Init(NULL,NULL);
}


int MPI_Init(int *argc, char ***argv){

  int err;
  
  err = PMPI_Init(argc, argv);

  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
  MPI_Comm_size(MPI_COMM_WORLD,&my_size);
  
  MPI_Barrier(MPI_COMM_WORLD);

  my_time = (long int)time(NULL);
  
  get_program_name();

  get_process_id();

  gethostname(hostname, STRING_LENGTH);

  p2p_small_head = (p2p_small_node_t *) malloc(sizeof(p2p_small_node_t));
  if(p2p_small_head == NULL){
    printf("Error creating the data structure for storing message data\n");
    return 1;
  }
  p2p_small_head->time = 0;
  p2p_small_head->id = -1;
  p2p_small_head->message_type = -1;
  p2p_small_head->sender = -1;
  p2p_small_head->receiver = -1;
  p2p_small_head->count = -1;
  p2p_small_head->datatype = -1;
  p2p_small_head->next = NULL;

  p2p_small_current = p2p_small_head;
  p2p_small_current_length = 0;
  p2p_small_total_length = 0;

  p2p_large_head = (p2p_large_node_t *) malloc(sizeof(p2p_large_node_t));
  if(p2p_large_head == NULL){
    printf("Error creating the data structure for storing message data\n");
    return 1;
  }

  p2p_large_head->time = 0;
  p2p_large_head->id = -1;
  p2p_large_head->message_type = -1;
  p2p_large_head->sender1 = -1;
  p2p_large_head->receiver1 = -1;
  p2p_large_head->count1 = -1;
  p2p_large_head->datatype1 = -1;
  p2p_large_head->sender2 = -1;
  p2p_large_head->receiver2 = -1;
  p2p_large_head->count2 = -1;
  p2p_large_head->datatype2 = -1;
  p2p_large_head->next = NULL;

  p2p_large_current = p2p_large_head;
  p2p_large_current_length = 0;
  p2p_large_total_length = 0;

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
  int ierr;
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
  int memory_used, node_max, node_min, node_total, root_indivi_max, root_indivi_min, root_node_av, root_node_min, root_node_max;  
  long int min_time, max_time, offset_time;
  int messages;

// Get the rank and size of the node communicator this process is involved
  // in.
  MPI_Comm_size(MPI_COMM_WORLD, &temp_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &temp_rank);
  world_comm.size = temp_size;
  world_comm.rank = temp_rank;  


  world_comm.comm = MPI_COMM_WORLD;
  
  // Get a integer key for this process that is different for every node
  // a process is run on.
  node_key = mpi_high_water_get_key();
  
  // Use the node key to split the MPI_COMM_WORLD communicator
  // to produce a communicator per node, containing all the processes
  // running on a given node.
  MPI_Comm_split(world_comm.comm, node_key, 0, &temp_comm);
  
  // Get the rank and size of the node communicator this process is involved
  // in.
  MPI_Comm_size(temp_comm, &temp_size);
  MPI_Comm_rank(temp_comm, &temp_rank);
  
  node_comm.comm = temp_comm;
  node_comm.rank = temp_rank;
  node_comm.size = temp_size;
  
  // Now create a communicator that goes across nodes. The functionality below will
  // create a communicator per rank on a node (i.e. one containing all the rank 0 processes
  // in the node communicators, one containing all the rank 1 processes in the
  // node communicators, etc...), although we are really only doing this to enable
  // all the rank 0 processes in the node communicators to undertake collective operations.
  MPI_Comm_split(world_comm.comm, node_comm.rank, 0, &temp_comm);
  
  MPI_Comm_size(temp_comm, &temp_size);
  MPI_Comm_rank(temp_comm, &temp_rank);
  
  root_comm.comm = temp_comm;
  root_comm.rank = temp_rank;
  root_comm.size = temp_size;
  
  pid = getpid();
  snprintf(statusfile_path, STRING_LENGTH, "/proc/%d/status", pid);
  
  if(statusfile = fopen(statusfile_path, "r")){
    
    while (fgets(line, STRING_LENGTH, statusfile)) {
      if(strstr(line,"VmPeak")){
	//              printf("%s:%d: %s",hostname,pid,line);
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

    MPI_Reduce(&memory_used, &node_max, 1, MPI_INT, MPI_MAX, 0, node_comm.comm);
    MPI_Reduce(&memory_used, &node_min, 1, MPI_INT, MPI_MIN, 0, node_comm.comm);
    MPI_Reduce(&memory_used, &node_total, 1, MPI_INT, MPI_SUM, 0, node_comm.comm);
    if(node_comm.rank == 0){
      MPI_Reduce(&node_max, &root_indivi_max, 1, MPI_INT, MPI_MAX, 0, root_comm.comm);
      MPI_Reduce(&node_min, &root_indivi_min, 1, MPI_INT, MPI_MIN, 0, root_comm.comm);
      MPI_Reduce(&node_total, &root_node_max, 1, MPI_INT, MPI_MAX, 0, root_comm.comm);
      MPI_Reduce(&node_total, &root_node_min, 1, MPI_INT, MPI_MIN, 0, root_comm.comm);      
      MPI_Reduce(&node_total, &root_node_av, 1, MPI_INT, MPI_SUM, 0, root_comm.comm);
      root_node_av = root_node_av/root_comm.size;
    }
    if(world_comm.rank == 0){
      printf("process max %dMB min %dMB\n",root_indivi_max/1024, root_indivi_min/1024);
      printf("node max %dMB min %dMB avg %dMB\n",root_node_max/1024, root_node_min/1024, root_node_av/1024);
    }
    
  }else{
    printf("%s:%d problem opening /proc/pid/status file\n",hostname,pid);
  }

  MPI_Allreduce(&my_time, &max_time, 1, MPI_LONG, MPI_MAX, MPI_COMM_WORLD);
  MPI_Allreduce(&my_time, &min_time, 1, MPI_LONG, MPI_MIN, MPI_COMM_WORLD);

  offset_time = my_time - min_time;
  
  write_data_output();

  free(p2p_small_head);
  free(p2p_large_head);

  close_data_file();

  communicate_total_message_numbers();

  if(my_rank == 0){

    process_data_files();
    close_global_file();

  }

  free(number_of_small_messages);
  free(number_of_large_messages);
  
  return PMPI_Finalize();
}

// The routine convert a string (name) into a number
// for use in a MPI_Comm_split call (where the number is
// known as a colour). It is effectively a hashing function
// for strings but is not necessarily robust (i.e. does not
// guarantee it is collision free) for all strings, but it
// should be reasonable for strings that different by small
// amounts (i.e the name of nodes where they different by a
// number of set of numbers and letters, for instance
// login01,login02..., or cn01q94,cn02q43, etc...)
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

  int err = 0;
  char filename[STRING_LENGTH];
  char tempfilename[STRING_LENGTH];

  assert(process_id != -1);

  get_local_filename(filename, hostname, process_id);
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_p2p_small");
  p2p_small_output_file = fopen(tempfilename,"wb");
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_p2p_large");
  p2p_large_output_file = fopen(tempfilename,"wb");

  return err;

}

int open_proc_data_files(FILE **p2p_small_input_file, FILE **p2p_large_input_file, int proc_id, char *local_hostname){

  int err;
  char filename[STRING_LENGTH];
  char tempfilename[STRING_LENGTH];

  get_local_filename(filename, local_hostname, proc_id);
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_p2p_small");
  *p2p_small_input_file = fopen(tempfilename,"rb");
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_p2p_large");
  *p2p_large_input_file = fopen(tempfilename,"rb");
  return err;

}

int remove_proc_data_files(int proc_id, char *hostname){

  int err;
  char filename[STRING_LENGTH];
  char tempfilename[STRING_LENGTH];

  get_local_filename(filename, hostname, proc_id);
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_p2p_small");
  unlink(tempfilename);
  strcpy(tempfilename, filename);
  strcat(tempfilename, "_p2p_large");
  unlink(tempfilename);

  return err;

}


int get_local_filename(char *filename, char *hostname, int proc_id){

  assert(process_id != -1);

  filename[0] = '.';
  filename[1] = '\0';
  strcat(filename, programname);
  strcat(filename, "-");
  strcat(filename, hostname);
  strcat(filename, "-");
  sprintf(filename+strlen(filename), "%d", proc_id);

  return 0;

}

int close_data_file(){

  int err;

  err = fclose(p2p_small_output_file);
  err += fclose(p2p_large_output_file);

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

  int err;
  char *oldname, *name, *ptrprogramname;

  err = readlink("/proc/self/exe",programname,sizeof(programname)-1);
  ptrprogramname = programname;
  while ((name = strsep(&ptrprogramname, "/"))){
    oldname = name;
  }
  strcpy(programname, oldname);

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
  int block_lengths[struct_length]; 
  MPI_Datatype block_types[struct_length];
  MPI_Aint block_displacements[struct_length];
  MPI_Aint current_displacement=0;

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
  
  //  fprintf(global_file,"%d\n", my_size);
  fwrite(&my_size, sizeof(int), 1, global_file);

  for(i=0; i<my_size; i++){
    //    fprintf(global_file, "%d %d %d %d %s\n", processes[i].rank, processes[i].process_id, processes[i].core, processes[i].chip, processes[i].hostname);
    fwrite(&processes[i], sizeof(struct process_info), 1, global_file);
  }

}

int communicate_total_message_numbers(){

  int i;
  MPI_Status status;

  if(my_rank == 0){

    number_of_small_messages = malloc(sizeof(int)*my_size);
    number_of_small_messages[0] = p2p_small_total_length;

    for(i=1; i<my_size; i++){
      PMPI_Recv(&number_of_small_messages[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
    }

    number_of_large_messages = malloc(sizeof(int)*my_size);
    number_of_large_messages[0] = p2p_large_total_length;

    for(i=1; i<my_size; i++){
      PMPI_Recv(&number_of_large_messages[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
    }

  }else{

    PMPI_Send(&p2p_small_total_length, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    PMPI_Send(&p2p_large_total_length, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

  }
  
  return 0;


}

int process_data_files(){

  int i;
  FILE *p2p_small_input_file;
  FILE *p2p_large_input_file;
  char *line = NULL;
  size_t line_length, read_length;
  p2p_small_node_no_link_t temp_p2p_small;
  p2p_large_node_no_link_t temp_p2p_large;


  assert(my_rank == 0);

  for(i=0; i<my_size; i++){
    open_proc_data_files(&p2p_small_input_file, &p2p_large_input_file, processes[i].process_id, processes[i].hostname);
    //    fprintf(global_file,"%d\n", processes[i].rank);
    fwrite(&processes[i].rank, sizeof(int), 1, global_file);
    //fprintf(global_file,"P2P Small Type Messages\n");
    line = malloc(sizeof(char)*24);
    strcpy(line, "P2P Small Type Messages");
    fwrite(line, sizeof(char), 24, global_file);
    free(line);
    fwrite(&number_of_small_messages[i], sizeof(int), 1, global_file);
    //    line_length = 0;
    //    while ((read_length = getline(&line, &line_length, p2p_small_input_file)) != -1) {
      //      fprintf(global_file, "%s", line);
    //      fwrite(&line, sizeof(char), strlen(line), global_file);
    //    }
  //    free(line);
    while(fread(&temp_p2p_small, sizeof(p2p_small_node_no_link_t), 1, p2p_small_input_file) == 1){
      fwrite(&temp_p2p_small, sizeof(p2p_small_node_no_link_t), 1, global_file);
    }
    fclose(p2p_small_input_file);
    //    fprintf(global_file,"P2P Large Type Messages\n");
    line = malloc(sizeof(char)*24);
    strcpy(line, "P2P Large Type Messages");
    fwrite(line, sizeof(char), 24, global_file);
    free(line);
    fwrite(&number_of_large_messages[i], sizeof(int), 1, global_file);
    //    line_length = 0
    while(fread(&temp_p2p_large, sizeof(p2p_large_node_no_link_t), 1, p2p_large_input_file) == 1){
      fwrite(&temp_p2p_large, sizeof(p2p_large_node_no_link_t), 1, global_file);
    }
    //    while ((read_length = getline(&line, &line_length, p2p_large_input_file)) != -1) {
      //      fprintf(global_file, "%s", line);
    //  fwrite(&line, sizeof(char), strlen(line), global_file);
    // }
      //    free(line);
    fclose(p2p_large_input_file);
    remove_proc_data_files(processes[i].process_id, processes[i].hostname);
  }
  return 0;

}

int check_data_limit(){

  int limit = get_data_limit();

  return (p2p_small_current_length > limit || p2p_large_current_length > limit);

}

int get_data_limit(){

  return 2000;

}

int write_data_output(){

  int i;
  p2p_small_node_t *current_small_node;
  p2p_large_node_t *current_large_node;
  p2p_small_node_no_link_t temp_small_node;
  p2p_large_node_no_link_t temp_large_node;

  current_small_node = p2p_small_head->next;
  

  for(i=0; i<p2p_small_current_length; i++){
    p2p_small_node_t *node = current_small_node;
    //    int time = current_small_node->time;
    //  int id = current_small_node->id;
    //  int message_type = current_small_node->message_type;
    // int sender = current_small_node->sender;
    // int receiver = current_small_node->receiver;
   // int count = current_small_node->count;
    // int datatype = current_small_node->datatype;
    temp_small_node.time = current_small_node->time;
    temp_small_node.id = current_small_node->id;
    temp_small_node.message_type = current_small_node->message_type;
    temp_small_node.sender = current_small_node->sender;
    temp_small_node.receiver = current_small_node->receiver;
    temp_small_node.count = current_small_node->count;
    temp_small_node.datatype = current_small_node->datatype;

    current_small_node = current_small_node->next;
    free(node);
    //    fprintf(p2p_small_output_file, "%d %d %d %d %d %d %d\n", time, id, message_type, sender, receiver, count, datatype);
 
    fwrite(&temp_small_node, sizeof(p2p_small_node_no_link_t), 1, p2p_small_output_file);
 

    //    fwrite(&time, sizeof(int), 1, p2p_small_output_file);
    //fwrite(&id, sizeof(int), 1, p2p_small_output_file);
    //fwrite(&message_type, sizeof(int), 1, p2p_small_output_file);
    // fwrite(&sender, sizeof(int), 1, p2p_small_output_file);
    //fwrite(&receiver, sizeof(int), 1, p2p_small_output_file);
    // fwrite(&count, sizeof(int), 1, p2p_small_output_file);
    //fwrite(&datatype, sizeof(int), 1, p2p_small_output_file);
  }

  p2p_small_current = p2p_small_head;
  p2p_small_total_length += p2p_small_current_length;
  p2p_small_current_length = 0;

  current_large_node = p2p_large_head->next;

  for(i=0; i<p2p_large_current_length; i++){
    p2p_large_node_t *node = current_large_node;
    // int time = current_large_node->time;
    // int id = current_large_node->id;
    // int message_type = current_large_node->message_type;
    // int sender1 = current_large_node->sender1;
    // int receiver1 = current_large_node->receiver1;
    //  int count1 = current_large_node->count1;
    //   int datatype1 = current_large_node->datatype1;
    // int sender2 = current_large_node->sender2;
    //  int receiver2 = current_large_node->receiver2;
    //   int count2 = current_large_node->count2;
    //    int datatype2 = current_large_node->datatype2;
    temp_large_node.time = current_large_node->time;
    temp_large_node.id = current_large_node->id;
    temp_large_node.message_type = current_large_node->message_type;
    temp_large_node.sender1 = current_large_node->sender1;
    temp_large_node.receiver1 = current_large_node->receiver1;
    temp_large_node.count1 = current_large_node->count1;
    temp_large_node.datatype1 = current_large_node->datatype1;
    temp_large_node.sender2 = current_large_node->sender2;
    temp_large_node.receiver2 = current_large_node->receiver2;
    temp_large_node.count2 = current_large_node->count2;
    temp_large_node.datatype2 = current_large_node->datatype2;
    current_large_node = current_large_node->next;
    free(node);
    //    fprintf(p2p_large_output_file, "%d %d %d %d %d %d %d %d %d %d %d\n", time, id, message_type, sender1, receiver1, count1, datatype1, sender2, receiver2, count2, datatype2);
    //    fwrite(&time, sizeof(int), 1, p2p_small_output_file);
    //  fwrite(&id, sizeof(int), 1, p2p_small_output_file);
    //  fwrite(&message_type, sizeof(int), 1, p2p_small_output_file);
    //fwrite(&sender1, sizeof(int), 1, p2p_small_output_file);
    // fwrite(&receiver1, sizeof(int), 1, p2p_small_output_file);
    //  fwrite(&count1, sizeof(int), 1, p2p_small_output_file);
    //fwrite(&datatype1, sizeof(int), 1, p2p_small_output_file);
    // fwrite(&sender2, sizeof(int), 1, p2p_small_output_file);
    // fwrite(&receiver2, sizeof(int), 1, p2p_small_output_file);
    // fwrite(&count2, sizeof(int), 1, p2p_small_output_file);
 //  fwrite(&datatype2, sizeof(int), 1, p2p_small_output_file);
    fwrite(&temp_large_node, sizeof(p2p_large_node_no_link_t), 1, p2p_large_output_file);
  }

  p2p_large_current = p2p_large_head;
  p2p_large_total_length += p2p_large_current_length;
  p2p_large_current_length = 0;

  return 0;

}

// Get an integer key for a process based on the name of the
// node this process is running on. This is useful for creating
// communicators for all the processes running on a node.
int mpi_high_water_get_key(){
  
  char name[MPI_MAX_PROCESSOR_NAME];
  int len;
  int lpar_key;
  
  MPI_Get_processor_name(name, &len);
  lpar_key = mpi_high_water_name_to_colour(name);
  
  return lpar_key;
  
}

#if defined(__aarch64__)
// TODO: This might be general enough to provide the functionality for any system
// regardless of processor type given we aren't worried about thread/process migration.
// Test on Intel systems and see if we can get rid of the architecture specificity
// of the code.
unsigned long get_processor_and_core(int *chip, int *core){
  return syscall(SYS_getcpu, core, chip, NULL);
}
// TODO: Add in AMD function
#else
// If we're not on an ARM processor assume we're on an intel processor and use the
// rdtscp instruction.
unsigned long get_processor_and_core(int *chip, int *core){
  unsigned long a,d,c;
  __asm__ volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  *chip = (c & 0xFFF000)>>12;
  *core = c & 0xFFF;
  return ((unsigned long)a) | (((unsigned long)d) << 32);;
}
#endif

void add_p2p_small_data(int message_type, int sender, int receiver, int count, int datatype){

  int temp_time;

  p2p_small_current->next = (p2p_small_node_t *) malloc(sizeof(p2p_small_node_t));
  temp_time = (int)(((long int)time(NULL)) - my_time);
  p2p_small_current->next->time = temp_time;
  p2p_small_current->next->id =  current_id;
  p2p_small_current->next->message_type = message_type;
  p2p_small_current->next->sender = sender;
  p2p_small_current->next->receiver = receiver;
  p2p_small_current->next->count = count;
  p2p_small_current->next->datatype = datatype;
  p2p_small_current->next->next = NULL;
  p2p_small_current = p2p_small_current->next;
  p2p_small_current_length++;
  current_id++;

  if(check_data_limit()){

    write_data_output();

  }

  return;

}

void add_p2p_large_data(int message_type, int sender1, int receiver1, int count1, int datatype1, int sender2, int receiver2, int count2, int datatype2){

  int temp_time;

  p2p_large_current->next = (p2p_large_node_t *) malloc(sizeof(p2p_large_node_t));
  temp_time = (int)(((long int)time(NULL)) - my_time);
  p2p_large_current->next->time = temp_time;
  p2p_large_current->next->id =  current_id;
  p2p_large_current->next->message_type = message_type;
  p2p_large_current->next->sender1 = sender1;
  p2p_large_current->next->receiver1 = receiver1;
  p2p_large_current->next->count1 = count1;
  p2p_large_current->next->datatype2 = datatype2;
  p2p_large_current->next->sender2 = sender2;
  p2p_large_current->next->receiver2 = receiver2;
  p2p_large_current->next->count2 = count2;
  p2p_large_current->next->datatype1 = datatype2;
  p2p_large_current->next->next = NULL;
  p2p_large_current = p2p_large_current->next;
  p2p_large_current_length++;
  current_id++;

  if(check_data_limit()){

    write_data_output();

  }

  return;

}

int mpi_send_(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, int *ierr){

  *ierr =  MPI_Send(buf, count, datatype, dest, tag, comm);

}

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm){

  add_p2p_small_data(MPI_SEND_TYPE, my_rank, dest, count, datatype);

  return PMPI_Send(buf, count, datatype, dest, tag, comm);

}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status){

  add_p2p_small_data(MPI_RECV_TYPE, source, my_rank, count, datatype);

  return PMPI_Recv(buf, count, datatype, source, tag, comm, status);

}

int MPI_Bsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm){

  add_p2p_small_data(MPI_BSEND_TYPE, my_rank, dest, count, datatype);

  return PMPI_Bsend(buf, count, datatype, dest, tag, comm);

}

int MPI_Ssend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm){

  add_p2p_small_data(MPI_SSEND_TYPE, my_rank, dest, count, datatype);

  return PMPI_Ssend(buf, count, datatype, dest, tag, comm);

}

int MPI_Rsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm){

  add_p2p_small_data(MPI_RSEND_TYPE, my_rank, dest, count, datatype);

  return PMPI_Rsend(buf, count, datatype, dest, tag, comm);

}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){

  add_p2p_small_data(MPI_ISEND_TYPE, my_rank, dest, count, datatype);

  return PMPI_Isend(buf, count, datatype, dest, tag, comm, request);

}

int MPI_Ibsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){

  add_p2p_small_data(MPI_IBSEND_TYPE, my_rank, dest, count, datatype);

  return PMPI_Ibsend(buf, count, datatype, dest, tag, comm, request);

}

int MPI_Issend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){

  add_p2p_small_data(MPI_ISSEND_TYPE, my_rank, dest, count, datatype);

  return PMPI_Issend(buf, count, datatype, dest, tag, comm, request);

}

int MPI_Irsend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){

  add_p2p_small_data(MPI_IRSEND_TYPE, my_rank, dest, count, datatype);

  return PMPI_Irsend(buf, count, datatype, dest, tag, comm, request);

}

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request){

  add_p2p_small_data(MPI_IRECV_TYPE, source, my_rank, count, datatype);

  return PMPI_Irecv(buf, count, datatype, source, tag, comm, request);

}

int MPI_Sendrecv(const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag, void *recvbuf, int recvcount, MPI_Datatype recvtype, int source, int recvtag, MPI_Comm comm, MPI_Status *status){

  add_p2p_large_data(MPI_SENDRECV_TYPE,dest, my_rank, sendcount, sendtype, my_rank, dest, recvcount, recvtype);

  return PMPI_Sendrecv(sendbuf, sendcount, sendtype, dest, sendtag, recvbuf, recvcount, recvtype, source, recvtag, comm, status);

}
                 
