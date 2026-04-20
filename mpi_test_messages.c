#include<mpi.h>
#include<stdio.h>
#include<stdlib.h>


int main(int argc, char **argv){

  int i, repeats, message_size;
  int rank, resultlen;
  char hostname[MPI_MAX_PROCESSOR_NAME];
  double start, stop, diff;
  double send_data;
  double recv_data;
  MPI_Status status;

  send_data = 1;

  message_size = 1;
  repeats = 10;

  MPI_Init(&argc, &argv);
  
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Get_processor_name(hostname, &resultlen);
  
  printf("Hi, I'm %d on %s\n",rank,hostname);
  
  if(rank == 0){
    start = MPI_Wtime();
    for (i=0; i<repeats; i++)  {
      MPI_Send(&send_data, message_size, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD);
      MPI_Recv(&recv_data, message_size, MPI_DOUBLE, 1, 1, MPI_COMM_WORLD, &status);
    }
    stop = MPI_Wtime();
    diff = stop - start;
    printf("%d %d-double messages took %8.8fs\n",repeats, message_size, diff);
  }else{
    for (i=0; i<repeats; i++) {
      MPI_Recv(&send_data, message_size, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, &status);
      MPI_Send(&recv_data, message_size, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
   }
  }
  
  MPI_Finalize();
  return 0;
}

