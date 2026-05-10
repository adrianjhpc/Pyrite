#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;

    // The tracer should intercept this and start the backends
    MPI_Init(&argc, &argv);
    
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (rank == 0) {
        printf("Running bare-minimum Init/Finalize test on %d ranks.\n", size);
    }

    // Do absolutely zero communication. 
    
    // The tracer should intercept this, flush the backends, and tear down gracefully
    MPI_Finalize();
    
    return 0;
}
