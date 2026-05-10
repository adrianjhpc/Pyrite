#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int value = -1;
    int cancelled = 0;
    MPI_Request req;
    MPI_Status status;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 2) {
        if (rank == 0) {
            fprintf(stderr, "test_c_cancel requires 2 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (rank == 1) {
        MPI_Irecv(&value, 1, MPI_INT, 0, 360, MPI_COMM_WORLD, &req);
        MPI_Cancel(&req);
        MPI_Wait(&req, &status);
        MPI_Test_cancelled(&status, &cancelled);

        if (!cancelled) {
            fprintf(stderr, "rank 1 expected cancelled receive\n");
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();
    return 0;
}

