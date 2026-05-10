#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int sendv, recvv;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 2) {
        if (rank == 0) {
            fprintf(stderr, "test_c_ssend requires 2 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (rank == 0) {
        sendv = 234;
        MPI_Ssend(&sendv, 1, MPI_INT, 1, 310, MPI_COMM_WORLD);
    } else {
        recvv = -1;
        MPI_Recv(&recvv, 1, MPI_INT, 0, 310, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (recvv != 234) {
            fprintf(stderr, "rank 1 expected 234, got %d\n", recvv);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Finalize();
    return 0;
}

