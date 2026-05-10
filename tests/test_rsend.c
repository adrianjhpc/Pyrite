#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int ready, sendv, recvv;
    MPI_Request req;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 2) {
        if (rank == 0) {
            fprintf(stderr, "test_c_rsend requires 2 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (rank == 0) {
        MPI_Recv(&ready, 1, MPI_INT, 1, 321, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        sendv = 345;
        MPI_Rsend(&sendv, 1, MPI_INT, 1, 320, MPI_COMM_WORLD);
    } else {
        recvv = -1;
        ready = 1;

        MPI_Irecv(&recvv, 1, MPI_INT, 0, 320, MPI_COMM_WORLD, &req);
        MPI_Send(&ready, 1, MPI_INT, 0, 321, MPI_COMM_WORLD);
        MPI_Wait(&req, MPI_STATUS_IGNORE);

        if (recvv != 345) {
            fprintf(stderr, "rank 1 expected 345, got %d\n", recvv);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Finalize();
    return 0;
}

