#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int sendbuf[2] = {500, 501};
    int recvbuf[2] = {-1, -1};
    MPI_Request reqs[2];

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 2) {
        if (rank == 0) {
            fprintf(stderr, "test_c_waitall requires 2 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    reqs[0] = MPI_REQUEST_NULL;
    reqs[1] = MPI_REQUEST_NULL;

    if (rank == 0) {
        MPI_Isend(&sendbuf[0], 1, MPI_INT, 1, 220, MPI_COMM_WORLD, &reqs[0]);
        MPI_Isend(&sendbuf[1], 1, MPI_INT, 1, 221, MPI_COMM_WORLD, &reqs[1]);
        MPI_Waitall(2, reqs, MPI_STATUSES_IGNORE);
    } else {
        MPI_Irecv(&recvbuf[0], 1, MPI_INT, 0, 220, MPI_COMM_WORLD, &reqs[0]);
        MPI_Irecv(&recvbuf[1], 1, MPI_INT, 0, 221, MPI_COMM_WORLD, &reqs[1]);
        MPI_Waitall(2, reqs, MPI_STATUSES_IGNORE);

        if (recvbuf[0] != 500 || recvbuf[1] != 501) {
            fprintf(stderr, "rank 1 expected [500,501], got [%d,%d]\n", recvbuf[0], recvbuf[1]);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Finalize();
    return 0;
}

