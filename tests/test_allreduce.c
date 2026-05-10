#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int sendbuf[4], recvbuf[4];
    int i, expected;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 4) {
        if (rank == 0) {
            fprintf(stderr, "test_c_allreduce requires 4 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    for (i = 0; i < 4; i++) {
        sendbuf[i] = rank + 1;
        recvbuf[i] = -1;
    }

    MPI_Allreduce(sendbuf, recvbuf, 4, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    expected = 1 + 2 + 3 + 4;
    for (i = 0; i < 4; i++) {
        if (recvbuf[i] != expected) {
            fprintf(stderr, "rank %d expected recvbuf[%d]=%d, got %d\n", rank, i, expected, recvbuf[i]);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Finalize();
    return 0;
}

