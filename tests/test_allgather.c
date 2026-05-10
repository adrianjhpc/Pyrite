#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int sendbuf;
    int recvbuf[4];
    int i;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 4) {
        if (rank == 0) {
            fprintf(stderr, "test_c_allgather requires 4 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    sendbuf = rank + 10;
    for (i = 0; i < 4; i++) {
        recvbuf[i] = -1;
    }

    MPI_Allgather(&sendbuf, 1, MPI_INT, recvbuf, 1, MPI_INT, MPI_COMM_WORLD);

    for (i = 0; i < 4; i++) {
        int expected = i + 10;
        if (recvbuf[i] != expected) {
            fprintf(stderr, "rank %d expected recvbuf[%d]=%d, got %d\n",
                    rank, i, expected, recvbuf[i]);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Finalize();
    return 0;
}

