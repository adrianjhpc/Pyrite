#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int sendbuf[28];
    int recvbuf[7];
    int i, j;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 4) {
        if (rank == 0) {
            fprintf(stderr, "test_c_scatter requires 4 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    for (i = 0; i < 28; i++) {
        sendbuf[i] = -1;
    }
    for (i = 0; i < 7; i++) {
        recvbuf[i] = -1;
    }

    if (rank == 0) {
        for (i = 0; i < 4; i++) {
            for (j = 0; j < 7; j++) {
                sendbuf[i * 7 + j] = i * 100 + j;
            }
        }
    }

    MPI_Scatter(sendbuf, 7, MPI_INT, recvbuf, 7, MPI_INT, 0, MPI_COMM_WORLD);

    for (j = 0; j < 7; j++) {
        int expected = rank * 100 + j;
        if (recvbuf[j] != expected) {
            fprintf(stderr, "rank %d expected recvbuf[%d]=%d, got %d\n",
                    rank, j, expected, recvbuf[j]);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Finalize();
    return 0;
}

