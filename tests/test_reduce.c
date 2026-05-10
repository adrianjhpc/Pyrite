#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int sendbuf[5], recvbuf[5];
    int i, expected_sum;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 4) {
        if (rank == 0) {
            fprintf(stderr, "test_c_reduce requires 4 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    for (i = 0; i < 5; i++) {
        sendbuf[i] = rank + 1;
        recvbuf[i] = -1;
    }

    MPI_Reduce(sendbuf, recvbuf, 5, MPI_INT, MPI_SUM, 1, MPI_COMM_WORLD);

    if (rank == 1) {
        expected_sum = 1 + 2 + 3 + 4;
        for (i = 0; i < 5; i++) {
            if (recvbuf[i] != expected_sum) {
                fprintf(stderr, "rank 1 expected recvbuf[%d]=%d, got %d\n",
                        i, expected_sum, recvbuf[i]);
                MPI_Abort(MPI_COMM_WORLD, 2);
            }
        }
    }

    MPI_Finalize();
    return 0;
}

