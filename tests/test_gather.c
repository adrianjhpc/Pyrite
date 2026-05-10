#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int sendbuf[2];
    int recvbuf[8];
    int i;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 4) {
        if (rank == 0) {
            fprintf(stderr, "test_c_gather requires 4 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    sendbuf[0] = rank * 10 + 0;
    sendbuf[1] = rank * 10 + 1;

    for (i = 0; i < 8; i++) {
        recvbuf[i] = -1;
    }

    MPI_Gather(sendbuf, 2, MPI_INT, recvbuf, 2, MPI_INT, 3, MPI_COMM_WORLD);

    if (rank == 3) {
        for (i = 0; i < 4; i++) {
            if (recvbuf[2 * i + 0] != i * 10 + 0 || recvbuf[2 * i + 1] != i * 10 + 1) {
                fprintf(stderr, "rank 3 gather mismatch at source rank %d: got [%d,%d]\n",
                        i, recvbuf[2 * i + 0], recvbuf[2 * i + 1]);
                MPI_Abort(MPI_COMM_WORLD, 2);
            }
        }
    }

    MPI_Finalize();
    return 0;
}

