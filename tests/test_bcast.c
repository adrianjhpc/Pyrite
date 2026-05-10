#include <mpi.h>
#include <stdio.h>

int main(int argc, char **argv) {
    int rank, size;
    int buf[3];
    int i;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 4) {
        if (rank == 0) {
            fprintf(stderr, "test_c_bcast requires 4 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    for (i = 0; i < 3; i++) {
        buf[i] = -1;
    }

    if (rank == 2) {
        buf[0] = 10;
        buf[1] = 20;
        buf[2] = 30;
    }

    MPI_Bcast(buf, 3, MPI_INT, 2, MPI_COMM_WORLD);

    if (buf[0] != 10 || buf[1] != 20 || buf[2] != 30) {
        fprintf(stderr, "rank %d expected [10,20,30], got [%d,%d,%d]\n",
                rank, buf[0], buf[1], buf[2]);
        MPI_Abort(MPI_COMM_WORLD, 2);
    }

    MPI_Finalize();
    return 0;
}

