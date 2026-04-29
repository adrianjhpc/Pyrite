#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

static void require_size(int size, int expected, int rank) {
    if (size != expected) {
        if (rank == 0) {
            fprintf(stderr, "test_send_recv requires %d ranks, got %d\n", expected, size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int main(int argc, char **argv) {
    int rank, size;
    int value = 42;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    require_size(size, 2, rank);

    if (rank == 0) {
        MPI_Send(&value, 1, MPI_INT, 1, 7, MPI_COMM_WORLD);
    } else {
        value = -1;
        MPI_Recv(&value, 1, MPI_INT, 0, 7, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (value != 42) {
            fprintf(stderr, "rank 1 expected 42, got %d\n", value);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Finalize();
    return 0;
}

