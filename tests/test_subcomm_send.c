#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

static void require_size(int size, int expected, int rank) {
    if (size != expected) {
        if (rank == 0) {
            fprintf(stderr, "test_subcomm_send requires %d ranks, got %d\n", expected, size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int main(int argc, char **argv) {
    int rank, size;
    MPI_Comm subcomm;
    int subrank, subsize;
    int value = -1;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    require_size(size, 4, rank);

    MPI_Comm_split(MPI_COMM_WORLD, rank % 2, rank, &subcomm);
    MPI_Comm_rank(subcomm, &subrank);
    MPI_Comm_size(subcomm, &subsize);

    if (subsize != 2) {
        fprintf(stderr, "rank %d expected subcomm size 2, got %d\n", rank, subsize);
        MPI_Abort(MPI_COMM_WORLD, 2);
    }

    if (subrank == 0) {
        value = rank;
        MPI_Send(&value, 1, MPI_INT, 1, 30, subcomm);
    } else {
        MPI_Recv(&value, 1, MPI_INT, 0, 30, subcomm, MPI_STATUS_IGNORE);
    }

    MPI_Comm_free(&subcomm);
    MPI_Finalize();
    return 0;
}

