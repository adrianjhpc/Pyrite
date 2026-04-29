#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

static void require_size(int size, int expected, int rank) {
    if (size != expected) {
        if (rank == 0) {
            fprintf(stderr, "test_nonblocking requires %d ranks, got %d\n", expected, size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int main(int argc, char **argv) {
    int rank, size;
    int send0 = 11;
    int send1 = 22;
    int recv0 = -1;
    int recv1 = -1;
    MPI_Request reqs[2];

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    require_size(size, 2, rank);

    if (rank == 0) {
        MPI_Isend(&send0, 1, MPI_INT, 1, 100, MPI_COMM_WORLD, &reqs[0]);
        MPI_Isend(&send1, 1, MPI_INT, 1, 101, MPI_COMM_WORLD, &reqs[1]);
        MPI_Waitall(2, reqs, MPI_STATUSES_IGNORE);
    } else {
        MPI_Irecv(&recv0, 1, MPI_INT, 0, 100, MPI_COMM_WORLD, &reqs[0]);
        MPI_Irecv(&recv1, 1, MPI_INT, 0, 101, MPI_COMM_WORLD, &reqs[1]);
        MPI_Waitall(2, reqs, MPI_STATUSES_IGNORE);

        if (recv0 != 11 || recv1 != 22) {
            fprintf(stderr, "rank 1 expected (11,22), got (%d,%d)\n", recv0, recv1);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Finalize();
    return 0;
}

