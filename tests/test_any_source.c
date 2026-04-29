#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

static void require_size(int size, int expected, int rank) {
    if (size != expected) {
        if (rank == 0) {
            fprintf(stderr, "test_any_source requires %d ranks, got %d\n", expected, size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int main(int argc, char **argv) {
    int rank, size;
    int msg1 = 111;
    int msg2 = 222;
    int go = 1;
    int recvbuf = -1;
    MPI_Status status;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    require_size(size, 3, rank);

    if (rank == 0) {
        MPI_Recv(&recvbuf, 1, MPI_INT, MPI_ANY_SOURCE, 10, MPI_COMM_WORLD, &status);
        if (status.MPI_SOURCE != 1 || recvbuf != 111) {
            fprintf(stderr, "rank 0 expected first ANY_SOURCE message from rank 1 with value 111, got source=%d value=%d\n",
                    status.MPI_SOURCE, recvbuf);
            MPI_Abort(MPI_COMM_WORLD, 2);
        }

        MPI_Send(&go, 1, MPI_INT, 2, 11, MPI_COMM_WORLD);

        recvbuf = -1;
        MPI_Recv(&recvbuf, 1, MPI_INT, 2, 10, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (recvbuf != 222) {
            fprintf(stderr, "rank 0 expected second message value 222, got %d\n", recvbuf);
            MPI_Abort(MPI_COMM_WORLD, 3);
        }
    } else if (rank == 1) {
        MPI_Send(&msg1, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
    } else if (rank == 2) {
        MPI_Recv(&go, 1, MPI_INT, 0, 11, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Send(&msg2, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}

