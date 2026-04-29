#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

static void require_size(int size, int expected, int rank) {
    if (size != expected) {
        if (rank == 0) {
            fprintf(stderr, "test_sendrecv requires %d ranks, got %d\n", expected, size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int main(int argc, char **argv) {
    int rank, size;
    int sendbuf;
    int recvbuf = -1;
    int peer;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    require_size(size, 2, rank);

    peer = 1 - rank;
    sendbuf = 100 + rank;

    MPI_Sendrecv(&sendbuf, 1, MPI_INT, peer, 20,
                 &recvbuf, 1, MPI_INT, peer, 20,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (recvbuf != 100 + peer) {
        fprintf(stderr, "rank %d expected %d, got %d\n", rank, 100 + peer, recvbuf);
        MPI_Abort(MPI_COMM_WORLD, 2);
    }

    MPI_Finalize();
    return 0;
}

