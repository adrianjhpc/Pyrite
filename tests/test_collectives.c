#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

static void require_size(int size, int expected, int rank) {
    if (size != expected) {
        if (rank == 0) {
            fprintf(stderr, "test_collectives requires %d ranks, got %d\n", expected, size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

static void fail(int rank, const char *msg) {
    fprintf(stderr, "rank %d: %s\n", rank, msg);
    MPI_Abort(MPI_COMM_WORLD, 2);
}

int main(int argc, char **argv) {
    int rank, size;
    int i;

    int bcast_buf[3] = {0, 0, 0};

    int reduce_send[5];
    int reduce_recv[5] = {0, 0, 0, 0, 0};

    int gather_send[2];
    int gather_recv[8] = {0};

    int scatter_send[28] = {0};
    int scatter_recv[7] = {0};

    int allgather_send[1];
    int allgather_recv[4] = {0};

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    require_size(size, 4, rank);

    /* Bcast: root = 2, count = 3 */
    if (rank == 2) {
        bcast_buf[0] = 11;
        bcast_buf[1] = 22;
        bcast_buf[2] = 33;
    }
    MPI_Bcast(bcast_buf, 3, MPI_INT, 2, MPI_COMM_WORLD);
    if (bcast_buf[0] != 11 || bcast_buf[1] != 22 || bcast_buf[2] != 33) {
        fail(rank, "Bcast data mismatch");
    }

    /* Reduce: root = 1, count = 5 */
    for (i = 0; i < 5; i++) {
        reduce_send[i] = rank + i;
    }
    MPI_Reduce(reduce_send, reduce_recv, 5, MPI_INT, MPI_SUM, 1, MPI_COMM_WORLD);
    if (rank == 1) {
        for (i = 0; i < 5; i++) {
            int expected = 6 + 4 * i; /* sum(rank+i) over ranks 0..3 */
            if (reduce_recv[i] != expected) {
                fail(rank, "Reduce result mismatch");
            }
        }
    }

    /* Gather: root = 3, sendcount = 2, recvcount = 2 */
    gather_send[0] = rank;
    gather_send[1] = rank + 10;
    MPI_Gather(gather_send, 2, MPI_INT,
               gather_recv, 2, MPI_INT,
               3, MPI_COMM_WORLD);

    if (rank == 3) {
        int expected[8] = {0, 10, 1, 11, 2, 12, 3, 13};
        for (i = 0; i < 8; i++) {
            if (gather_recv[i] != expected[i]) {
                fail(rank, "Gather result mismatch");
            }
        }
    }

    /* Scatter: root = 0, sendcount = 7, recvcount = 7 */
    if (rank == 0) {
        for (i = 0; i < 28; i++) {
            scatter_send[i] = 100 + i;
        }
    }
    MPI_Scatter(scatter_send, 7, MPI_INT,
                scatter_recv, 7, MPI_INT,
                0, MPI_COMM_WORLD);

    for (i = 0; i < 7; i++) {
        int expected = 100 + rank * 7 + i;
        if (scatter_recv[i] != expected) {
            fail(rank, "Scatter result mismatch");
        }
    }

    /* Allgather: sendcount = 1, recvcount = 1 */
    allgather_send[0] = rank;
    MPI_Allgather(allgather_send, 1, MPI_INT,
                  allgather_recv, 1, MPI_INT,
                  MPI_COMM_WORLD);

    for (i = 0; i < 4; i++) {
        if (allgather_recv[i] != i) {
            fail(rank, "Allgather result mismatch");
        }
    }

    MPI_Finalize();
    return 0;
}

