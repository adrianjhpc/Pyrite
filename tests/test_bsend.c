#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    int rank, size;
    int sendv, recvv;
    int pack_size, bsize;
    void *bbuf = NULL;
    void *detached_buf = NULL;
    int detached_size = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 2) {
        if (rank == 0) {
            fprintf(stderr, "test_c_bsend requires 2 ranks, got %d\n", size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (rank == 0) {
        MPI_Pack_size(1, MPI_INT, MPI_COMM_WORLD, &pack_size);
        bsize = pack_size + MPI_BSEND_OVERHEAD;
        bbuf = malloc((size_t)bsize);
        if (bbuf == NULL) {
            fprintf(stderr, "rank 0 failed to allocate bsend buffer\n");
            MPI_Abort(MPI_COMM_WORLD, 2);
        }

        MPI_Buffer_attach(bbuf, bsize);

        sendv = 123;
        MPI_Bsend(&sendv, 1, MPI_INT, 1, 300, MPI_COMM_WORLD);

        MPI_Buffer_detach(&detached_buf, &detached_size);
        free(detached_buf);
    } else {
        recvv = -1;
        MPI_Recv(&recvv, 1, MPI_INT, 0, 300, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (recvv != 123) {
            fprintf(stderr, "rank 1 expected 123, got %d\n", recvv);
            MPI_Abort(MPI_COMM_WORLD, 3);
        }
    }

    MPI_Finalize();
    return 0;
}

