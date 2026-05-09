#include <stdlib.h>
#include <mpi.h>
#include <ISO_Fortran_binding.h>

void mpi_trace_f08_send_helper(CFI_cdesc_t *buf, int count, int datatype_f, int dest, int tag, int comm_f, int *ierr)
{
    const void *base = (buf != NULL) ? buf->base_addr : NULL;

    if (ierr == NULL) return;

    *ierr = (int)MPI_Send(base, count, PMPI_Type_f2c((MPI_Fint)datatype_f), dest, tag, MPI_Comm_f2c((MPI_Fint)comm_f));
}

void mpi_trace_f08_recv_helper(CFI_cdesc_t *buf, int count, int datatype_f, int source, int tag, int comm_f, MPI_Status *status, int *ierr)
{
    void *base = (buf != NULL) ? buf->base_addr : NULL;

    if (ierr == NULL) return;

    *ierr = (int)MPI_Recv(base, count, PMPI_Type_f2c((MPI_Fint)datatype_f), source, tag, MPI_Comm_f2c((MPI_Fint)comm_f), status);
}

void mpi_trace_f08_irecv_helper(CFI_cdesc_t *buf, int count, int datatype_f, int source, int tag, int comm_f, int *request_f, int *ierr)
{
    void *base = (buf != NULL) ? buf->base_addr : NULL;
    MPI_Request c_req = MPI_REQUEST_NULL;
    int rc;

    if (ierr == NULL) return;

    rc = (int)MPI_Irecv(base, count, PMPI_Type_f2c((MPI_Fint)datatype_f), source, tag, MPI_Comm_f2c((MPI_Fint)comm_f), &c_req);

    if (request_f != NULL) {
        *request_f = (int)PMPI_Request_c2f(c_req);
    }

    *ierr = rc;
}

void mpi_trace_f08_wait_helper(int *request_f, MPI_Status *status, int *ierr)
{
    MPI_Request c_req = MPI_REQUEST_NULL;
    int rc;

    if (ierr == NULL) return;

    if (request_f != NULL) {
        c_req = PMPI_Request_f2c((MPI_Fint)*request_f);
    }

    rc = (int)MPI_Wait(&c_req, status);

    if (request_f != NULL) {
        *request_f = (int)PMPI_Request_c2f(c_req);
    }

    *ierr = rc;
}

void mpi_trace_f08_isend_helper(CFI_cdesc_t *buf, int count, int datatype_f, int dest, int tag, int comm_f, int *request_f, int *ierr)
{
    const void *base = (buf != NULL) ? buf->base_addr : NULL;
    MPI_Request c_req = MPI_REQUEST_NULL;
    int rc;

    if (ierr == NULL) return;

    rc = (int)MPI_Isend(base, count, PMPI_Type_f2c((MPI_Fint)datatype_f), dest, tag, MPI_Comm_f2c((MPI_Fint)comm_f), &c_req);

    if (request_f != NULL) {
        *request_f = (int)PMPI_Request_c2f(c_req);
    }

    *ierr = rc;
}

void mpi_trace_f08_waitall_helper(int count, int request_f[], MPI_Status statuses[], int *ierr)
{
    int i;
    MPI_Request *c_requests = NULL;
    int rc;

    if (ierr == NULL) return;

    if (count < 0) {
        *ierr = MPI_ERR_ARG;
        return;
    }

    if (count == 0) {
        *ierr = (int)MPI_Waitall(0, NULL, statuses);
        return;
    }

    c_requests = (MPI_Request *)malloc((size_t)count * sizeof(MPI_Request));
    if (c_requests == NULL) {
        *ierr = MPI_ERR_NO_MEM;
        return;
    }

    for (i = 0; i < count; i++) {
        c_requests[i] = PMPI_Request_f2c((MPI_Fint)request_f[i]);
    }

    rc = (int)MPI_Waitall(count, c_requests, statuses);

    for (i = 0; i < count; i++) {
        request_f[i] = (int)PMPI_Request_c2f(c_requests[i]);
    }

    free(c_requests);
    *ierr = rc;
}

