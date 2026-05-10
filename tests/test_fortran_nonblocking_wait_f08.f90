program test_fortran_nonblocking_wait_f08
  use mpi_f08
  implicit none

  integer :: ierr, rank, size
  integer, asynchronous :: sendv, recvv
  type(MPI_Request) :: req
  type(MPI_Status)  :: status

  call MPI_Init(ierr)
  call MPI_Comm_rank(MPI_COMM_WORLD, rank, ierr)
  call MPI_Comm_size(MPI_COMM_WORLD, size, ierr)

  if (size /= 2) then
     if (rank == 0) then
        write(*,*) 'test_fortran_nonblocking_wait requires 2 ranks, got ', size
     end if
     call MPI_Abort(MPI_COMM_WORLD, 1, ierr)
  end if

  if (rank == 0) then
     sendv = 42
     call MPI_Isend(sendv, 1, MPI_INTEGER, 1, 200, MPI_COMM_WORLD, req, ierr)
     call MPI_Wait(req, status, ierr)
  else
     recvv = -1
     call MPI_Irecv(recvv, 1, MPI_INTEGER, 0, 200, MPI_COMM_WORLD, req, ierr)
     call MPI_Wait(req, status, ierr)

     if (recvv /= 42) then
        write(*,*) 'rank 1 expected 42, got ', recvv
        call MPI_Abort(MPI_COMM_WORLD, 2, ierr)
     end if
  end if

  call MPI_Finalize(ierr)
end program test_fortran_nonblocking_wait_f08

