program test_fortran_rsend
  use mpi_f08
  implicit none

  integer :: ierr, rank, size
  integer :: msg, ready, recvbuf
  type(MPI_Request) :: req
  type(MPI_Status) :: status

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 2) then
     if (rank .eq. 0) write(*,*) 'test_fortran_rsend requires 2 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  ready = 1
  msg = 345
  recvbuf = -1

  if (rank .eq. 1) then
     call MPI_IRECV(recvbuf, 1, MPI_INTEGER, 0, 320, MPI_COMM_WORLD, req, ierr)
     call MPI_SEND(ready, 1, MPI_INTEGER, 0, 321, MPI_COMM_WORLD, ierr)
     call MPI_WAIT(req, status, ierr)

     if (recvbuf .ne. 345) then
        write(*,*) 'rank 1 expected 345, got ', recvbuf
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  else
     call MPI_RECV(ready, 1, MPI_INTEGER, 1, 321, MPI_COMM_WORLD, status, ierr)
     call MPI_RSEND(msg, 1, MPI_INTEGER, 1, 320, MPI_COMM_WORLD, ierr)
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_rsend

