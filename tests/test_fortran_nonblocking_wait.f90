program test_fortran_nonblocking_wait
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size
  integer :: sendv, recvv
  integer :: req
  integer :: status(MPI_STATUS_SIZE)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 2) then
     if (rank .eq. 0) then
        write(*,*) 'test_fortran_nonblocking_wait requires 2 ranks, got ', size
     end if
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  if (rank .eq. 0) then
     sendv = 42
     call MPI_ISEND(sendv, 1, MPI_INTEGER, 1, 200, MPI_COMM_WORLD, req, ierr)
     call MPI_WAIT(req, status, ierr)
  else
     recvv = -1
     call MPI_IRECV(recvv, 1, MPI_INTEGER, 0, 200, MPI_COMM_WORLD, req, ierr)
     call MPI_WAIT(req, status, ierr)

     if (recvv .ne. 42) then
        write(*,*) 'rank 1 expected 42, got ', recvv
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_nonblocking_wait

