program test_fortran_cancel
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size
  integer :: recvbuf
  integer :: req
  integer :: status(MPI_STATUS_SIZE)
  logical :: cancelled

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 2) then
     if (rank .eq. 0) write(*,*) 'test_fortran_cancel requires 2 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  recvbuf = -1
  cancelled = .false.

  if (rank .eq. 1) then
     call MPI_IRECV(recvbuf, 1, MPI_INTEGER, 0, 390, MPI_COMM_WORLD, req, ierr)
     call MPI_CANCEL(req, ierr)
     call MPI_WAIT(req, status, ierr)
     call MPI_TEST_CANCELLED(status, cancelled, ierr)

     if (.not. cancelled) then
        write(*,*) 'rank 1 expected cancelled receive'
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  end if

  call MPI_BARRIER(MPI_COMM_WORLD, ierr)
  call MPI_FINALIZE(ierr)
end program test_fortran_cancel

