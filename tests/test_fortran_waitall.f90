program test_fortran_waitall
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size
  integer :: send0, send1
  integer :: recv0, recv1
  integer :: reqs(2)
  integer :: statuses(MPI_STATUS_SIZE, 2)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 2) then
     if (rank .eq. 0) then
        write(*,*) 'test_fortran_waitall requires 2 ranks, got ', size
     end if
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  send0 = 11
  send1 = 22
  recv0 = -1
  recv1 = -1
  statuses = 0

  if (rank .eq. 0) then
     call MPI_ISEND(send0, 1, MPI_INTEGER, 1, 220, MPI_COMM_WORLD, reqs(1), ierr)
     call MPI_ISEND(send1, 1, MPI_INTEGER, 1, 221, MPI_COMM_WORLD, reqs(2), ierr)
     call MPI_WAITALL(2, reqs, statuses, ierr)
  else
     call MPI_IRECV(recv0, 1, MPI_INTEGER, 0, 220, MPI_COMM_WORLD, reqs(1), ierr)
     call MPI_IRECV(recv1, 1, MPI_INTEGER, 0, 221, MPI_COMM_WORLD, reqs(2), ierr)
     call MPI_WAITALL(2, reqs, statuses, ierr)

     if (recv0 .ne. 11 .or. recv1 .ne. 22) then
        write(*,*) 'rank 1 expected 11 and 22, got ', recv0, recv1
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_waitall

