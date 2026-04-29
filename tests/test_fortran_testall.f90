program test_fortran_testall
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size
  integer :: ready
  integer :: send0, send1
  integer :: recv0(4), recv1(4)
  integer :: reqs(2)
  integer :: statuses(MPI_STATUS_SIZE, 2)
  logical :: flag

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 2) then
     if (rank .eq. 0) then
        write(*,*) 'test_fortran_testall requires 2 ranks, got ', size
     end if
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  ready = 1
  send0 = 800
  send1 = 801
  recv0 = -1
  recv1 = -1
  statuses = 0
  flag = .false.

  if (rank .eq. 0) then
     call MPI_RECV(ready, 1, MPI_INTEGER, 1, 242, MPI_COMM_WORLD, statuses(:,1), ierr)
     call MPI_SEND(send0, 1, MPI_INTEGER, 1, 240, MPI_COMM_WORLD, ierr)
     call MPI_SEND(send1, 1, MPI_INTEGER, 1, 241, MPI_COMM_WORLD, ierr)

  else
     call MPI_IRECV(recv0, 4, MPI_INTEGER, 0, 240, MPI_COMM_WORLD, reqs(1), ierr)
     call MPI_IRECV(recv1, 4, MPI_INTEGER, 0, 241, MPI_COMM_WORLD, reqs(2), ierr)

     call MPI_SEND(ready, 1, MPI_INTEGER, 0, 242, MPI_COMM_WORLD, ierr)

     do while (.not. flag)
        call MPI_TESTALL(2, reqs, flag, statuses, ierr)
     end do

     if (recv0(1) .ne. 800 .or. recv1(1) .ne. 801) then
        write(*,*) 'rank 1 expected 800 and 801, got ', recv0(1), recv1(1)
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_testall

