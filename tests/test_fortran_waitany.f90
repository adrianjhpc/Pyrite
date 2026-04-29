program test_fortran_waitany
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size
  integer :: ready, ack
  integer :: send0, send1
  integer :: recv0(4), recv1(4)
  integer :: reqs(2)
  integer :: status(MPI_STATUS_SIZE)
  integer :: index, count

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 2) then
     if (rank .eq. 0) then
        write(*,*) 'test_fortran_waitany requires 2 ranks, got ', size
     end if
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  ready = 1
  ack = 1
  send0 = 100
  send1 = 101
  recv0 = -1
  recv1 = -1
  index = -1
  count = -1

  if (rank .eq. 0) then
     call MPI_RECV(ready, 1, MPI_INTEGER, 1, 232, MPI_COMM_WORLD, status, ierr)
     call MPI_SEND(send0, 1, MPI_INTEGER, 1, 230, MPI_COMM_WORLD, ierr)
     call MPI_RECV(ack, 1, MPI_INTEGER, 1, 233, MPI_COMM_WORLD, status, ierr)
     call MPI_SEND(send1, 1, MPI_INTEGER, 1, 231, MPI_COMM_WORLD, ierr)

  else
     call MPI_IRECV(recv0, 4, MPI_INTEGER, 0, 230, MPI_COMM_WORLD, reqs(1), ierr)
     call MPI_IRECV(recv1, 4, MPI_INTEGER, 0, 231, MPI_COMM_WORLD, reqs(2), ierr)

     call MPI_SEND(ready, 1, MPI_INTEGER, 0, 232, MPI_COMM_WORLD, ierr)

     call MPI_WAITANY(2, reqs, index, status, ierr)
     call MPI_GET_COUNT(status, MPI_INTEGER, count, ierr)

     if (index .ne. 1 .or. status(MPI_SOURCE) .ne. 0 .or. count .ne. 1 .or. recv0(1) .ne. 100) then
        write(*,*) 'rank 1 expected index=1 source=0 count=1 value=100, got index=', index, &
                   ' source=', status(MPI_SOURCE), ' count=', count, 'value=', recv0(1)
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if

     call MPI_SEND(ack, 1, MPI_INTEGER, 0, 233, MPI_COMM_WORLD, ierr)
     call MPI_WAIT(reqs(2), status, ierr)

     if (recv1(1) .ne. 101) then
        write(*,*) 'rank 1 expected second value 101, got ', recv1(1)
        call MPI_ABORT(MPI_COMM_WORLD, 3, ierr)
     end if
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_waitany

