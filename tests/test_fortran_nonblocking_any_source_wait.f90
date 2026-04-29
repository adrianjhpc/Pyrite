program test_fortran_nonblocking_any_source_wait
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size
  integer :: msg1, msg2, go, tail, count
  integer :: recvbuf(4)
  integer :: req
  integer :: status(MPI_STATUS_SIZE)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 3) then
     if (rank .eq. 0) then
        write(*,*) 'test_fortran_nonblocking_any_source_wait requires 3 ranks, got ', size
     end if
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  msg1 = 111
  msg2 = 222
  go = 1
  tail = -1
  recvbuf = -1

  if (rank .eq. 0) then
     call MPI_IRECV(recvbuf, 4, MPI_INTEGER, MPI_ANY_SOURCE, 210, MPI_COMM_WORLD, req, ierr)
     call MPI_WAIT(req, status, ierr)
     call MPI_GET_COUNT(status, MPI_INTEGER, count, ierr)

     if (status(MPI_SOURCE) .ne. 1 .or. count .ne. 1 .or. recvbuf(1) .ne. 111) then
        write(*,*) 'rank 0 expected source=1 count=1 value=111, got source=', &
                   status(MPI_SOURCE), ' count=', count, ' value=', recvbuf(1)
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if

     call MPI_SEND(go, 1, MPI_INTEGER, 2, 211, MPI_COMM_WORLD, ierr)
     call MPI_RECV(tail, 1, MPI_INTEGER, 2, 210, MPI_COMM_WORLD, status, ierr)

     if (tail .ne. 222) then
        write(*,*) 'rank 0 expected second value 222, got ', tail
        call MPI_ABORT(MPI_COMM_WORLD, 3, ierr)
     end if

  else if (rank .eq. 1) then
     call MPI_SEND(msg1, 1, MPI_INTEGER, 0, 210, MPI_COMM_WORLD, ierr)

  else if (rank .eq. 2) then
     call MPI_RECV(go, 1, MPI_INTEGER, 0, 211, MPI_COMM_WORLD, status, ierr)
     call MPI_SEND(msg2, 1, MPI_INTEGER, 0, 210, MPI_COMM_WORLD, ierr)
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_nonblocking_any_source_wait

