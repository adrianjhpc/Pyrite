program test_fortran_nonblocking_any_source_wait_f08
  use mpi_f08
  implicit none

  integer :: ierr, rank, size
  integer :: msg1, msg2, go, tail, count
  integer :: recvbuf(4)
  type(MPI_Request) :: req
  type(MPI_Status)  :: status

  call MPI_Init(ierr)
  call MPI_Comm_rank(MPI_COMM_WORLD, rank, ierr)
  call MPI_Comm_size(MPI_COMM_WORLD, size, ierr)

  if (size /= 3) then
     if (rank == 0) then
        write(*,*) 'test_fortran_nonblocking_any_source_wait requires 3 ranks, got ', size
     end if
     call MPI_Abort(MPI_COMM_WORLD, 1, ierr)
  end if

  msg1 = 111
  msg2 = 222
  go = 1
  tail = -1
  recvbuf = -1

  if (rank == 0) then
     call MPI_Irecv(recvbuf, 4, MPI_INTEGER, MPI_ANY_SOURCE, 210, MPI_COMM_WORLD, req, ierr)
     call MPI_Wait(req, status, ierr)
     call MPI_Get_count(status, MPI_INTEGER, count, ierr)

     if (status%MPI_SOURCE /= 1 .or. count /= 1 .or. recvbuf(1) /= 111) then
        write(*,*) 'rank 0 expected source=1 count=1 value=111, got source=', &
                   status%MPI_SOURCE, ' count=', count, ' value=', recvbuf(1)
        call MPI_Abort(MPI_COMM_WORLD, 2, ierr)
     end if

     call MPI_Send(go,   1, MPI_INTEGER, 2, 211, MPI_COMM_WORLD, ierr)
     call MPI_Recv(tail, 1, MPI_INTEGER, 2, 210, MPI_COMM_WORLD, status, ierr)

     if (tail /= 222) then
        write(*,*) 'rank 0 expected second value 222, got ', tail
        call MPI_Abort(MPI_COMM_WORLD, 3, ierr)
     end if

  else if (rank == 1) then
     call MPI_Send(msg1, 1, MPI_INTEGER, 0, 210, MPI_COMM_WORLD, ierr)

  else if (rank == 2) then
     call MPI_Recv(go, 1, MPI_INTEGER, 0, 211, MPI_COMM_WORLD, status, ierr)
     call MPI_Send(msg2, 1, MPI_INTEGER, 0, 210, MPI_COMM_WORLD, ierr)
  end if

  call MPI_Finalize(ierr)
end program test_fortran_nonblocking_any_source_wait_f08

