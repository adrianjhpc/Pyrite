program test_fortran_sendrecv
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size
  integer :: sendbuf, recvbuf, peer
  integer :: status(MPI_STATUS_SIZE)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 2) then
     if (rank .eq. 0) write(*,*) 'test_fortran_sendrecv requires 2 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  peer = 1 - rank
  sendbuf = 500 + rank
  recvbuf = -1

  call MPI_SENDRECV(sendbuf, 1, MPI_INTEGER, peer, 330, recvbuf, 1, MPI_INTEGER, peer, 330, MPI_COMM_WORLD, status, ierr)

  if (recvbuf .ne. 500 + peer) then
     write(*,*) 'rank ', rank, ' expected ', 500 + peer, ' got ', recvbuf
     call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_sendrecv

