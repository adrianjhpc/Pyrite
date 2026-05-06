program test_fortran_allgather
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size, i
  integer :: sendbuf(1), recvbuf(4)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 4) then
     if (rank .eq. 0) write(*,*) 'test_fortran_allgather requires 4 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  sendbuf(1) = rank
  recvbuf = -1

  call MPI_ALLGATHER(sendbuf, 1, MPI_INTEGER, recvbuf, 1, MPI_INTEGER, MPI_COMM_WORLD, ierr)

  do i = 1, 4
     if (recvbuf(i) .ne. (i - 1)) then
        write(*,*) 'rank ', rank, ' allgather mismatch at index ', i, ' value ', recvbuf(i)
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  end do

  call MPI_FINALIZE(ierr)
end program test_fortran_allgather

