program test_fortran_scatter
  use mpi_f08
  implicit none

  integer :: ierr, rank, size, i
  integer :: sendbuf(28), recvbuf(7)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 4) then
     if (rank .eq. 0) write(*,*) 'test_fortran_scatter requires 4 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  if (rank .eq. 0) then
     do i = 1, 28
        sendbuf(i) = 100 + (i - 1)
     end do
  end if

  recvbuf = -1

  call MPI_SCATTER(sendbuf, 7, MPI_INTEGER, recvbuf, 7, MPI_INTEGER, 0, MPI_COMM_WORLD, ierr)

  do i = 1, 7
     if (recvbuf(i) .ne. (100 + rank * 7 + (i - 1))) then
        write(*,*) 'rank ', rank, ' scatter mismatch at index ', i, ' value ', recvbuf(i)
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  end do

  call MPI_FINALIZE(ierr)
end program test_fortran_scatter

