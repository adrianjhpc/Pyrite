program test_fortran_reduce
  use mpi_f08
  implicit none

  integer :: ierr, rank, size, i
  integer :: sendbuf(5), recvbuf(5)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 4) then
     if (rank .eq. 0) write(*,*) 'test_fortran_reduce requires 4 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  do i = 1, 5
     sendbuf(i) = rank + (i - 1)
     recvbuf(i) = 0
  end do

  call MPI_REDUCE(sendbuf, recvbuf, 5, MPI_INTEGER, MPI_SUM, 1, MPI_COMM_WORLD, ierr)

  if (rank .eq. 1) then
     do i = 1, 5
        if (recvbuf(i) .ne. (6 + 4 * (i - 1))) then
           write(*,*) 'reduce mismatch at index ', i, ' value ', recvbuf(i)
           call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
        end if
     end do
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_reduce

