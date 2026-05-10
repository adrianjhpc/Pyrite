program test_fortran_allreduce
  use mpi_f08
  implicit none

  integer :: ierr, rank, size, i
  integer :: sendbuf(4), recvbuf(4)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 4) then
     if (rank .eq. 0) write(*,*) 'test_fortran_allreduce requires 4 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  do i = 1, 4
     sendbuf(i) = rank + i
     recvbuf(i) = 0
  end do

  call MPI_ALLREDUCE(sendbuf, recvbuf, 4, MPI_INTEGER, MPI_SUM, MPI_COMM_WORLD, ierr)

  do i = 1, 4
     if (recvbuf(i) .ne. (6 + 4 * i)) then
        write(*,*) 'rank ', rank, ' allreduce mismatch at index ', i, ' value ', recvbuf(i)
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  end do

  call MPI_FINALIZE(ierr)
end program test_fortran_allreduce

