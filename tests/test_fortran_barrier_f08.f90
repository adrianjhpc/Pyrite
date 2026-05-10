program test_fortran_barrier
  use mpi_f08
  implicit none

  integer :: ierr, rank, size

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 3) then
     if (rank .eq. 0) write(*,*) 'test_fortran_barrier requires 3 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  call MPI_BARRIER(MPI_COMM_WORLD, ierr)

  call MPI_FINALIZE(ierr)
end program test_fortran_barrier

