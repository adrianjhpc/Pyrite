program test_fortran_ssend
  use mpi_f08
  implicit none

  integer :: ierr, rank, size
  integer :: sendv, recvv
  type(MPI_Status) :: status

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 2) then
     if (rank .eq. 0) write(*,*) 'test_fortran_ssend requires 2 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  if (rank .eq. 0) then
     sendv = 234
     call MPI_SSEND(sendv, 1, MPI_INTEGER, 1, 310, MPI_COMM_WORLD, ierr)
  else
     recvv = -1
     call MPI_RECV(recvv, 1, MPI_INTEGER, 0, 310, MPI_COMM_WORLD, status, ierr)
     if (recvv .ne. 234) then
        write(*,*) 'rank 1 expected 234, got ', recvv
        call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
     end if
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_ssend

