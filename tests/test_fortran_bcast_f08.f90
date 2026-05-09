program test_fortran_bcast
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size
  integer :: buf(3)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 4) then
     if (rank .eq. 0) write(*,*) 'test_fortran_bcast requires 4 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  buf = 0
  if (rank .eq. 2) then
     buf(1) = 11
     buf(2) = 22
     buf(3) = 33
  end if

  call MPI_BCAST(buf, 3, MPI_INTEGER, 2, MPI_COMM_WORLD, ierr)

  if (buf(1) .ne. 11 .or. buf(2) .ne. 22 .or. buf(3) .ne. 33) then
     write(*,*) 'rank ', rank, ' bcast data mismatch'
     call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_bcast

