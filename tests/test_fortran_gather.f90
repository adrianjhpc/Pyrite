program test_fortran_gather
  implicit none
  include 'mpif.h'

  integer :: ierr, rank, size, i
  integer :: sendbuf(2), recvbuf(8)
  integer :: expected(8)

  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD, rank, ierr)
  call MPI_COMM_SIZE(MPI_COMM_WORLD, size, ierr)

  if (size .ne. 4) then
     if (rank .eq. 0) write(*,*) 'test_fortran_gather requires 4 ranks, got ', size
     call MPI_ABORT(MPI_COMM_WORLD, 1, ierr)
  end if

  sendbuf(1) = rank
  sendbuf(2) = rank + 10
  recvbuf = -1

  call MPI_GATHER(sendbuf, 2, MPI_INTEGER, recvbuf, 2, MPI_INTEGER, 3, MPI_COMM_WORLD, ierr)

  if (rank .eq. 3) then
     expected = (/ 0, 10, 1, 11, 2, 12, 3, 13 /)
     do i = 1, 8
        if (recvbuf(i) .ne. expected(i)) then
           write(*,*) 'gather mismatch at index ', i, ' value ', recvbuf(i)
           call MPI_ABORT(MPI_COMM_WORLD, 2, ierr)
        end if
     end do
  end if

  call MPI_FINALIZE(ierr)
end program test_fortran_gather

