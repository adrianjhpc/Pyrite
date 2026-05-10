program test_fortran_bsend_f08
  use mpi_f08
  use, intrinsic :: iso_c_binding, only : c_ptr
  implicit none

  integer :: ierr, rank, size
  integer :: sendv, recvv
  type(MPI_Status) :: status
  integer :: bsize, detached_size
  integer, asynchronous :: bbuf(512)
  type(c_ptr) :: detached_buf

  call MPI_Init(ierr)
  call MPI_Comm_rank(MPI_COMM_WORLD, rank, ierr)
  call MPI_Comm_size(MPI_COMM_WORLD, size, ierr)

  if (size /= 2) then
     if (rank == 0) write(*,*) 'test_fortran_bsend requires 2 ranks, got ', size
     call MPI_Abort(MPI_COMM_WORLD, 1, ierr)
  end if

  if (rank == 0) then
     bsize = 2048
     call MPI_Buffer_attach(bbuf, bsize, ierr)

     sendv = 123
     call MPI_Bsend(sendv, 1, MPI_INTEGER, 1, 300, MPI_COMM_WORLD, ierr)

     call MPI_Buffer_detach(detached_buf, detached_size, ierr)

  else
     recvv = -1
     call MPI_Recv(recvv, 1, MPI_INTEGER, 0, 300, MPI_COMM_WORLD, status, ierr)

     if (recvv /= 123) then
        write(*,*) 'rank 1 expected 123, got ', recvv
        call MPI_Abort(MPI_COMM_WORLD, 2, ierr)
     end if
  end if

  call MPI_Finalize(ierr)
end program test_fortran_bsend_f08

