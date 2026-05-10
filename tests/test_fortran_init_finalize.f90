program test_init_finalize
    implicit none
    include 'mpif.h'

    integer :: rank, size, ierr

    ! The tracer should intercept this and start the backends
    call MPI_Init(ierr)
    
    call MPI_Comm_rank(MPI_COMM_WORLD, rank, ierr)
    call MPI_Comm_size(MPI_COMM_WORLD, size, ierr)

    if (rank == 0) then
        write(*,*) "Running bare-minimum Fortran Init/Finalize test on ", size, " ranks."
    end if

    ! Do absolutely zero communication. 
    
    ! The tracer should intercept this, flush the backends, and tear
    ! down gracefully
    call MPI_Finalize(ierr)

end program test_init_finalize
