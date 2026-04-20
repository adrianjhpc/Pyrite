        program hello
                use mpi
                implicit none
                integer :: ierr
                integer :: rank, length
                character(MPI_MAX_PROCESSOR_NAME) :: hostname

                call mpi_init(ierr)

                call mpi_comm_rank(MPI_COMM_WORLD, rank, ierr)
                call mpi_get_processor_name(hostname, length, ierr)
                write(*,*) "Hi, I'm ",rank," on ",hostname
                
                call mpi_finalize(ierr)

        end program hello

