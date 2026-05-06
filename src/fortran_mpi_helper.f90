module mpi_tracking_fortran_helpers
  use, intrinsic :: iso_c_binding, only : c_int
  use mpi
  implicit none
contains

  function mpi_tracking_get_status_size() bind(C, name="mpi_tracking_get_status_size") result(n)
    integer(c_int) :: n
    n = MPI_STATUS_SIZE
  end function mpi_tracking_get_status_size

end module mpi_tracking_fortran_helpers

