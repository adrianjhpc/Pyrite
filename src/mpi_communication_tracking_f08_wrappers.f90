subroutine mpi_send_f08ts(buf, count, datatype, dest, tag, comm, ierror)
  use, intrinsic :: iso_c_binding, only : c_int
  use mpi_f08, only : MPI_Datatype, MPI_Comm
  implicit none

  type(*), dimension(..), intent(in) :: buf
  integer, intent(in) :: count, dest, tag
  type(MPI_Datatype), intent(in) :: datatype
  type(MPI_Comm), intent(in) :: comm
  integer, optional, intent(out) :: ierror

  integer(c_int) :: ierr_c

  interface
    subroutine mpi_trace_f08_send_helper(buf, count, datatype_f, dest, tag, comm_f, ierr) bind(C)
      use, intrinsic :: iso_c_binding, only : c_int
      type(*), dimension(..), intent(in) :: buf
      integer(c_int), value :: count, datatype_f, dest, tag, comm_f
      integer(c_int) :: ierr
    end subroutine
  end interface

  call mpi_trace_f08_send_helper(buf, int(count, c_int), int(datatype%MPI_VAL, c_int), int(dest, c_int), int(tag, c_int), int(comm%MPI_VAL, c_int), ierr_c)

  if (present(ierror)) ierror = int(ierr_c)
end subroutine mpi_send_f08ts


subroutine mpi_recv_f08ts(buf, count, datatype, source, tag, comm, status, ierror)
  use, intrinsic :: iso_c_binding, only : c_int
  use mpi_f08, only : MPI_Datatype, MPI_Comm, MPI_Status
  implicit none

  type(*), dimension(..) :: buf
  integer, intent(in) :: count, source, tag
  type(MPI_Datatype), intent(in) :: datatype
  type(MPI_Comm), intent(in) :: comm
  type(MPI_Status), intent(out) :: status
  integer, optional, intent(out) :: ierror

  integer(c_int) :: ierr_c

  interface
    subroutine mpi_trace_f08_recv_helper(buf, count, datatype_f, source, tag, comm_f, status, ierr) bind(C)
      use, intrinsic :: iso_c_binding, only : c_int
      use mpi_f08, only : MPI_Status
      type(*), dimension(..) :: buf
      integer(c_int), value :: count, datatype_f, source, tag, comm_f
      type(MPI_Status) :: status
      integer(c_int) :: ierr
    end subroutine
  end interface

  call mpi_trace_f08_recv_helper(buf, int(count, c_int), int(datatype%MPI_VAL, c_int), int(source, c_int), int(tag, c_int), int(comm%MPI_VAL, c_int), status, ierr_c)

  if (present(ierror)) ierror = int(ierr_c)
end subroutine mpi_recv_f08ts


subroutine mpi_irecv_f08ts(buf, count, datatype, source, tag, comm, request, ierror)
  use, intrinsic :: iso_c_binding, only : c_int
  use mpi_f08, only : MPI_Datatype, MPI_Comm, MPI_Request
  implicit none

  type(*), dimension(..), asynchronous :: buf
  integer, intent(in) :: count, source, tag
  type(MPI_Datatype), intent(in) :: datatype
  type(MPI_Comm), intent(in) :: comm
  type(MPI_Request), intent(out) :: request
  integer, optional, intent(out) :: ierror

  integer(c_int) :: ierr_c
  integer(c_int) :: request_f_c

  interface
    subroutine mpi_trace_f08_irecv_helper(buf, count, datatype_f, source, tag, comm_f, request_f, ierr) bind(C)
      use, intrinsic :: iso_c_binding, only : c_int
      type(*), dimension(..), asynchronous :: buf
      integer(c_int), value :: count, datatype_f, source, tag, comm_f
      integer(c_int) :: request_f
      integer(c_int) :: ierr
    end subroutine
  end interface

  call mpi_trace_f08_irecv_helper(buf, int(count, c_int), int(datatype%MPI_VAL, c_int), int(source, c_int), int(tag, c_int), int(comm%MPI_VAL, c_int), request_f_c, ierr_c)

  request%MPI_VAL = request_f_c

  if (present(ierror)) ierror = int(ierr_c)
end subroutine mpi_irecv_f08ts


subroutine mpi_wait_f08(request, status, ierror)
  use, intrinsic :: iso_c_binding, only : c_int
  use mpi_f08, only : MPI_Request, MPI_Status
  implicit none

  type(MPI_Request), intent(inout) :: request
  type(MPI_Status), intent(out) :: status
  integer, optional, intent(out) :: ierror

  integer(c_int) :: request_f_c
  integer(c_int) :: ierr_c

  interface
    subroutine mpi_trace_f08_wait_helper(request_f, status, ierr) bind(C)
      use, intrinsic :: iso_c_binding, only : c_int
      use mpi_f08, only : MPI_Status
      integer(c_int) :: request_f
      type(MPI_Status) :: status
      integer(c_int) :: ierr
    end subroutine
  end interface

  request_f_c = request%MPI_VAL

  call mpi_trace_f08_wait_helper(request_f_c, status, ierr_c)

  request%MPI_VAL = request_f_c

  if (present(ierror)) ierror = int(ierr_c)
end subroutine mpi_wait_f08

subroutine mpi_isend_f08ts(buf, count, datatype, dest, tag, comm, request, ierror)
  use, intrinsic :: iso_c_binding, only : c_int
  use mpi_f08, only : MPI_Datatype, MPI_Comm, MPI_Request
  implicit none

  type(*), dimension(..), asynchronous :: buf
  integer, intent(in) :: count, dest, tag
  type(MPI_Datatype), intent(in) :: datatype
  type(MPI_Comm), intent(in) :: comm
  type(MPI_Request), intent(out) :: request
  integer, optional, intent(out) :: ierror

  integer(c_int) :: ierr_c
  integer(c_int) :: request_f_c

  interface
    subroutine mpi_trace_f08_isend_helper(buf, count, datatype_f, dest, tag, comm_f, request_f, ierr) bind(C)
      use, intrinsic :: iso_c_binding, only : c_int
      type(*), dimension(..), asynchronous :: buf
      integer(c_int), value :: count, datatype_f, dest, tag, comm_f
      integer(c_int) :: request_f
      integer(c_int) :: ierr
    end subroutine
  end interface

  call mpi_trace_f08_isend_helper(buf, int(count, c_int), int(datatype%MPI_VAL, c_int), int(dest, c_int), int(tag, c_int), int(comm%MPI_VAL, c_int), request_f_c, ierr_c)

  request%MPI_VAL = request_f_c

  if (present(ierror)) ierror = int(ierr_c)
end subroutine mpi_isend_f08ts

subroutine mpi_waitall_f08(count, array_of_requests, array_of_statuses, ierror)
  use, intrinsic :: iso_c_binding, only : c_int
  use mpi_f08, only : MPI_Request, MPI_Status
  implicit none

  integer, intent(in) :: count
  type(MPI_Request), intent(inout) :: array_of_requests(count)
  type(MPI_Status), intent(out) :: array_of_statuses(count)
  integer, optional, intent(out) :: ierror

  integer(c_int) :: ierr_c
  integer(c_int) :: request_f_c(count)
  integer :: i

  interface
    subroutine mpi_trace_f08_waitall_helper(count, request_f, statuses, ierr) bind(C)
      use, intrinsic :: iso_c_binding, only : c_int
      use mpi_f08, only : MPI_Status
      integer(c_int), value :: count
      integer(c_int) :: request_f(*)
      type(MPI_Status) :: statuses(*)
      integer(c_int) :: ierr
    end subroutine
  end interface

  do i = 1, count
     request_f_c(i) = array_of_requests(i)%MPI_VAL
  end do

  call mpi_trace_f08_waitall_helper(int(count, c_int), request_f_c, array_of_statuses, ierr_c)

  do i = 1, count
     array_of_requests(i)%MPI_VAL = request_f_c(i)
  end do

  if (present(ierror)) ierror = int(ierr_c)
end subroutine mpi_waitall_f08

