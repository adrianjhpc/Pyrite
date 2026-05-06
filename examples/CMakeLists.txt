# ==========================================
# Target 1: C Test Application (Required)
# ==========================================
# We know C works, so we require the C components
find_package(MPI REQUIRED COMPONENTS C)

add_executable(mpi_test_messages mpi_test_messages.c)
target_link_libraries(mpi_test_messages PRIVATE MPI::MPI_C mpitrace_file)


# ==========================================
# Target 2: Fortran Test App (Optional)
# ==========================================
# Safely check if a Fortran compiler exists before enabling it
include(CheckLanguage)
check_language(Fortran)

if(CMAKE_Fortran_COMPILER)
    # Compiler exists, so it is safe to enable the language
    enable_language(Fortran)
    
    # Now check if the MPI Fortran libraries exist
    find_package(MPI COMPONENTS Fortran)
    
    if(MPI_Fortran_FOUND)
        add_executable(fortran_mpi_pingpong fortran_mpi_pingpong.f90)
        target_link_libraries(fortran_mpi_pingpong PRIVATE MPI::MPI_Fortran mpitrace_file)
        message(STATUS "Fortran and MPI_Fortran found: Building fortran_mpi_pingpong")
    else()
        message(WARNING "Fortran compiler found, but MPI Fortran libraries are missing. Skipping Fortran example.")
    endif()

else()
    # No compiler found, fail gracefully without crashing CMake
    message(WARNING "No Fortran compiler found on this system. Skipping fortran_mpi_pingpong.")
endif()
