cmake_minimum_required(VERSION 3.18)

include(CTest)

if(NOT BUILD_TESTING)
    return()
endif()

include(CheckLanguage)

find_package(Python3 REQUIRED COMPONENTS Interpreter)

# -----------------------------------------------------------------------------
# Optional Fortran test support
#
# Values:
#   AUTO : enable Fortran tests only if compiler + MPI Fortran are available
#   ON   : require Fortran tests
#   OFF  : disable Fortran tests
# -----------------------------------------------------------------------------

set(MPI_TRACE_FORTRAN_TESTS
    "AUTO"
    CACHE STRING
    "Enable Fortran MPI trace tests: AUTO, ON, or OFF"
)
set_property(CACHE MPI_TRACE_FORTRAN_TESTS PROPERTY STRINGS AUTO ON OFF)

set(MPI_TRACE_HAVE_FORTRAN_TESTS OFF)

if(MPI_TRACE_FORTRAN_TESTS STREQUAL "OFF")
    find_package(MPI REQUIRED COMPONENTS C)

elseif(MPI_TRACE_FORTRAN_TESTS STREQUAL "ON")
    check_language(Fortran)
    if(NOT CMAKE_Fortran_COMPILER)
        message(FATAL_ERROR
            "MPI_TRACE_FORTRAN_TESTS=ON but no Fortran compiler was found."
        )
    endif()

    enable_language(Fortran)
    find_package(MPI REQUIRED COMPONENTS C Fortran)
    set(MPI_TRACE_HAVE_FORTRAN_TESTS ON)

elseif(MPI_TRACE_FORTRAN_TESTS STREQUAL "AUTO")
    find_package(MPI REQUIRED COMPONENTS C)

    check_language(Fortran)
    if(CMAKE_Fortran_COMPILER)
        enable_language(Fortran)
        find_package(MPI QUIET COMPONENTS Fortran)
        if(MPI_Fortran_FOUND)
            set(MPI_TRACE_HAVE_FORTRAN_TESTS ON)
            message(STATUS "Fortran MPI trace tests enabled")
        else()
            message(STATUS "MPI Fortran not found; Fortran MPI trace tests disabled")
        endif()
    else()
        message(STATUS "No Fortran compiler found; Fortran MPI trace tests disabled")
    endif()

else()
    message(FATAL_ERROR
        "Invalid value for MPI_TRACE_FORTRAN_TESTS='${MPI_TRACE_FORTRAN_TESTS}'. "
        "Expected AUTO, ON, or OFF."
    )
endif()

# -----------------------------------------------------------------------------
# Locate the tracing library target built in src/
# -----------------------------------------------------------------------------

set(_default_trace_target "")
if(TARGET mpi_comm_tracker)
    set(_default_trace_target mpi_comm_tracker)
elseif(TARGET mpi_communication_tracking)
    set(_default_trace_target mpi_communication_tracking)
endif()

set(MPI_TRACE_LIBRARY_TARGET
    "${_default_trace_target}"
    CACHE STRING
    "CMake target name of the MPI tracing shared library built in src/"
)

if(MPI_TRACE_LIBRARY_TARGET STREQUAL "")
    message(FATAL_ERROR
        "Could not auto-detect the tracing library target.\n"
        "Please configure with:\n"
        "  -DMPI_TRACE_LIBRARY_TARGET=<target-built-in-src>"
    )
endif()

if(NOT TARGET ${MPI_TRACE_LIBRARY_TARGET})
    message(FATAL_ERROR
        "MPI_TRACE_LIBRARY_TARGET='${MPI_TRACE_LIBRARY_TARGET}' is not a known target.\n"
        "Make sure src/ is added before tests/ in the top-level CMakeLists.txt."
    )
endif()

# -----------------------------------------------------------------------------
# MPI launcher options
# -----------------------------------------------------------------------------

string(JOIN " " _mpiexec_preflags ${MPIEXEC_PREFLAGS})

set(MPI_TRACE_MPIEXEC_N_FLAG
    "${MPIEXEC_NUMPROC_FLAG}"
    CACHE STRING
    "Flag used by the MPI launcher to specify process count"
)

set(MPI_TRACE_MPIEXEC_EXTRA_ARGS
    "${_mpiexec_preflags}"
    CACHE STRING
    "Extra arguments passed to the MPI launcher before the process-count flag"
)

set(MPI_TRACE_SLURM_ARGS
    ""
    CACHE STRING
    "Extra arguments passed to srun when running MPI trace tests"
)

get_filename_component(_mpiexec_name "${MPIEXEC_EXECUTABLE}" NAME)

set(_combined_mpiexec_args "${MPI_TRACE_MPIEXEC_EXTRA_ARGS}")
if(_mpiexec_name STREQUAL "srun" AND NOT MPI_TRACE_SLURM_ARGS STREQUAL "")
    if(_combined_mpiexec_args STREQUAL "")
        set(_combined_mpiexec_args "${MPI_TRACE_SLURM_ARGS}")
    else()
        set(_combined_mpiexec_args "${_combined_mpiexec_args} ${MPI_TRACE_SLURM_ARGS}")
    endif()
endif()

# -----------------------------------------------------------------------------
# Helper script
# -----------------------------------------------------------------------------

set(MPI_TRACE_CTEST_DRIVER "${CMAKE_CURRENT_SOURCE_DIR}/ctest_driver.py")

if(NOT EXISTS "${MPI_TRACE_CTEST_DRIVER}")
    message(FATAL_ERROR
        "Expected helper script not found: ${MPI_TRACE_CTEST_DRIVER}"
    )
endif()

# -----------------------------------------------------------------------------
# Track built test targets for the convenience target
# -----------------------------------------------------------------------------

set(_mpi_trace_case_targets "")

# -----------------------------------------------------------------------------
# Build each C MPI test as its own executable
# -----------------------------------------------------------------------------

function(add_mpi_trace_case case_name source_file nprocs)
    set(target_name "mpi_trace_case_${case_name}")

    add_executable(${target_name} "${CMAKE_CURRENT_SOURCE_DIR}/${source_file}")
    target_link_libraries(${target_name} PRIVATE MPI::MPI_C)
    target_compile_features(${target_name} PRIVATE c_std_11)

    set(run_dir "${CMAKE_CURRENT_BINARY_DIR}/runs/${case_name}")

    add_test(
        NAME "mpi_trace_${case_name}"
        COMMAND
            ${CMAKE_COMMAND} -E env
                PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}
                ${Python3_EXECUTABLE}
                ${MPI_TRACE_CTEST_DRIVER}
                --case=${case_name}
                --launcher=${MPIEXEC_EXECUTABLE}
                --nflag=${MPI_TRACE_MPIEXEC_N_FLAG}
                --launcher-extra-args=${_combined_mpiexec_args}
                --nprocs=${nprocs}
                --exe=$<TARGET_FILE:${target_name}>
                --trace-lib=$<TARGET_FILE:${MPI_TRACE_LIBRARY_TARGET}>
                --work-dir=${run_dir}
    )

    set_tests_properties(
        "mpi_trace_${case_name}"
        PROPERTIES
            WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
            LABELS "mpi;trace;mpi-trace;${case_name}"
    )

    set(_mpi_trace_case_targets "${_mpi_trace_case_targets};${target_name}" PARENT_SCOPE)
endfunction()

# -----------------------------------------------------------------------------
# Build each Fortran MPI test as its own executable
# -----------------------------------------------------------------------------

function(add_mpi_trace_fortran_case case_name source_file nprocs)
    set(target_name "mpi_trace_case_${case_name}")

    add_executable(${target_name} "${CMAKE_CURRENT_SOURCE_DIR}/${source_file}")
    target_link_libraries(${target_name} PRIVATE MPI::MPI_Fortran)

    set(run_dir "${CMAKE_CURRENT_BINARY_DIR}/runs/${case_name}")

    add_test(
        NAME "mpi_trace_${case_name}"
        COMMAND
            ${CMAKE_COMMAND} -E env
                PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}
                ${Python3_EXECUTABLE}
                ${MPI_TRACE_CTEST_DRIVER}
                --case=${case_name}
                --launcher=${MPIEXEC_EXECUTABLE}
                --nflag=${MPI_TRACE_MPIEXEC_N_FLAG}
                --launcher-extra-args=${_combined_mpiexec_args}
                --nprocs=${nprocs}
                --exe=$<TARGET_FILE:${target_name}>
                --trace-lib=$<TARGET_FILE:${MPI_TRACE_LIBRARY_TARGET}>
                --work-dir=${run_dir}
    )

    set_tests_properties(
        "mpi_trace_${case_name}"
        PROPERTIES
            WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
            LABELS "mpi;trace;mpi-trace;fortran;${case_name}"
    )

    set(_mpi_trace_case_targets "${_mpi_trace_case_targets};${target_name}" PARENT_SCOPE)
endfunction()

# -----------------------------------------------------------------------------
# Register C tests
# -----------------------------------------------------------------------------

add_mpi_trace_case(send_recv                    test_send_recv.c                    2)
add_mpi_trace_case(any_source                   test_any_source.c                   3)
add_mpi_trace_case(sendrecv                     test_sendrecv.c                     2)
add_mpi_trace_case(subcomm_send                 test_subcomm_send.c                 4)
add_mpi_trace_case(collectives                  test_collectives.c                  4)
add_mpi_trace_case(nonblocking                  test_nonblocking.c                  2)
add_mpi_trace_case(nonblocking_any_source_wait  test_nonblocking_any_source_wait.c  3)
add_mpi_trace_case(waitany                      test_waitany.c                      2)
add_mpi_trace_case(waitsome                     test_waitsome.c                     2)
add_mpi_trace_case(test_single                  test_test.c                         2)
add_mpi_trace_case(testall                      test_testall.c                      2)
add_mpi_trace_case(testany                      test_testany.c                      2)
add_mpi_trace_case(testsome                     test_testsome.c                     2)

# -----------------------------------------------------------------------------
# Register Fortran tests only if available/enabled
# -----------------------------------------------------------------------------

if(MPI_TRACE_HAVE_FORTRAN_TESTS)
    add_mpi_trace_fortran_case(fortran_nonblocking_wait             test_fortran_nonblocking_wait.f90             2)
    add_mpi_trace_fortran_case(fortran_nonblocking_any_source_wait  test_fortran_nonblocking_any_source_wait.f90  3)
    add_mpi_trace_fortran_case(fortran_waitall                      test_fortran_waitall.f90                      2)
    add_mpi_trace_fortran_case(fortran_waitany                      test_fortran_waitany.f90                      2)
    add_mpi_trace_fortran_case(fortran_testall                      test_fortran_testall.f90                      2)
endif()

# -----------------------------------------------------------------------------
# Convenience target
# -----------------------------------------------------------------------------

add_custom_target(check-mpi-trace
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L mpi-trace
    DEPENDS
        ${MPI_TRACE_LIBRARY_TARGET}
        ${_mpi_trace_case_targets}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    USES_TERMINAL
)

