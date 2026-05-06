# Ensure MPI is found
find_package(MPI REQUIRED COMPONENTS C)

# Define common compiler flags
set(COMMON_COMPILER_FLAGS -Wall -Wextra -O2)

# =========================================================================
# File-Based Tracing Library (LD_PRELOAD target)
# =========================================================================
add_library(mpitrace_file SHARED 
    mpi_communication_tracking.c 
    backend_file.c
)

target_link_libraries(mpitrace_file PRIVATE MPI::MPI_C)
target_include_directories(mpitrace_file PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(mpitrace_file PRIVATE ${COMMON_COMPILER_FLAGS})
set_target_properties(mpitrace_file PROPERTIES POSITION_INDEPENDENT_CODE ON)

# =========================================================================
# Shared-Memory Telemetry Library (LD_PRELOAD target)
# =========================================================================
add_library(mpitrace_shm SHARED 
    mpi_communication_tracking.c 
    backend_shm.c
)

# SHM requires 'rt' (POSIX Realtime) for shm_open, and 'pthread' for atomics/forking
target_link_libraries(mpitrace_shm PRIVATE MPI::MPI_C rt pthread)
target_include_directories(mpitrace_shm PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(mpitrace_shm PRIVATE ${COMMON_COMPILER_FLAGS})
set_target_properties(mpitrace_shm PROPERTIES POSITION_INDEPENDENT_CODE ON)

# =========================================================================
# The Standalone Telemetry Daemon Executable
# =========================================================================

# Ask CMake to locate the cURL headers and binaries on the system
find_package(CURL REQUIRED)

# Declare the executable
add_executable(telemetry_daemon telemetry_daemon.c)

# Add the cURL include directories so the compiler can find <curl/curl.h>
target_include_directories(telemetry_daemon PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${CURL_INCLUDE_DIRS}
)

# Link the Daemon against MPI, Realtime (rt), Pthreads, and the cURL libraries
target_link_libraries(telemetry_daemon PRIVATE 
    MPI::MPI_C 
    rt 
    pthread 
    ${CURL_LIBRARIES}
)

target_compile_options(telemetry_daemon PRIVATE ${COMMON_COMPILER_FLAGS})
