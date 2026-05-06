include(CheckLanguage)

# -----------------------------------------------------------------------------
# Optional Fortran support for wrapper/helper layer
#
# Values:
#   AUTO : enable if compiler + MPI Fortran are available
#   ON   : require Fortran support
#   OFF  : disable Fortran support
# -----------------------------------------------------------------------------

set(MPI_TRACE_FORTRAN_SUPPORT
    "AUTO"
    CACHE STRING
    "Enable Fortran wrapper/helper support: AUTO, ON, or OFF"
)
set_property(CACHE MPI_TRACE_FORTRAN_SUPPORT PROPERTY STRINGS AUTO ON OFF)

# Always require C MPI
find_package(MPI REQUIRED COMPONENTS C)

set(MPI_TRACE_HAVE_FORTRAN OFF)

if(MPI_TRACE_FORTRAN_SUPPORT STREQUAL "OFF")

    # C-only build
    set(MPI_TRACE_HAVE_FORTRAN OFF)

elseif(MPI_TRACE_FORTRAN_SUPPORT STREQUAL "ON")

    check_language(Fortran)
    if(NOT CMAKE_Fortran_COMPILER)
        message(FATAL_ERROR
            "MPI_TRACE_FORTRAN_SUPPORT=ON but no Fortran compiler was found."
        )
    endif()

    enable_language(Fortran)
    find_package(MPI REQUIRED COMPONENTS Fortran)
    set(MPI_TRACE_HAVE_FORTRAN ON)

elseif(MPI_TRACE_FORTRAN_SUPPORT STREQUAL "AUTO")

    check_language(Fortran)
    if(CMAKE_Fortran_COMPILER)
        enable_language(Fortran)
        find_package(MPI QUIET COMPONENTS Fortran)
        if(MPI_Fortran_FOUND)
            set(MPI_TRACE_HAVE_FORTRAN ON)
            message(STATUS "Fortran wrapper/helper support enabled in src/")
        else()
            message(STATUS "MPI Fortran not found; building C-only tracing libraries")
        endif()
    else()
        message(STATUS "No Fortran compiler found; building C-only tracing libraries")
    endif()

else()
    message(FATAL_ERROR
        "Invalid value for MPI_TRACE_FORTRAN_SUPPORT='${MPI_TRACE_FORTRAN_SUPPORT}'. "
        "Expected AUTO, ON, or OFF."
    )
endif()

# -----------------------------------------------------------------------------
# Common sources, flags, and optional Fortran additions
# -----------------------------------------------------------------------------

set(COMMON_C_COMPILER_FLAGS -Wall -Wextra -O2)
set(COMMON_Fortran_COMPILER_FLAGS -O2)

set(MPI_TRACE_COMMON_SOURCES
    mpi_communication_tracking.c
)

set(MPI_TRACE_COMMON_LIBS
    MPI::MPI_C
)

set(MPI_TRACE_COMMON_DEFINES
)

if(MPI_TRACE_HAVE_FORTRAN)
    list(APPEND MPI_TRACE_COMMON_SOURCES fortran_mpi_helper.f90)
    list(APPEND MPI_TRACE_COMMON_LIBS MPI::MPI_Fortran)
    list(APPEND MPI_TRACE_COMMON_DEFINES
        MPI_TRACE_ENABLE_FORTRAN_SUPPORT=1
        MPI_TRACE_ENABLE_FORTRAN_HELPER=1
    )
endif()

# =========================================================================
# File-Based Tracing Library (LD_PRELOAD target)
# =========================================================================
add_library(mpitrace_file SHARED
    ${MPI_TRACE_COMMON_SOURCES}
    backend_file.c
)

target_link_libraries(mpitrace_file PRIVATE ${MPI_TRACE_COMMON_LIBS})
target_include_directories(mpitrace_file PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(mpitrace_file PRIVATE ${MPI_TRACE_COMMON_DEFINES})
target_compile_options(mpitrace_file PRIVATE
    $<$<COMPILE_LANGUAGE:C>:${COMMON_C_COMPILER_FLAGS}>
    $<$<COMPILE_LANGUAGE:Fortran>:${COMMON_Fortran_COMPILER_FLAGS}>
)
set_target_properties(mpitrace_file PROPERTIES POSITION_INDEPENDENT_CODE ON)

# =========================================================================
# Shared-Memory Telemetry Library (LD_PRELOAD target)
# =========================================================================
add_library(mpitrace_shm SHARED
    ${MPI_TRACE_COMMON_SOURCES}
    backend_shm.c
)

target_link_libraries(mpitrace_shm PRIVATE ${MPI_TRACE_COMMON_LIBS} rt pthread)
target_include_directories(mpitrace_shm PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(mpitrace_shm PRIVATE ${MPI_TRACE_COMMON_DEFINES})
target_compile_options(mpitrace_shm PRIVATE
    $<$<COMPILE_LANGUAGE:C>:${COMMON_C_COMPILER_FLAGS}>
    $<$<COMPILE_LANGUAGE:Fortran>:${COMMON_Fortran_COMPILER_FLAGS}>
)
set_target_properties(mpitrace_shm PROPERTIES POSITION_INDEPENDENT_CODE ON)

# =========================================================================
# The Standalone Telemetry Daemon Executable
# =========================================================================

find_package(CURL REQUIRED)

add_executable(telemetry_daemon telemetry_daemon.c)

target_include_directories(telemetry_daemon PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CURL_INCLUDE_DIRS}
)

target_link_libraries(telemetry_daemon PRIVATE
    MPI::MPI_C
    rt
    pthread
    ${CURL_LIBRARIES}
)

target_compile_options(telemetry_daemon PRIVATE ${COMMON_C_COMPILER_FLAGS})

