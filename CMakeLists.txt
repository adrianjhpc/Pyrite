cmake_minimum_required(VERSION 3.10)

project(MPICommunicationTracking VERSION 1.0 LANGUAGES C)

# Find the MPI library system-wide
find_package(MPI REQUIRED)

include(CTest)
enable_testing()

add_subdirectory(src)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
add_subdirectory(examples)
