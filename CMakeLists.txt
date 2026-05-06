cmake_minimum_required(VERSION 3.10)

project(Pyrite VERSION 1.0 LANGUAGES C)

# Find the MPI library system-wide for C
find_package(MPI REQUIRED COMPONENTS C)

include(CTest)
enable_testing()

add_subdirectory(src)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

add_subdirectory(examples)

