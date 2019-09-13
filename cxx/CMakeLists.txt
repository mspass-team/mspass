cmake_minimum_required (VERSION 3.4)

project (mspass)

set (default_build_type "Release")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message (STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set (CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING "Choose the type of build." FORCE)
endif ()

set (CMAKE_CXX_STANDARD 11)
set (CMAKE_CXX_FLAGS_DEBUG "-Wall -O0 -g")
set (CMAKE_CXX_FLAGS_RELEASE "-Wall -O3")

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

find_package (Boost 1.71.0)
if (NOT Boost_FOUND)
  include (cmake/boost.cmake)
  fetch_boost (
    ${PROJECT_SOURCE_DIR}/cmake
    ${PROJECT_BINARY_DIR}/boost
    )
endif ()

find_package (CBLAS)
if (NOT CBLAS_FOUND)
  include (cmake/openblas.cmake)
  fetch_openblas (
    ${PROJECT_SOURCE_DIR}/cmake
    ${PROJECT_BINARY_DIR}/openblas
    )
endif ()

add_subdirectory (src)
add_subdirectory (include)

enable_testing ()
add_subdirectory (test)
