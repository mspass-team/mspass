cmake_minimum_required (VERSION 3.4)

project (mspass Fortran C CXX)

#set (default_build_type "Release")
set (default_build_type "Debug")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message (STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set (CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING "Choose the type of build." FORCE)
endif ()

set (CMAKE_CXX_STANDARD 14)
set (CMAKE_CXX_FLAGS_DEBUG "-Wall -O0 -g")
set (CMAKE_CXX_FLAGS_RELEASE "-Wall -O3")
set (CMAKE_POSITION_INDEPENDENT_CODE ON)
set (PYBIND11_CPP_STANDARD -std=c++14)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)

find_package (yaml-cpp 0.5.0)
if (NOT YAML_CPP_INCLUDE_DIR)
  message (STATUS "Building yaml-cpp")
  include (cmake/yaml-cpp.cmake)
  fetch_yaml_cpp (
    ${PROJECT_SOURCE_DIR}/cmake
    ${PROJECT_BINARY_DIR}/yaml-cpp
    )
endif ()
message (STATUS "YAML_CPP_LIBRARIES   = ${YAML_CPP_LIBRARIES}")
message (STATUS "YAML_CPP_INCLUDE_DIR = ${YAML_CPP_INCLUDE_DIR}")


#find_package (Boost 1.64.0)
find_package (Boost 1.64.0 REQUIRED COMPONENTS serialization )
if (NOT Boost_FOUND)
  message (STATUS "Building Boost")
  include (cmake/boost.cmake)
  fetch_boost (
    ${PROJECT_SOURCE_DIR}/cmake
    ${PROJECT_BINARY_DIR}/boost
    )
endif ()
message (STATUS "BOOSTROOT="${BOOSTROOT})
message (STATUS "Boost_LIBRARIES="${Boost_LIBRARIES})
message (STATUS "Boost_LIBRARY_DIR_RELEASE="${Boost_LIBRARY_DIR_RELEASE})
message (STATUS "Boost_INCLUDE_DIRS="${Boost_INCLUDE_DIRS})

find_package (BLAS)
if (NOT BLAS_FOUND)
  message (STATUS "Building OpenBLAS")
  include (cmake/openblas.cmake)
  fetch_openblas (
    ${PROJECT_SOURCE_DIR}/cmake
    ${PROJECT_BINARY_DIR}/openblas
    )
  set (BLAS_LIBRARIES ${PROJECT_BINARY_DIR}/openblas/lib/libopenblas.so)
endif ()

find_package (LAPACK)
if (NOT LAPACK_FOUND AND NOT BLAS_FOUND)
  message (STATUS "Use OpenBLAS for LAPACK")
  set (BLAS_LIBRARIES ${PROJECT_BINARY_DIR}/openblas/lib/libopenblas.so)
  set (LAPACK_LIBRARIES ${PROJECT_BINARY_DIR}/openblas/lib/libopenblas.so)
elseif (NOT LAPACK_FOUND)
  message (STATUS "Building OpenBLAS for LAPACK")
  include (cmake/openblas.cmake)
  fetch_openblas (
    ${PROJECT_SOURCE_DIR}/cmake
    ${PROJECT_BINARY_DIR}/openblas
    )
  set (LAPACK_LIBRARIES ${PROJECT_BINARY_DIR}/openblas/lib/libopenblas.so)
endif ()

include (FortranCInterface)
list (APPEND FORTRAN_FUNCTIONS ddot dscal daxpy dcopy)
list (APPEND FORTRAN_FUNCTIONS dgetrf dgetri)
FortranCInterface_HEADER (include/FC.h 
  MACRO_NAMESPACE "FC_"
  SYMBOLS ${FORTRAN_FUNCTIONS}
  )

find_package (PythonInterp 3)

if (PYTHONINTERP_FOUND)
  find_package (pybind11)
  if (NOT pybind11_FOUND)
    message (STATUS "Building Pybind11")
    include (cmake/pybind11.cmake)
    fetch_pybind11 (
      ${PROJECT_SOURCE_DIR}/cmake
      ${PROJECT_BINARY_DIR}/pybind11
      )
  endif ()
  message (STATUS "PYTHON_LIBRARIES = ${PYTHON_LIBRARIES}")
  message (STATUS "PYTHON_EXECUTABLE = ${PYTHON_EXECUTABLE}")
  message (STATUS "PYTHON_INCLUDE_DIRS = ${PYTHON_INCLUDE_DIRS}")
  message (STATUS "pybind11_LIBRARIES = ${pybind11_LIBRARIES}")
  message (STATUS "pybind11_INCLUDE_DIR= ${pybind11_INCLUDE_DIR}")
  add_subdirectory (python)
else ()
  message (WARNING "Python not found")
endif ()

add_subdirectory (src)
add_subdirectory (include)

enable_testing ()
add_subdirectory (test)
