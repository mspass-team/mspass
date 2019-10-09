cmake_minimum_required (VERSION 2.8)

project (mspass Fortran C CXX)

#set (default_build_type "Release")
set (default_build_type "Debug")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message (STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set (CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING "Choose the type of build." FORCE)
endif ()

set (CMAKE_CXX_STANDARD 11)
set (CMAKE_CXX_FLAGS_DEBUG "-Wall -O0 -g")
set (CMAKE_CXX_FLAGS_RELEASE "-Wall -O3")

find_package (Boost 1.64.0)
if (NOT Boost_FOUND)
  message (STATUS "Building Boost")
  include (cmake/boost.cmake)
  fetch_boost (
    ${PROJECT_SOURCE_DIR}/cmake
    ${PROJECT_BINARY_DIR}/boost
    )
endif ()

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

find_package (PythonInterp)

if (PYTHONINTERP_FOUND)
  if (UNIX AND NOT APPLE)
    if (PYTHON_VERSION_MAJOR EQUAL 3)
        FIND_PACKAGE(Boost COMPONENTS python${PYTHON_VERSION_SUFFIX})
        FIND_PACKAGE(PythonInterp 3)
        FIND_PACKAGE(PythonLibs 3 REQUIRED)
    else()
        FIND_PACKAGE(Boost COMPONENTS python)
        FIND_PACKAGE(PythonInterp)
        FIND_PACKAGE(PythonLibs REQUIRED)
    endif()
  else()	
    if (PYTHON_VERSION_MAJOR EQUAL 3)
        FIND_PACKAGE(Boost COMPONENTS python${PYTHON_VERSION_MAJOR}${PYTHON_VERSION_MINOR})
        FIND_PACKAGE(PythonInterp 3)
        FIND_PACKAGE(PythonLibs 3 REQUIRED)
    else()
        FIND_PACKAGE(Boost COMPONENTS python${PYTHON_VERSION_MAJOR}${PYTHON_VERSION_MINOR})
        FIND_PACKAGE(PythonInterp)
        FIND_PACKAGE(PythonLibs REQUIRED)
    endif()
  endif()
else()
    message("Python not found")
endif()

message(STATUS "PYTHON_LIBRARIES = ${PYTHON_LIBRARIES}")
message(STATUS "PYTHON_EXECUTABLE = ${PYTHON_EXECUTABLE}")
message(STATUS "PYTHON_INCLUDE_DIRS = ${PYTHON_INCLUDE_DIRS}")
message(STATUS "Boost_LIBRARIES = ${Boost_LIBRARIES}")

add_subdirectory (src)
add_subdirectory (include)

add_subdirectory (python)

enable_testing ()
add_subdirectory (test)
