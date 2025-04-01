cmake_minimum_required(VERSION 3.11)

project(openblas-download NONE)

include(ExternalProject)

ExternalProject_Add(
  openblas
  SOURCE_DIR "@OPENBLAS_DOWNLOAD_ROOT@/openblas-src"
  GIT_REPOSITORY
    https://github.com/OpenMathLib/OpenBLAS.git 
  GIT_TAG
    v0.3.27
  UPDATE_COMMAND ""
  CONFIGURE_COMMAND ""
  BUILD_COMMAND make libs -j 8 PREFIX=${PROJECT_BINARY_DIR} NO_SHARED=1 USE_THREAD=0 USE_OPENMP=0 DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 TARGET=GENERIC
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install PREFIX=${PROJECT_BINARY_DIR} NO_SHARED=1
  TEST_COMMAND ""
  )
