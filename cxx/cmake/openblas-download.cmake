cmake_minimum_required(VERSION 3.4)

project(openblas-download NONE)

include(ExternalProject)

ExternalProject_Add(
  openblas
  SOURCE_DIR "@OPENBLAS_DOWNLOAD_ROOT@/openblas-src"
  GIT_REPOSITORY
    https://github.com/xianyi/OpenBLAS.git 
  GIT_TAG
    v0.3.7
  UPDATE_COMMAND ""
  CONFIGURE_COMMAND ""
  BUILD_COMMAND make libs -j 8 PREFIX=${PROJECT_BINARY_DIR} NO_SHARED=1 USE_THREAD=0 USE_OPENMP=0
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install PREFIX=${PROJECT_BINARY_DIR} NO_SHARED=1
  TEST_COMMAND ""
  )
