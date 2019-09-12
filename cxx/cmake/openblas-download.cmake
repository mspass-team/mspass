cmake_minimum_required(VERSION 3.4)

project(openblas-download NONE)

include(ExternalProject)

ExternalProject_Add(
  openblas
  SOURCE_DIR "@OPENBLAS_DOWNLOAD_ROOT@/openblas-src"
  BINARY_DIR "@OPENBLAS_DOWNLOAD_ROOT@/openblas-build"
  GIT_REPOSITORY
    https://github.com/xianyi/OpenBLAS.git 
  GIT_TAG
    v0.3.7
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND ""
  TEST_COMMAND ""
  )
