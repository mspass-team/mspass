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
  BUILD_COMMAND ${CMAKE_BUILD_TOOL}
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install PREFIX=${PROJECT_BINARY_DIR}
  TEST_COMMAND ""
  )
