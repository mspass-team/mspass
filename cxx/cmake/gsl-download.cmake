cmake_minimum_required(VERSION 3.11)

project(gsl-download NONE)

include(ExternalProject)

ExternalProject_Add(
  gsl
  SOURCE_DIR "@GSL_DOWNLOAD_ROOT@/gsl-src"
  URL
    https://mirror.ibcp.fr/pub/gnu/gsl/gsl-latest.tar.gz
  CONFIGURE_COMMAND ./configure --enable-shared=no --prefix=${PROJECT_BINARY_DIR} CFLAGS=-fPIC
  BUILD_COMMAND make -j 8
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install
  TEST_COMMAND ""
  )
