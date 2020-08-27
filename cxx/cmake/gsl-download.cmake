cmake_minimum_required(VERSION 3.4)

project(gsl-download NONE)

include(ExternalProject)

ExternalProject_Add(
  gsl
  SOURCE_DIR "@GSL_DOWNLOAD_ROOT@/gsl-src"
  URL
    http://mirror.us-midwest-1.nexcess.net/gnu/gsl/gsl-2.6.tar.gz
  URL_HASH
    SHA256=b782339fc7a38fe17689cb39966c4d821236c28018b6593ddb6fd59ee40786a8
  CONFIGURE_COMMAND ./configure --enable-shared=no --prefix=${PROJECT_BINARY_DIR} CFLAGS=-fPIC
  BUILD_COMMAND make -j 8
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install
  TEST_COMMAND ""
  )
