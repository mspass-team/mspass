cmake_minimum_required(VERSION 3.4)

project(mseed-download NONE)

include(ExternalProject)

ExternalProject_Add(
  mseed
  SOURCE_DIR "@MSEED_DOWNLOAD_ROOT@/mseed-src"
  URL
    https://github.com/iris-edu/libmseed/archive/refs/tags/v2.19.6.tar.gz
  URL_HASH
    SHA256=80b7f653589a30dcb4abcd53fe6bc1276f634262ee673815e12abdc1ff179d1d
  CONFIGURE_COMMAND ""
  BUILD_COMMAND make -j 8
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND cp libmseed.a libmseed.h ${PROJECT_BINARY_DIR}
  TEST_COMMAND ""
  )
