cmake_minimum_required(VERSION 3.4)

project(mseed-download NONE)

include(ExternalProject)

ExternalProject_Add(
  mseed
  SOURCE_DIR "@MSEED_DOWNLOAD_ROOT@/mseed-src"
  URL
    https://github.com/iris-edu/libmseed/archive/refs/tags/v3.0.8.tar.gz
  URL_HASH
    SHA256=989e15e3dfe8469d86535a4654d280638f3ef4b21f75994f59ce5b68c0538496
  CONFIGURE_COMMAND ""
  BUILD_COMMAND CFLAGS=-fPIC make -j 8
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND cp libmseed.a libmseed.h ${PROJECT_BINARY_DIR}
  TEST_COMMAND ""
  )
