cmake_minimum_required(VERSION 3.4)

project(boost-download NONE)

include(ExternalProject)

ExternalProject_Add(
  boost
  SOURCE_DIR "@BOOST_DOWNLOAD_ROOT@/boost-src"
  BINARY_DIR "@BOOST_DOWNLOAD_ROOT@/boost-build"
  URL
    https://dl.bintray.com/boostorg/release/1.71.0/source/boost_1_71_0.tar.bz2
  URL_HASH
    SHA256=d73a8da01e8bf8c7eda40b4c84915071a8c8a0df4a6734537ddde4a8580524ee
  CONFIGURE_COMMAND "./bootstrap.sh --prefix=${PROJECT_BINARY_DIR}/boost"
  BUILD_COMMAND "./b2"
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND "./b2 install"
  TEST_COMMAND ""
  )
