cmake_minimum_required(VERSION 3.4)

project(boost-download NONE)

include(ExternalProject)

ExternalProject_Add(
  boost
  SOURCE_DIR "@BOOST_DOWNLOAD_ROOT@/boost-src"
  URL
    https://dl.bintray.com/boostorg/release/1.71.0/source/boost_1_71_0.tar.gz
  URL_HASH
    SHA256=96b34f7468f26a141f6020efb813f1a2f3dfb9797ecf76a7d7cbd843cc95f5bd
  CONFIGURE_COMMAND ./bootstrap.sh --prefix=${PROJECT_BINARY_DIR}
  BUILD_COMMAND ./b2 cxxflags=-fPIC --with-python
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND ./b2 install
  TEST_COMMAND ""
  )
