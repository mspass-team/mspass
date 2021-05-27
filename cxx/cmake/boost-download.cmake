cmake_minimum_required(VERSION 3.4)

project(boost-download NONE)

include(ExternalProject)

ExternalProject_Add(
  boost
  SOURCE_DIR "@BOOST_DOWNLOAD_ROOT@/boost-src"
  URL
    https://boostorg.jfrog.io/artifactory/main/release/1.71.0/source/boost_1_71_0.tar.gz
  URL_HASH
    SHA256=96b34f7468f26a141f6020efb813f1a2f3dfb9797ecf76a7d7cbd843cc95f5bd
  UPDATE_COMMAND wget https://svn.boost.org/trac10/raw-attachment/ticket/11120/python_jam.patch && patch tools/build/src/tools/python.jam python_jam.patch
  CONFIGURE_COMMAND ./bootstrap.sh --prefix=${PROJECT_BINARY_DIR}
  BUILD_COMMAND ./b2 cxxflags=-fPIC --with-serialization -j 8
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND ./b2 install
  TEST_COMMAND ""
  )
