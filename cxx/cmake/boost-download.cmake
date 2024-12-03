cmake_minimum_required(VERSION 3.4)

project(boost-download NONE)

include(ExternalProject)

ExternalProject_Add(
  boost
  SOURCE_DIR "@BOOST_DOWNLOAD_ROOT@/boost-src"
  URL
    https://boostorg.jfrog.io/artifactory/main/release/1.86.0/source/boost_1_86_0.tar.gz
  URL_HASH
    SHA256=2575e74ffc3ef1cd0babac2c1ee8bdb5782a0ee672b1912da40e5b4b591ca01f
  CONFIGURE_COMMAND ./bootstrap.sh --prefix=${PROJECT_BINARY_DIR} --with-toolset=gcc
  BUILD_COMMAND ./b2 cxxflags=-fPIC --with-serialization -j 8
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND ./b2 install
  TEST_COMMAND ""
  )
