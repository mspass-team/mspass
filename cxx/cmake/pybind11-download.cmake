cmake_minimum_required(VERSION 3.4)

project(pybind11-download NONE)

include(ExternalProject)

ExternalProject_Add(
  pybind11
  SOURCE_DIR "@PYBIND11_DOWNLOAD_ROOT@/pybind11-src"
  GIT_REPOSITORY
    https://github.com/pybind/pybind11.git
  GIT_TAG
    v2.6.0
  UPDATE_COMMAND bash -c "pip3 show pytest || pip3 install --user pytest"
  CONFIGURE_COMMAND cmake -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR} -DPYBIND11_TEST=OFF .
  BUILD_COMMAND make -j 8
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install 
  TEST_COMMAND ""
  )
