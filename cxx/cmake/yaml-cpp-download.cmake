cmake_minimum_required(VERSION 3.11)

project(yaml-cpp-download NONE)

include(ExternalProject)

ExternalProject_Add(
  yaml-cpp
  SOURCE_DIR "@YAML_CPP_DOWNLOAD_ROOT@/yaml-cpp-src"
  GIT_REPOSITORY
    https://github.com/jbeder/yaml-cpp.git
  GIT_TAG
    0.8.0
  UPDATE_COMMAND ""
  CONFIGURE_COMMAND cmake -DYAML_CPP_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR} .
  BUILD_COMMAND make -j 8
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install
  TEST_COMMAND ""
  )
