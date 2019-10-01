add_executable(test_dmatrix test_dmatrix.cc)
include_directories(
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_dmatrix PRIVATE mspass)
