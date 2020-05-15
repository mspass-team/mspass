add_executable(test_dmatrix test_dmatrix.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_dmatrix PRIVATE mspass ${Boost_LIBRARIES})
