add_executable(test_scale_contract test_scale_contract.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${GSL_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(test_scale_contract PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
