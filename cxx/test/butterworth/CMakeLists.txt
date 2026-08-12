add_executable(test_butterworth test_butterworth.cc)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${GSL_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_butterworth PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
