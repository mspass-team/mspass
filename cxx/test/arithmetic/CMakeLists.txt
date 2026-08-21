add_executable(test_waveform_arithmetic test_waveform_arithmetic.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_waveform_arithmetic PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
