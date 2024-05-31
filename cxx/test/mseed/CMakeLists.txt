add_executable(test_mseed test_mseed.cc)
configure_file(test.msd test.msd COPYONLY)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_mseed PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
