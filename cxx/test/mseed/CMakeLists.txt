add_executable(test_mseed test_mseed.cc)
add_executable(test_mseed_recovery test_mseed_recovery.cc)
configure_file(test.msd test.msd COPYONLY)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${MSEED_INCLUDE_DIR}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_mseed PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
target_link_libraries(test_mseed_recovery PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
