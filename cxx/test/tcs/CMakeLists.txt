add_executable(test_tcs test_tcs.cc)
#configure_file(test_tcs.out test_tcs.out COPYONLY)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_tcs PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
