add_executable(test_amap test_amap.cc)
configure_file(axref_test.pf axref_test.pf COPYONLY)
configure_file(test_amap.out test_amap.out COPYONLY)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_amap PRIVATE mspass ${Boost_LIBRARIES})
