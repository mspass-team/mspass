add_executable(test_taper test_taper.cc)
configure_file(test_taper.out test_taper.out COPYONLY)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_taper PRIVATE mspass ${Boost_LIBRARIES} )
