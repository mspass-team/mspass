add_executable(test_md test_md.cc)
configure_file(simple.txt simple.txt COPYONLY)
configure_file(test_md.out test_md.out COPYONLY)
configure_file(test_md.pf test_md.pf COPYONLY)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_md PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
