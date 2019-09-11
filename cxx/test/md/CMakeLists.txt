add_executable(test_md EXCLUDE_FROM_ALL test_md.cc)
configure_file(simple.txt simple.txt COPYONLY)
configure_file(test_md.out test_md.out COPYONLY)
configure_file(test_md.pf test_md.pf COPYONLY)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)
link_directories(${Boost_LIBRARY_DIRS})

target_link_libraries(test_md PUBLIC mspass)
