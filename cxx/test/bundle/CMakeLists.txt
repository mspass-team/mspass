add_executable(test_bundle test_bundle.cc)
#configure_file(test_bundle.out test_bundle.out COPYONLY)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_bundle PRIVATE mspass ${Boost_LIBRARIES})
