add_executable(test_dmatrix EXCLUDE_FROM_ALL test_dmatrix.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${CBLAS_INCLUDE_DIR}
  ${PROJECT_SOURCE_DIR}/include/)
link_directories(${Boost_LIBRARY_DIRS})

target_link_libraries(test_dmatrix PUBLIC mspass ${CBLAS_LIBRARIES})
