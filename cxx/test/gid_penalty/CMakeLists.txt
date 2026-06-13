add_executable(test_gid_penalty test_gid_penalty.cc)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_gid_penalty PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
