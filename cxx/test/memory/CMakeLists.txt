add_executable(test_memory_use test_memory_use.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_memory_use PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
