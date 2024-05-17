add_executable(test_tswgaps test_tswgaps.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_tswgaps PRIVATE mspass ${Boost_LIBRARIES})
