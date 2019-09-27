add_executable(test_amap test_amap.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)
link_directories(${Boost_LIBRARY_DIRS})

target_link_libraries(test_amap PUBLIC mspass)
