add_executable(test_amap test_amap.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_amap PRIVATE mspass)
