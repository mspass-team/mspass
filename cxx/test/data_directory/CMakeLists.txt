add_executable(test_data_directory test_data_directory.cc)
include_directories(${Boost_INCLUDE_DIRS} ${PROJECT_SOURCE_DIR}/include/)
target_link_libraries(test_data_directory PRIVATE mspass)
