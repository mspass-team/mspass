add_executable(test_spectrum test_spectrum.cc)
#configure_file(test_spectrum.out test_spectrum.out COPYONLY)
include_directories(
    ${Boost_INCLUDE_DIRS}
    ${GSL_INCLUDE_DIRS}
    ${pybind11_INCLUDE_DIR}
    ${Python_INCLUDE_DIRS}
    ${PROJECT_BINARY_DIR}/include
    ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(test_spectrum PRIVATE mspass ${Boost_LIBRARIES})
