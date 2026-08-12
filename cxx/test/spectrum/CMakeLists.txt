add_executable(test_spectrum test_spectrum.cc)
add_executable(test_spectral_snr test_spectral_snr.cc)
add_executable(test_frequency_grid test_frequency_grid.cc)
#configure_file(test_spectrum.out test_spectrum.out COPYONLY)
include_directories(
    ${Boost_INCLUDE_DIRS}
    ${GSL_INCLUDE_DIRS}
    ${pybind11_INCLUDE_DIR}
    ${Python_INCLUDE_DIRS}
    ${PROJECT_BINARY_DIR}/include
    ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(test_spectrum PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
target_link_libraries(test_spectral_snr PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
add_test(NAME test_spectral_snr COMMAND test_spectral_snr)
target_link_libraries(test_frequency_grid PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
