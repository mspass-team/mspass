FILE(GLOB sources_alg_basics 
      agc.cc 
      seismogram_helpers.cc 
      slice_and_dice.cc 
      sparse_convolve.cc 
      tseries_helpers.cc)
FILE(GLOB sources_amplitudes 
      amplitudes.cc)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include)

add_library(alg_basics STATIC ${sources_alg_basics})
add_library(amplitudes STATIC ${sources_amplitudes})
target_link_libraries(alg_basics PRIVATE utility seismic ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES})
target_link_libraries(amplitudes PRIVATE utility seismic ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES})

add_subdirectory(deconvolution)

file(WRITE ${CMAKE_BINARY_DIR}/src/dummy.cc "")
add_library(algorithms ${CMAKE_BINARY_DIR}/src/dummy.cc)
target_link_libraries(algorithms PUBLIC alg_basics amplitudes deconvolution)

install (TARGETS algorithms DESTINATION lib)
