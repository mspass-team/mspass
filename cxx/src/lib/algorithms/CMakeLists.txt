FILE(GLOB sources_alg_basics 
      agc.cc 
      bundle.cc
      seismogram_helpers.cc 
      slice_and_dice.cc 
      sparse_convolve.cc 
      tseries_helpers.cc
      Butterworth.cc)
FILE(GLOB sources_amplitudes 
      amplitudes.cc)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${GSL_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include)

add_library(alg_basics OBJECT ${sources_alg_basics})
add_library(amplitudes OBJECT ${sources_amplitudes})

add_subdirectory(deconvolution)

#add_library(algorithms OBJECT $<TARGET_OBJECTS:alg_basics> $<TARGET_OBJECTS:amplitudes> $<TARGET_OBJECTS:deconvolution>)
#target_link_libraries(algorithms PRIVATE seismic utility ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES} ${GSL_LIBRARIES})
