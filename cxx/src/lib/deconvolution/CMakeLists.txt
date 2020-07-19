FILE(GLOB sources_deconvolution *.cc *.c)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${GSL_INCLUDE_DIRS}
  ${pylib11_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include)

add_library(deconvolution STATIC ${sources_deconvolution})
target_link_libraries(deconvolution PRIVATE seismic utility algorithms ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES} ${GSL_LIBRARIES} )

install (TARGETS deconvolution DESTINATION lib)
