FILE(GLOB sources_deconvolution *.cc *.c)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${GSL_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include)

add_library(deconvolution OBJECT ${sources_deconvolution})
#target_link_libraries(deconvolution PRIVATE seismic utility alg_basics ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES} ${GSL_LIBRARIES} )
