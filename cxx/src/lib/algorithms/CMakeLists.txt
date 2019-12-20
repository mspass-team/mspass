FILE(GLOB sources_algorithms *.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include)

add_library(algorithms STATIC ${sources_algorithms})
target_link_libraries(algorithms PRIVATE utility ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES})

install (TARGETS algorithms DESTINATION lib)
