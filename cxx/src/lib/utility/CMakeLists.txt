FILE(GLOB sources_utility *.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${YAML_CPP_INCLUDE_DIR}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_BINARY_DIR}/include)

add_library(utility OBJECT ${sources_utility})
#target_link_libraries(utility PRIVATE ${BLAS_LIBRARIES} ${YAML_CPP_LIBRARIES} ${Python_LIBRARIES})

#install (TARGETS utility DESTINATION lib)
