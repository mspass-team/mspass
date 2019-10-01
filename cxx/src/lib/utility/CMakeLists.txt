FILE(GLOB sources_utility *.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_BINARY_DIR}/include)

add_library(mspass STATIC ${sources_utility})
target_link_libraries(mspass PRIVATE ${Boost_LIBRARIES} ${BLAS_LIBRARIES})

install (TARGETS mspass DESTINATION lib)
