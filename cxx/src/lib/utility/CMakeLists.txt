FILE(GLOB sources_utility *.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${CBLAS_INCLUDE_DIR}
  ${PROJECT_SOURCE_DIR}/include)

add_library(mspass STATIC ${sources_utility})
target_link_libraries(mspass PUBLIC ${Boost_LIBRARIES} ${CBLAS_LIBRARIES})

install (TARGETS mspass DESTINATION lib)
