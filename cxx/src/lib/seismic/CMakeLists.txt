FILE(GLOB sources_utility *.cc)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${CBLAS_INCLUDE_DIR}
  ${PROJECT_SOURCE_DIR}/include)

add_library(seismic STATIC ${sources_utility})
target_link_libraries(seismic PUBLIC ${Boost_LIBRARIES} ${CBLAS_LIBRARIES})

install (TARGETS seismic DESTINATION lib)
