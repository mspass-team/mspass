FILE(GLOB sources_import *.cc)
include_directories(
  ${MSEED_INCLUDE_DIR}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include
)

add_library(import OBJECT ${sources_import})
#target_link_libraries(import PRIVATE ${MSEED_LIBRARIES})

#install (TARGETS import DESTINATION lib)
