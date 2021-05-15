FILE(GLOB sources_io *.cc)
include_directories(
  ${MSEED_INCLUDE_DIR}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include
)

add_library(io OBJECT ${sources_io})
