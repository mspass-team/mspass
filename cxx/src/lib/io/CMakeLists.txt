FILE(GLOB sources_io *.cc)
include_directories(
  ${MSEED_INCLUDE_DIR}
  ${pybind11_INCLUDE_DIR}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include
)

add_library(io OBJECT ${sources_io})
