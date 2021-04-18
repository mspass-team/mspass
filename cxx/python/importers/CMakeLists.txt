include_directories(
  ${MSEED_INCLUDE_DIR}
  ${pylib11_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_BINARY_DIR}/include)

#add_library(mspasspython STATIC ${sources_python})
#target_link_libraries(mspasspython PRIVATE mspass ${PYTHON_LIBRARIES})

pybind11_add_module(pyimport import_py.cc)

target_link_libraries(pyimport PRIVATE import ${MSEED_LIBRARIES})
set_target_properties(pyimport PROPERTIES OUTPUT_NAME "importers")
if(DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY)
  set_target_properties(pyimport PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ccore)
endif()
