#FILE(GLOB sources_python *.cpp)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${pylib11_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_BINARY_DIR}/include)

#add_library(mspasspython STATIC ${sources_python})
#target_link_libraries(mspasspython PRIVATE mspass ${PYTHON_LIBRARIES})

add_custom_target(make-mspasspy-dir ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory
  ${PROJECT_BINARY_DIR}/python/mspasspy)
add_custom_target(make-mspasspy-lib-dir ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory
  ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/mspasspy)

pybind11_add_module(ccore mspass_wrapper.cpp MongoDBConverter.cpp)
set_target_properties(ccore PROPERTIES OUTPUT_NAME mspasspy/ccore)

target_link_libraries(ccore PRIVATE mspass)

