include_directories(
  ${Boost_INCLUDE_DIRS}
  ${GSL_INCLUDE_DIRS}
  ${pylib11_INCLUDE_DIRS}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_BINARY_DIR}/include)

#add_library(mspasspython STATIC ${sources_python})
#target_link_libraries(mspasspython PRIVATE mspass ${Python_LIBRARIES})

#pybind11_add_module(pyutility utility_py.cc)
Python_add_library(pyutility utility_py.cc)
target_link_libraries(pyutility PRIVATE mspass ${Boost_LIBRARIES} pybind11::headers)
set_target_properties(pyutility PROPERTIES OUTPUT_NAME "utility")
if(DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY)
  set_target_properties(pyutility PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ccore)
endif()


