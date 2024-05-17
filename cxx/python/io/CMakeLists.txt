include_directories(
  ${MSEED_INCLUDE_DIR}
  ${pylib11_INCLUDE_DIRS}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_BINARY_DIR}/include
  ${Boost_INCLUDE_DIRS}
)

#add_library(mspasspython STATIC ${sources_python})
#target_link_libraries(mspasspython PRIVATE mspass ${Python_LIBRARIES})

#pybind11_add_module(pyio io_py.cc)
Python_add_library(pyio io_py.cc)
target_link_libraries(pyio PRIVATE mspass ${MSEED_LIBRARIES} ${Boost_LIBRARIES} pybind11::headers)
set_target_properties(pyio PROPERTIES OUTPUT_NAME "io")
if(DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY)
  set_target_properties(pyio PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ccore)
endif()
