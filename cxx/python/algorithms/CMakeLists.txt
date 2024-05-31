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

#pybind11_add_module(pybasic basic_py.cc)
Python_add_library(pybasic basic_py.cc)
target_link_libraries(pybasic PRIVATE mspass ${Boost_LIBRARIES} pybind11::headers)
set_target_properties(pybasic PROPERTIES OUTPUT_NAME "basic")

#pybind11_add_module(pyamplitudes amplitudes_py.cc)
Python_add_library(pyamplitudes amplitudes_py.cc)
target_link_libraries(pyamplitudes PRIVATE mspass ${Boost_LIBRARIES} pybind11::headers)
set_target_properties(pyamplitudes PROPERTIES OUTPUT_NAME "amplitudes")

#pybind11_add_module(pydeconvolution deconvolution_py.cc)
Python_add_library(pydeconvolution deconvolution_py.cc)
target_link_libraries(pydeconvolution PRIVATE mspass ${Boost_LIBRARIES} pybind11::headers)
set_target_properties(pydeconvolution PROPERTIES OUTPUT_NAME "deconvolution")

if(DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY)
  set_target_properties(pybasic PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ccore/algorithms)
  set_target_properties(pyamplitudes PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ccore/algorithms)
  set_target_properties(pydeconvolution PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ccore/algorithms)
endif()

