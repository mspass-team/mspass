#FILE(GLOB sources_python *.cpp)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${GSL_INCLUDE_DIRS}
  ${pylib11_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_BINARY_DIR}/include)

#add_library(mspasspython STATIC ${sources_python})
#target_link_libraries(mspasspython PRIVATE mspass ${PYTHON_LIBRARIES})

pybind11_add_module(ccore mspass_wrapper.cpp)

target_link_libraries(ccore PRIVATE mspass ${Boost_LIBRARIES})

