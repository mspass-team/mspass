FILE(GLOB sources_python *.cpp)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_BINARY_DIR}/include)

add_library(mspasspython STATIC ${sources_python})
target_link_libraries(mspasspython PRIVATE mspass ${Boost_LIBRARIES} ${PYTHON_LIBRARIES})

PYTHON_ADD_MODULE(mspasspy mspasspy.cpp)

target_link_libraries(mspasspy PUBLIC mspasspython ${Boost_LIBRARIES})

install (TARGETS mspasspy DESTINATION lib)
