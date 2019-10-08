add_subdirectory(utility)
add_subdirectory(seismic)

file(WRITE ${CMAKE_BINARY_DIR}/src/dummy.cc "")
add_library(mspass ${CMAKE_BINARY_DIR}/src/dummy.cc)
target_link_libraries(mspass PUBLIC seismic utility)

install (TARGETS mspass DESTINATION lib)
