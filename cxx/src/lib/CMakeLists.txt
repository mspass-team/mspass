add_subdirectory(utility)
add_subdirectory(seismic)
add_subdirectory(algorithms)
add_subdirectory(deconvolution)

file(WRITE ${CMAKE_BINARY_DIR}/src/dummy.cc "")
add_library(mspass ${CMAKE_BINARY_DIR}/src/dummy.cc)
target_link_libraries(mspass PUBLIC seismic utility algorithms deconvolution)

install (TARGETS mspass DESTINATION lib)
