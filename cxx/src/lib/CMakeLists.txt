add_subdirectory(utility)
add_subdirectory(seismic)

add_library(mspass seismic utility)
target_link_libraries(mspass PUBLIC seismic utility)

install (TARGETS mspass DESTINATION lib)
