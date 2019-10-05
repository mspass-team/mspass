add_subdirectory(utility)
add_subdirectory(seismic)

add_library(mspass)
target_link_libraries(mspass PUBLIC seismic utility)

install (TARGETS mspass DESTINATION lib)
