add_subdirectory(utility)
add_subdirectory(seismic)
add_subdirectory(algorithms)
add_subdirectory(io)

add_library(mspass $<TARGET_OBJECTS:seismic> $<TARGET_OBJECTS:utility> $<TARGET_OBJECTS:alg_basics> $<TARGET_OBJECTS:amplitudes> $<TARGET_OBJECTS:deconvolution> $<TARGET_OBJECTS:io>)
target_link_libraries(mspass 
    PRIVATE 
    ${BLAS_LIBRARIES} 
    ${LAPACK_LIBRARIES} 
    ${YAML_CPP_LIBRARIES} 
    ${GSL_LIBRARIES} 
    ${MSEED_LIBRARIES} 
    ${Boost_LIBRARIES}
)
target_link_libraries(mspass 
    INTERFACE 
    ${BLAS_LIBRARIES} 
    ${LAPACK_LIBRARIES} 
    ${YAML_CPP_LIBRARIES} 
    ${GSL_LIBRARIES} 
    ${MSEED_LIBRARIES}
    ${Boost_LIBRARIES}
)
#set_target_properties(mspass PROPERTIES LINK_FLAGS "-Wl,--unresolved-symbols=ignore-in-shared-libs -Wl,--export-dynamic")

install (TARGETS mspass DESTINATION lib)
