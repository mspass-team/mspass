add_executable(test_decon_serialization test_decon_serialization.cc)
configure_file(RFdeconProcessor.pf RFDeconProcessor.pf COPYONLY)
configure_file(CNRDeconEngine.pf CNRDeconEngine.pf COPYONLY)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${GSL_INCLUDE_DIRS}
  ${pybind11_INCLUDE_DIR}
  ${Python_INCLUDE_DIRS}
  ${PROJECT_BINARY_DIR}/include
  ${PROJECT_SOURCE_DIR}/include/)

target_link_libraries(test_decon_serialization PRIVATE mspass ${Boost_LIBRARIES} ${Python_LIBRARIES})
