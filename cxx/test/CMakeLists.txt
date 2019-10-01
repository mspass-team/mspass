include(CTest)
if(BUILD_TESTING)
  add_subdirectory(dmatrix)
  add_subdirectory(md)
  add_subdirectory(amap)
  add_subdirectory(tcs)

  add_test(NAME test_dmatrix COMMAND ${PROJECT_BINARY_DIR}/test/dmatrix/test_dmatrix)
endif()
