include(CTest)
if(BUILD_TESTING)
  add_subdirectory(amap)
  add_subdirectory(dmatrix)
  add_subdirectory(md)
  add_subdirectory(spectrum)
  add_subdirectory(splicing)
  add_subdirectory(taper)
  add_subdirectory(tcs)
  add_subdirectory(tswgaps)
  add_subdirectory(history)
  add_subdirectory(bundle)
  add_subdirectory(memory)
  add_subdirectory(mseed)

  add_test(NAME test_dmatrix COMMAND ${PROJECT_BINARY_DIR}/test/dmatrix/test_dmatrix)
#  add_test(NAME test_Metadata COMMAND ${PROJECT_BINARY_DIR}/test/md/test_md)
#  add_test(NAME test_amap COMMAND ${PROJECT_BINARY_DIR}/test/amap/test_amap)
#  set_tests_properties(test_amap PROPERTIES ENVIRONMENT MSPASS_HOME=${PROJECT_SOURCE_DIR}/..)
  add_test(NAME test_splicing COMMAND ${PROJECT_BINARY_DIR}/test/splicing/test_splicing)
  add_test(NAME test_tcs COMMAND ${PROJECT_BINARY_DIR}/test/tcs/test_tcs)
  add_test(NAME test_tswgaps COMMAND ${PROJECT_BINARY_DIR}/test/tswgaps/test_tswgaps)
  add_test(NAME test_history COMMAND ${PROJECT_BINARY_DIR}/test/history/test_history)
  add_test(NAME test_bundle COMMAND ${PROJECT_BINARY_DIR}/test/bundle/test_bundle)
  add_test(NAME test_memory_use COMMAND ${PROJECT_BINARY_DIR}/test/memory/test_memory_use)
  add_test(NAME test_mseed COMMAND ${PROJECT_BINARY_DIR}/test/mseed/test_mseed ${PROJECT_BINARY_DIR}/test/mseed/test.msd)
endif()
