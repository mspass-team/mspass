macro(fetch_boost _download_module_path _download_root)
    set(BOOST_DOWNLOAD_ROOT ${_download_root})
    configure_file(
        ${_download_module_path}/boost-download.cmake
        ${_download_root}/CMakeLists.txt
        @ONLY
        )
    unset(BOOST_DOWNLOAD_ROOT)

    execute_process(
        COMMAND
            "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY
            ${_download_root}
        )
    execute_process(
        COMMAND
            "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY
            ${_download_root}
        )

    set (BOOST_ROOT ${PROJECT_BINARY_DIR}/boost)
#    find_package (Boost 1.71.0 REQUIRED)
endmacro()
