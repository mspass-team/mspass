macro(fetch_mseed _download_module_path _download_root)
    set(MSEED_DOWNLOAD_ROOT ${_download_root})
    configure_file(
        ${_download_module_path}/mseed-download.cmake
        ${_download_root}/CMakeLists.txt
        @ONLY
        )
    unset(MSEED_DOWNLOAD_ROOT)

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

    set (MSEED_LIBRARIES ${PROJECT_BINARY_DIR}/mseed/libmseed.a)
    set (MSEED_INCLUDE_DIR ${PROJECT_BINARY_DIR}/mseed/libmseed.h)
endmacro()
