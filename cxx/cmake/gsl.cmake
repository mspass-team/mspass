macro(fetch_gsl _download_module_path _download_root)
    set(GSL_DOWNLOAD_ROOT ${_download_root})
    configure_file(
        ${_download_module_path}/gsl-download.cmake
        ${_download_root}/CMakeLists.txt
        @ONLY
        )
    unset(GSL_DOWNLOAD_ROOT)

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

    set (GSL_ROOT_DIR ${PROJECT_BINARY_DIR}/gsl)

    find_package (gsl REQUIRED)
endmacro()
