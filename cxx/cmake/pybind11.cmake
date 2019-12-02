macro(fetch_pybind11 _download_module_path _download_root)
    set(PYBIND11_DOWNLOAD_ROOT ${_download_root})
    configure_file(
        ${_download_module_path}/pybind11-download.cmake
        ${_download_root}/CMakeLists.txt
        @ONLY
        )
    unset(PYBIND11_DOWNLOAD_ROOT)

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

    set (pybind11_DIR ${PROJECT_BINARY_DIR}/pybind11/share/cmake/pybind11/)
    find_package (pybind11 REQUIRED)
endmacro()
