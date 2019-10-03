macro(fetch_openblas _download_module_path _download_root)
    set(OPENBLAS_DOWNLOAD_ROOT ${_download_root})
    configure_file(
        ${_download_module_path}/openblas-download.cmake
        ${_download_root}/CMakeLists.txt
        @ONLY
        )
    unset(OPENBLAS_DOWNLOAD_ROOT)

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

#    set (BLAS_LIBRARIES ${PROJECT_BINARY_DIR}/openblas/lib/libopenblas.so)
endmacro()
