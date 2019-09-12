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
            make install PREFIX=${_download_root}
        WORKING_DIRECTORY
            ${_download_root}/openblas-src
        )
endmacro()
