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
            ./bootstrap.sh --prefix=${_download_root}
        WORKING_DIRECTORY
            ${_download_root}/boost-src
        )
    execute_process(
        COMMAND
            ./b2 install
        WORKING_DIRECTORY
            ${_download_root}/boost-src
        )
endmacro()
