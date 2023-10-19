macro(fetch_yaml_cpp _download_module_path _download_root)
    set(YAML_CPP_DOWNLOAD_ROOT ${_download_root})
    configure_file(
        ${_download_module_path}/yaml-cpp-download.cmake
        ${_download_root}/CMakeLists.txt
        @ONLY
        )
    unset(YAML_CPP_DOWNLOAD_ROOT)

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
#    list (APPEND CMAKE_MODULE_PATH ${PROJECT_BINARY_DIR}/yaml-cpp/lib/cmake/yaml-cpp/)
    set (yaml-cpp_DIR ${PROJECT_BINARY_DIR}/yaml-cpp/lib/cmake/yaml
-cpp/)
    find_package (yaml-cpp REQUIRED)
endmacro()
