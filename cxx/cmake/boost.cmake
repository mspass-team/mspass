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
    set (Boost_NO_BOOST_CMAKE ON)
    set (Boost_USE_STATIC_LIBS ON)

    find_package (PythonInterp)

    if (PYTHONINTERP_FOUND)
        FIND_PACKAGE(Boost 1.71.0 REQUIRED COMPONENTS python${PYTHON_VERSION_MAJOR}${PYTHON_VERSION_MINOR})
    else()
        message(FATAL_ERRORO "Python not found")
    endif()
endmacro()
