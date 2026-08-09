macro(fetch_boost _download_module_path _download_root)
    set(BOOST_DOWNLOAD_ROOT ${_download_root})
    set(BOOST_INSTALL_ROOT ${_download_root})
    if (DEFINED ENV{MSPASS_BOOST_ROOT} AND NOT "$ENV{MSPASS_BOOST_ROOT}" STREQUAL "")
        set(BOOST_INSTALL_ROOT "$ENV{MSPASS_BOOST_ROOT}")
    endif()
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
        RESULT_VARIABLE BOOST_CONFIGURE_RESULT
        )
    if (NOT BOOST_CONFIGURE_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to configure the Boost 1.86 build")
    endif()
    execute_process(
        COMMAND
            "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY
            ${_download_root}
        RESULT_VARIABLE BOOST_BUILD_RESULT
        )
    if (NOT BOOST_BUILD_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to build Boost 1.86 serialization")
    endif()

    set (BOOST_ROOT ${BOOST_INSTALL_ROOT})
    set (BOOST_INCLUDEDIR ${BOOST_INSTALL_ROOT}/include)
    set (BOOST_LIBRARYDIR ${BOOST_INSTALL_ROOT}/lib)
    set (Boost_NO_BOOST_CMAKE ON)
    set (Boost_NO_SYSTEM_PATHS ON)
    set (Boost_USE_STATIC_LIBS ON)

    # A failed initial find_package call can leave usable headers or libraries
    # from a different prefix in CMake's cache.  Clear those results so the
    # bundled build cannot mix its headers with an incompatible installed
    # Boost library.
    foreach (_boost_cache_var IN ITEMS
        Boost_DIR
        Boost_FOUND
        Boost_INCLUDE_DIR
        Boost_INCLUDE_DIRS
        Boost_LIBRARIES
        Boost_LIBRARY_DIRS
        Boost_LIBRARY_DIR_DEBUG
        Boost_LIBRARY_DIR_RELEASE
        Boost_SERIALIZATION_LIBRARY_DEBUG
        Boost_SERIALIZATION_LIBRARY_RELEASE)
        unset (${_boost_cache_var})
        unset (${_boost_cache_var} CACHE)
    endforeach ()

    set (_MSPASS_SAVED_CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH}")
    set (CMAKE_PREFIX_PATH "${BOOST_INSTALL_ROOT}")
    find_package (Boost 1.86.0 EXACT REQUIRED COMPONENTS serialization)
    set (CMAKE_PREFIX_PATH "${_MSPASS_SAVED_CMAKE_PREFIX_PATH}")
    unset (_MSPASS_SAVED_CMAKE_PREFIX_PATH)
endmacro()
