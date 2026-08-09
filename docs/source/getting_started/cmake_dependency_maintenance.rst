.. _cmake_dependency_maintenance:

Maintaining C++ Dependencies
============================

This page is for core contributors who add or change a compiled dependency.
End users building MsPASS should follow the :ref:`source-build instructions
<advanced_setup_considerations>` instead.

The official CMake `Using Dependencies Guide
<https://cmake.org/cmake/help/latest/guide/using-dependencies/index.html>`__
is the best general introduction.  MsPASS currently uses a system-first
variant of that model: use an installed package when CMake can find it and,
for selected dependencies, build a pinned source release otherwise.

How dependency discovery works
------------------------------

``cxx/CMakeLists.txt`` is the entry point for the compiled build.  Most
dependencies follow this sequence:

#. Call `find_package
   <https://cmake.org/cmake/help/latest/command/find_package.html>`__ without
   ``REQUIRED`` so an installed package is preferred.
#. If the package was not found, include ``cxx/cmake/<package>.cmake`` and
   call its ``fetch_<package>`` macro.
#. The macro configures ``cxx/cmake/<package>-download.cmake`` as a small,
   separate CMake project.  That project uses `ExternalProject
   <https://cmake.org/cmake/help/latest/module/ExternalProject.html>`__ to
   download, build, and install a pinned release beneath the MsPASS build
   directory.
#. The macro sets the package-specific search hint, such as
   ``<Package>_DIR`` or ``<Package>_ROOT``, then calls ``find_package`` with
   ``REQUIRED``.  The rest of the build therefore consumes the same package
   variables or imported targets for either discovery path.
#. The library and header information is attached to the targets that use the
   dependency.  ``cxx/src/lib/CMakeLists.txt`` links the combined ``mspass``
   library; component and Python-extension ``CMakeLists.txt`` files add any
   direct include or link requirements.

The current tree contains concrete examples for yaml-cpp, Boost, GSL,
OpenBLAS, libmseed, and pybind11 under ``cxx/cmake``.  yaml-cpp, Boost, GSL,
BLAS/LAPACK, and pybind11 first try an installed package.  libmseed is always
built from its pinned source release.  Python itself must already be present.

Files that normally change
--------------------------

Adding a dependency requires checking each layer below.  Only add a file or
entry when that layer actually consumes or distributes the dependency.

``cxx/CMakeLists.txt``
   Add discovery, the optional source-build fallback, and status output that
   identifies the resolved library and include locations.

``cxx/cmake/<package>.cmake``
   Add this orchestration macro only when MsPASS will build the package if it
   is absent.  It should configure and build the companion download project,
   set the correct search hint, and finish with a required package lookup.

``cxx/cmake/<package>-download.cmake``
   Define the pinned source URL or Git tag, checksum when an archive is used,
   configure/build/install commands, and an install prefix inside the build
   tree.  Static libraries linked into the Python extension must be compiled
   as position-independent code; see CMake's
   `POSITION_INDEPENDENT_CODE
   <https://cmake.org/cmake/help/latest/prop_tgt/POSITION_INDEPENDENT_CODE.html>`__
   reference.

Consumer ``CMakeLists.txt`` files
   Add the dependency to the narrowest target that needs it.  Prefer an
   imported target supplied by the package when available; otherwise use the
   include and library variables exposed by its Find module or config file.
   See `target_link_libraries
   <https://cmake.org/cmake/help/latest/command/target_link_libraries.html>`__.

Package and container manifests
   Keep ``pyproject.toml``, ``scripts/dependency_map.toml``, ``meta.yaml``,
   ``scripts/conda_build.sh``, and the relevant Docker image stages aligned.
   A dependency built only as a private static fallback may not need a runtime
   package entry, but a shared library required after installation does.

CI and tests
   Update ``.github/workflows/cmake.yml`` or a focused test when the existing
   build does not exercise the new discovery or fallback behavior.

Package search hints
--------------------

Do not create an MsPASS package-config file merely to make ``find_package``
work.  CMake may use either a bundled ``Find<Package>.cmake`` module or a
``<Package>Config.cmake``/``<package>-config.cmake`` file installed by the
dependency.

For a dependency installed outside standard system prefixes, configure with
``CMAKE_PREFIX_PATH`` set to its installation prefix.  For config-mode
packages, ``<Package>_DIR`` must name the directory containing the package's
config file, not the library file or the prefix above it.  Package names and
their result variables are case-sensitive; use the spelling documented by
the corresponding Find module or upstream package.

Validation checklist
--------------------

Before opening a pull request for a compiled dependency change:

#. Configure and build with the dependency installed by the system or active
   environment.
#. In a clean container or environment where it is absent, configure again
   and verify that the intended fallback is selected and installed beneath
   the build directory.  Do not uninstall packages from a working developer
   machine just to exercise this path.
#. Build the Python extensions and run CTest:

   .. code-block:: bash

      cmake -S cxx -B /tmp/mspass-cmake-build -DCMAKE_BUILD_TYPE=Release
      cmake --build /tmp/mspass-cmake-build --parallel
      ctest --test-dir /tmp/mspass-cmake-build --output-on-failure

#. If package manifests changed, verify their generated copies:

   .. code-block:: bash

      python3 scripts/sync_dependencies.py --check

#. Confirm both configure logs identify the expected headers and libraries.
   A successful download alone is insufficient; the final required
   ``find_package`` call and the consuming target must resolve the built
   package.

Common failures
---------------

* A static archive used by a Python extension was not compiled with ``-fPIC``
  or ``CMAKE_POSITION_INDEPENDENT_CODE=ON``.
* ``<Package>_DIR`` points at an installation prefix instead of the directory
  containing the config file.
* The package name has the wrong case, so CMake selects a different search
  mode or fails to load its Find module.
* Headers were added globally but the library was not linked to the consuming
  target, or the inverse.
* A Git branch was used without a fixed tag or commit, making builds change
  over time.
* Only the installed-package path was tested, leaving a broken fallback that
  CI never exercises.
