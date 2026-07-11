.. _advanced_setup_considerations:


Advanced Setup Considerations
=============================

This page is for contributors and developers who install MsPASS from a source
checkout.  Most users do not need a source build.  Use the :ref:`Docker quick
start <quick_start>` for the simplest first run, or the :ref:`Conda deployment
guide <deploy_mspass_with_conda>` for a packaged Python environment.

Source installs include native code
-----------------------------------

MsPASS is not a Python-only package.  Its build backend invokes CMake to
compile the ``mspasspy.ccore`` C++ extension.  Follow the project's
`source-build instructions
<https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__
for the required compilers, libraries, and complete build procedure.  A
``pip`` command by itself is not a substitute for those prerequisites.

The practices below supplement that guide by keeping the Python interpreter,
installer, and imported package in the same environment.

Use an isolated environment
---------------------------

Use a separate environment for source development instead of installing into
the system Python.  Conda is convenient when native dependencies must also be
managed.  For example:

.. code-block:: bash

   conda create --name mspass-dev python=3.12
   conda activate mspass-dev

A virtual environment is also suitable when the required native libraries
are already available on the host:

.. code-block:: bash

   python3 -m venv .venv
   source .venv/bin/activate

Use one environment per independent checkout or build configuration.  After
activation, confirm that both Python and pip belong to it:

.. code-block:: bash

   python -c "import sys; print(sys.executable)"
   python -m pip --version

Use ``python -m pip``
---------------------

When following the source-build procedure, invoke pip as ``python -m pip``.
This binds pip to the interpreter selected above; a bare ``pip`` or ``pip3``
command can resolve to another installation on ``PATH``.

After the source-build guide's prerequisites and Python dependencies are
installed, run the installation from the root of the source tree:

.. code-block:: bash

   python -m pip install --no-deps -v .

``--no-deps`` is appropriate here because the prepared environment already
contains the required dependencies.  It prevents pip from trying to resolve
and replace that environment's dependency set; do not use it as a substitute
for installing the prerequisites in the source-build guide.

Do not add ``--user`` while a Conda or virtual environment is active.
``--user`` targets the per-user site (or is rejected when that site is
disabled) instead of reliably installing into the active environment.  It can
therefore leave a second MsPASS installation that competes with the intended
one.  Also avoid ``sudo pip`` or ``sudo python -m pip``: those commands can
modify files managed by the operating system and create root-owned build
artifacts.

Verify the interpreter and import location
------------------------------------------

``PATH`` selects the ``python`` executable, but Python resolves imports from
``sys.path``.  Check both the interpreter and the actual imported package
after every install or rebuild:

.. code-block:: bash

   python - <<'PY'
   import site
   import sys
   import mspasspy

   user_site = site.getusersitepackages()
   print("Python:", sys.executable)
   print("MsPASS:", mspasspy.__file__)
   print("User site:", user_site)
   print("User site enabled:", site.ENABLE_USER_SITE)
   print("User site on sys.path:", user_site in sys.path)
   PY
   python -m pip show mspasspy

The printed Python executable should be inside the active environment.  The
MsPASS path and the ``Location`` reported by pip should identify that same
environment (or the expected source checkout for an editable install).  An
MsPASS path inside the printed user-site directory indicates a user-site
installation; its default location varies by operating system and Python
distribution.

Prevent stale user-site imports
-------------------------------

If the location checks show that a user-site copy is shadowing the intended
environment, do not try to fix the import by rearranging ``PATH``.  Before
hiding anything, record the user-site directory and the installation metadata:

.. code-block:: bash

   python -m site --user-site
   python -m pip show mspasspy

Only remove the copy if the ``Location`` reported by pip is inside the printed
user-site directory.  Use that same interpreter so pip removes the
corresponding installation:

.. code-block:: bash

   python -m pip uninstall mspasspy

If the two locations do not match, do not uninstall or delete arbitrary files
from ``site-packages``.  Identify the interpreter that installed the stale
copy and repeat the checks with that interpreter.

Finally, start a clean shell, activate the intended environment, and exclude
the user site and old checkout paths while rebuilding:

.. code-block:: bash

   export PYTHONNOUSERSITE=1
   unset PYTHONPATH

Run the location checks again before rebuilding.

For future builds, activate the intended environment first, keep
``PYTHONPATH`` free of old checkout paths, use ``python -m pip`` without
``--user``, and verify ``sys.executable`` and ``mspasspy.__file__`` before
running tests.
