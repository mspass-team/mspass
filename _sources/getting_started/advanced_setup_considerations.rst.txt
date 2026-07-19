.. _advanced_setup_considerations:

Advanced Setup Considerations
=============================

This page is for contributors and developers installing MsPASS from a source
checkout.  Most users should use the :ref:`Docker quick start <quick_start>`
or the :ref:`Conda installation guide <deploy_mspass_with_conda>`.

Source builds
-------------

MsPASS is not a Python-only package.  Installing it invokes CMake to compile
the ``mspasspy.ccore`` C++ extension.  Follow the project's
`source-build instructions
<https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__
for the required compilers and libraries.  The current package metadata
requires Python 3.10 or newer.

Use an isolated environment
---------------------------

Create and activate a Conda environment or Python virtual environment before
building.  An isolated environment avoids conflicts with the operating
system's Python packages and with other MsPASS checkouts.

After activation, confirm that Python and pip refer to the intended
environment:

.. code-block:: bash

   python -c "import sys; print(sys.executable)"
   python -m pip --version

Install from the root of the source tree with the command specified by the
source-build procedure.  When that procedure has already installed all
dependencies, the local installation command is:

.. code-block:: bash

   python -m pip install --no-deps -v .

Using ``python -m pip`` ensures that pip installs for the active interpreter.
Do not add ``--user`` inside a Conda or virtual environment: it can install a
second copy outside the environment.  Also avoid ``sudo pip``, which can
modify files managed by the operating system.

Check which copy is imported
----------------------------

If multiple installations may exist, check both the selected interpreter and
the imported package:

.. code-block:: bash

   python -c "import sys, mspasspy; print(sys.executable); print(mspasspy.__file__)"
   python -m pip show mspasspy

The interpreter, imported package, and location reported by pip should all
refer to the intended environment or checkout.  ``PATH`` chooses the Python
executable, but it does not by itself control Python's package search path.

If an old installation is imported, first identify its location with the
commands above.  Remove it with the interpreter that owns that installation;
do not delete arbitrary files from ``site-packages``.  Old ``PYTHONPATH``
entries can also shadow a new build, so start a clean shell and unset that
variable when diagnosing an unexpected import:

.. code-block:: bash

   unset PYTHONPATH

Activate the intended environment again, rebuild, and repeat the location
check before running tests.
