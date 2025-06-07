.. _advanced_setup_considerations:


Advanced Setup Considerations
=============================

Handling Conda and pip installations
------------------------------------

When building MsPASS from the source, it is common to use pip to install the Python bindings after compiling the C++ core.
The recommended command from the root of the source tree is:

.. code-block:: bash

    pip install --user ./

This command installs MsPASS locally within the user's ``~/.local`` directory on Linux systems.
The behavior on macOS should be similar, although slight variations in directory structures can occur.

Conda vs. pip Precedence
~~~~~~~~~~~~~~~~~~~~~~~~

If both a local pip installation (in ``~/.local``) and a Conda environment installation exist,
the system's behavior depends on the environment configuration and the order of paths in the ``PATH`` environment variable.
Typically, Python packages installed with ``--user`` are placed in a path that has precedence over Conda environments.
You can verify which installation is currently active by using:

.. code-block:: bash

    which python
    python -c "import mspasspy; print(mspasspy.__path__)"

This will show you the path of the Python interpreter and the location of the MsPASS module being used, helping you determine which version is being prioritized.

Understanding pip and Conda
~~~~~~~~~~~~~~~~~~~~~~~~~~~

While both pip and Conda are package managers, they handle package installations differently.
Conda manages packages and the environment to which they are installed, ensuring compatibility and avoiding conflicts.
pip, on the other hand, installs Python packages and may not consider the broader environment, leading to potential conflicts.

- **Use Conda for environments and binary dependencies**: It is best to use Conda to create environments and manage packages that have binary dependencies.
- **Use pip for Python-only packages not available on Conda**: If a package is only available via pip and does not have complex dependencies, it can be safely installed in a Conda environment.

**Best Practices**:
Avoid using ``sudo pip install`` as it may change files that are managed by the system's package manager, potentially leading to inconsistent states.
Always prefer installing packages within a user environment (``--user``) or within a virtual environment managed by Conda or virtualenv.
