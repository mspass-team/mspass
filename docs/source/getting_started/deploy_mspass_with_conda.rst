.. _deploy_mspass_with_conda:

Deploy MsPASS with Conda
========================

Installing the MsPASS Conda package
-----------------------------------

MsPASS is available as a Conda package, offering a convenient method for installing a local copy suitable for desktop environments. 
This installation method is ideal for users primarily focused on development, although it may also be beneficial in other scenarios. 
This guide focuses on setting up MsPASS for desktop usage.

First Steps: Installing Conda
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before installing MsPASS, ensure you have a Conda distribution installed. If not, follow the system-specific instructions provided by Anaconda:

- For a comprehensive setup on personal devices, a full installation of Anaconda is recommended. Visit the `Anaconda installation guide <https://docs.anaconda.com/free/anaconda/install/>`_ for detailed instructions.
- For shared systems, lighter versions such as `Miniconda <https://docs.anaconda.com/free/miniconda/miniconda-install/>`_ or `Mamba <https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html>`_ are advised.

Creating an Isolated Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To prevent conflicts with pre-existing Python packages, it is strongly advised to create a separate Conda environment for MsPASS:

1. Ensure you are in the base environment, then create a new environment named ``mspass_env`` (you can choose a different name if preferred):

   .. code-block:: bash

       conda create --name mspass_env

2. Activate the new environment:

   .. code-block:: bash

       conda activate mspass_env

Adding Necessary Channels
~~~~~~~~~~~~~~~~~~~~~~~~~

To install MsPASS successfully, add the required channels to your Conda configuration:

.. code-block:: bash

    conda config --add channels mspass
    conda config --add channels conda-forge

Installing MsPASS
~~~~~~~~~~~~~~~~~

With the environment set and channels added, you can now install MsPASS:

.. code-block:: bash

    conda install -y mspasspy

The MsPASS Conda package is available on `Anaconda Cloud <https://anaconda.org/mspass/mspasspy>`_ and supports multiple platforms including Linux and macOS (both Intel and ARM architectures).

Define MSPASS_HOME
------------------

MsPASS requires specific data from the ``data/yaml`` directory found in the MsPASS code repository. 
Download a local copy of the repository and set the ``MSPASS_HOME`` environment variable to this directory or to a custom location if you prefer. 
Ensure the variable is correctly set in your shell's initialization file.

Setting up MongoDB
------------------

MsPASS workflows typically utilize MongoDB. 
Setting up a MongoDB instance on your desktop is easier using the standard MsPASS Docker container:

1. Open a terminal and navigate to the directory you wish to use for MongoDB data storage. 
   When launched, the MsPASS container will automatically create a ``data`` directory for workspace and a ``logs`` directory for server logs.

2. Execute the following Docker command to start the MongoDB server within the MsPASS container:

   .. code-block:: bash

       docker run --env MSPASS_ROLE=db -p 27017:27017 --mount src=`pwd`,target=/home,type=bind mspass/mspass

Note that the ``-p`` option is essential to expose the MongoDB server port (27017) for external access. 
If port 27017 is already in use on your system, modify this parameter as necessary.

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

