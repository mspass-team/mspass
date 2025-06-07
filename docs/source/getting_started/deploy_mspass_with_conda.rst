.. _deploy_mspass_with_conda:

Deploy MsPASS with Conda
===========================

Installing the MsPASS Conda package
-------------------------------------

MsPASS is available as a Conda package, offering a convenient method for installing a local copy suitable for desktop environments.
This installation method is ideal for users primarily focused on development, although it may also be beneficial in other scenarios.
This guide focuses on setting up MsPASS for desktop usage.

First Steps: Installing Conda
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before installing MsPASS, ensure you have a Conda distribution installed. If not, follow the system-specific instructions provided by Anaconda:

- For a comprehensive setup on personal devices, a full installation of Anaconda is recommended. Visit the `Anaconda installation guide <https://docs.anaconda.com/free/anaconda/install/>`_ for detailed instructions.
- For shared systems, lighter versions such as `Miniconda <https://docs.anaconda.com/free/miniconda/miniconda-install/>`_ or `Mamba <https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html>`_ are advised.

Creating an Isolated Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
