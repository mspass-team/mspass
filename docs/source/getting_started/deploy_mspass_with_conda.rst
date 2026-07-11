.. _deploy_mspass_with_conda:

Deploy MsPASS with Conda
========================

The ``mspasspy`` Conda package is a convenient way to install MsPASS in a
local Python environment.  It is especially useful for development or for
running MsPASS from your own scripts and notebooks.

Install Conda
-------------

If Conda is not already installed, follow the `official Conda installation
instructions
<https://docs.conda.io/projects/conda/en/stable/user-guide/install/index.html>`__.
Miniconda is sufficient; a full Anaconda installation is not required.

Create an environment and install MsPASS
-----------------------------------------

Create a separate environment to avoid conflicts with other Python packages:

.. code-block:: bash

   conda create --name mspass_env --channel mspass --channel conda-forge mspasspy
   conda activate mspass_env

Conda will select a compatible Python version.  Current MsPASS source and
package builds support Python 3.10 through 3.13, but not every build is
necessarily published on the main channel.  Available packages are listed on
the `MsPASS Anaconda Cloud page <https://anaconda.org/mspass/mspasspy>`__.

Verify the installation
-----------------------

After activating the environment, verify that both the Python package and its
compiled extension can be imported:

.. code-block:: bash

   python -c "import mspasspy; from mspasspy.ccore.seismic import TimeSeries; print(mspasspy.__file__)"

The printed path should be inside ``mspass_env``.  If it points to a source
checkout or ``~/.local``, leave the source directory, clear any custom
``PYTHONPATH``, and activate the environment again.

About ``MSPASS_HOME``
---------------------

The Conda package includes the standard MsPASS schema and parameter files, so
you do not normally need to clone the repository or set ``MSPASS_HOME``.

Some older C++-backed interfaces still look for defaults below
``$MSPASS_HOME/data``.  If one of those interfaces reports that a data file is
missing, set ``MSPASS_HOME`` to the installed package directory for that
shell:

.. code-block:: bash

   export MSPASS_HOME="$(python -c 'from pathlib import Path; import mspasspy; print(Path(mspasspy.__file__).resolve().parent)')"

MongoDB and other services
--------------------------

The Conda package installs the libraries used to connect to MongoDB and Dask,
but it does not start a MongoDB server, scheduler, workers, or JupyterLab.
If you want a complete desktop environment with those services already
configured, use the :ref:`desktop quick start <quick_start>` instead.

If you already manage MongoDB or a distributed scheduler, connect to those
services from the Conda environment using the settings for your deployment.
See :ref:`Database Concepts <database_concepts>` and
:ref:`CRUD Operations in MsPASS <CRUD_operations>` for the database model and
client examples.

For source builds, editable installs, or other custom environments, continue
with :ref:`Advanced Setup Considerations <advanced_setup_considerations>`.
