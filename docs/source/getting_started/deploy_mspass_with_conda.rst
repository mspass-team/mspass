.. _deploy_mspass_with_conda:

Install the MsPASS Conda package
================================

The Conda distribution installs the ``mspasspy`` Python package and its
compiled extension into a local environment.  It is a good fit when you want
to import MsPASS from your own scripts, notebooks, or development tools and
will manage the runtime services yourself.

This is intentionally a narrower path than the :ref:`desktop quick start
<quick_start>`.  Installing the package does **not** start MongoDB, a Dask
scheduler, a Spark master, workers, or JupyterLab.  Use the quick start when
you want a complete local runtime instead of separately managed components.

Prerequisites
-------------

Before starting, you need:

* A working Conda client.  If ``conda --version`` does not succeed, follow the
  `official Conda installation instructions
  <https://docs.conda.io/projects/conda/en/stable/user-guide/install/index.html>`__
  and initialize Conda for your shell.
* Network access to the ``mspass`` and ``conda-forge`` channels.
* An ``mspasspy`` build that matches your operating system, architecture, and
  chosen Python version.  Check the actual builds on the `MsPASS Anaconda
  Cloud page <https://anaconda.org/mspass/mspasspy>`__ before pinning a Python
  version.  The current source metadata requires Python 3.10 or newer, but
  that requirement alone does not guarantee that a package was published for
  every platform and Python combination.

The commands below use a POSIX shell and name the environment
``mspass_env``.  You can choose a different environment name.

Create the environment and install MsPASS
------------------------------------------

Create a new environment and install ``mspasspy`` in one transaction:

.. code-block:: bash

   conda create --name mspass_env \
       --override-channels \
       --channel mspass \
       --channel conda-forge \
       mspasspy
   conda activate mspass_env

The channel options apply only to this command; they do not add entries to
your user-wide Conda configuration.  ``--override-channels`` also prevents
unrelated channels already listed in that configuration from participating in
the solve.  Omit that option if your site requires an administrator-provided
mirror, but do not run ``conda config --add channels ...`` merely for this
installation.

Unless you have confirmed that a matching build exists, let Conda select a
compatible Python version.  Keep source checkouts and user-site packages out
of this environment so that they do not shadow the installed package.

Verify the installation
-----------------------

Run the following check after activation.  It verifies the interpreter and
package locations, imports the native extension, and loads the default schema
bundled in the package.  It does not require MongoDB to be running.

.. code-block:: bash

   python - <<'PY'
   import os
   import sys
   from importlib.metadata import version
   from pathlib import Path

   import mspasspy
   from mspasspy.ccore.seismic import TimeSeries
   from mspasspy.db.schema import DatabaseSchema

   prefix = Path(os.environ["CONDA_PREFIX"]).resolve()
   package_dir = Path(mspasspy.__file__).resolve().parent
   try:
       package_dir.relative_to(prefix)
   except ValueError as error:
       raise SystemExit(
           f"MsPASS was imported from {package_dir}, outside {prefix}"
       ) from error

   TimeSeries()
   DatabaseSchema()
   print("Python:", sys.executable)
   print("mspasspy:", version("mspasspy"))
   print("package:", package_dir)
   PY

The Python executable and package path should both be inside the active Conda
environment.  If the package path instead names a source checkout or a user
site such as ``~/.local``, leave that directory, clear any stale
``PYTHONPATH``, reactivate the environment, and run the check again.  See
:ref:`Advanced Setup Considerations <advanced_setup_considerations>` before
using a source build or editable installation.

When ``MSPASS_HOME`` is needed
------------------------------

Do not set ``MSPASS_HOME`` just to import ``mspasspy``.  The Conda package
includes its ``data/yaml`` and ``data/pf`` resources.  Python loaders such as
the default database schema and ``Janitor`` use those installed resources when
``MSPASS_HOME`` is unset.

Some older and C++-backed paths still resolve defaults through
``$MSPASS_HOME/data``.  These include the C++ metadata-definition and
attribute-map loaders and parameter-file defaults used by the Antelope/CSS
interfaces.  If your workflow uses one of those paths, point ``MSPASS_HOME``
at the installed ``mspasspy`` package directory for the current shell:

.. code-block:: bash

   export MSPASS_HOME="$(python -c 'from pathlib import Path; import mspasspy; print(Path(mspasspy.__file__).resolve().parent)')"
   test -f "${MSPASS_HOME}/data/yaml/mspass.yaml"
   test -f "${MSPASS_HOME}/data/pf/attribute_maps.pf"

A repository clone is not required merely to supply those installed data
files.  If you intentionally maintain custom schema or parameter files under
another ``data`` tree, set ``MSPASS_HOME`` to the parent of that tree instead
and treat those files as part of your workflow configuration.

MongoDB and schedulers are separate services
---------------------------------------------

The package includes client-side libraries such as PyMongo and Dask.  It does
not install or launch a ``mongod`` server, start a Dask scheduler or workers,
or provide a running Spark service.  A successful import therefore confirms
the package installation only; it does not confirm that a database or
distributed-compute service is reachable.

For an integrated MongoDB, scheduler, worker, and JupyterLab environment, use
the :ref:`desktop quick start <quick_start>`.  If you already operate those
services, configure your MsPASS code with their addresses according to your
deployment.

Optional local MongoDB helper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you need only a local MongoDB server and already have Docker, the MsPASS
image's ``db`` role starts MongoDB without starting a scheduler, workers, or
JupyterLab.  The following example publishes MongoDB only on the host loopback
interface and stores its database and logs in a named volume:

.. code-block:: bash

   docker volume create mspass-conda-mongodb
   docker run --name mspass-conda-mongodb --detach \
       --env MSPASS_ROLE=db \
       --publish 127.0.0.1:27017:27017 \
       --mount type=volume,source=mspass-conda-mongodb,target=/home \
       mspass/mspass:latest

The container startup script writes the database below ``/home/db/data`` and
logs below ``/home/logs``; both persist in ``mspass-conda-mongodb``.  Binding
to ``127.0.0.1`` avoids exposing an unauthenticated development database on
other host interfaces.  This is a single-host development service, not a
production MongoDB deployment.

If host port ``27017`` is already in use, change only the host side of the
publication, for example to ``--publish 127.0.0.1:27018:27017``, and use the
same host port in the client URI below (``mongodb://127.0.0.1:27018``).

After activating ``mspass_env``, verify the service through the MsPASS client:

.. code-block:: bash

   python - <<'PY'
   from mspasspy.db.client import DBClient

   client = DBClient(
       "mongodb://127.0.0.1:27017",
       serverSelectionTimeoutMS=5000,
   )
   print(client.admin.command("ping"))
   client.close()
   PY

Stop the container without deleting the named volume, and restart it later,
with:

.. code-block:: console

   docker stop --timeout 60 mspass-conda-mongodb
   docker start mspass-conda-mongodb

Where to go next
----------------

* Use the :ref:`desktop quick start <quick_start>` for the complete local
  runtime and the safest first user experience.
* Read :ref:`Advanced Setup Considerations
  <advanced_setup_considerations>` before building from a checkout or mixing
  Conda with local source installs.
* Continue with :ref:`Database Concepts <database_concepts>` for MsPASS's data
  model and :ref:`CRUD Operations in MsPASS <CRUD_operations>` for practical
  database access patterns.
