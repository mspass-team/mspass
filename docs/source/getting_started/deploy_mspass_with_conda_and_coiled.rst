.. _deploy_mspass_with_conda_and_coiled:

Deploy MsPASS with Conda and Coiled
===================================

Overview
--------

`Coiled <https://www.coiled.io/>`__ can create a Dask cluster in an AWS,
Google Cloud, or Azure account.  MsPASS does not include or configure Coiled,
and Coiled does not provide MongoDB.  You therefore need:

* a local Conda environment containing MsPASS and Coiled,
* a cloud account configured for Coiled, and
* a MongoDB service that the cloud workers can reach.

Cloud resources incur charges while they are running.  Review your provider's
pricing and quotas before creating a cluster.

1. Install MsPASS and Coiled
----------------------------

Create a separate environment and install both packages:

.. code-block:: bash

   conda create --name mspass \
       --channel mspass \
       --channel conda-forge \
       mspasspy coiled
   conda activate mspass

The environment name ``mspass`` is only an example.  See
:ref:`deploy_mspass_with_conda` for more information about the MsPASS Conda
package.

2. Connect Coiled to your cloud account
----------------------------------------

Authenticate the local client:

.. code-block:: console

   coiled login

Then follow the current `Coiled setup guide
<https://docs.coiled.io/user_guide/setup/index.html>`__ for your cloud
provider.  The setup process requests permission before creating identity and
network resources in your account.  Site-specific networking may require help
from your cloud administrator.

3. Configure MongoDB
--------------------

Use MongoDB Atlas or another MongoDB deployment that is reachable from every
Coiled worker.  Follow the `Atlas connection instructions
<https://www.mongodb.com/docs/atlas/connect-to-database-deployment/>`__ to
create a database user and configure network access.

Keep credentials out of notebooks and source control.  For a simple test,
place the connection string in an environment variable:

.. code-block:: bash

   export MSPASS_MONGODB_URI='mongodb://<encoded-credentials>@<host-list>/?<options>'

Pass that variable to workers using the secret or environment mechanism
recommended by Coiled.  Do not make a production database public merely to
allow worker access.

You can test the connection on a machine where the variable is available:

.. code-block:: python

   import os
   from mspasspy.db.client import DBClient

   dbclient = DBClient(os.environ["MSPASS_MONGODB_URI"])
   print(dbclient.admin.command("ping"))
   db = dbclient["mspass"]

This test does not prove that cloud workers can reach the database.  Test from
a worker before starting a large job.

4. Create a Dask cluster
------------------------

The basic Coiled workflow is:

.. code-block:: python

   from coiled import Cluster

   cluster = Cluster(n_workers=2)
   client = cluster.get_client()

   def inc(x):
       return x + 1

   future = client.submit(inc, 10)
   print(future.result())  # 11

The workers must receive an environment containing ``mspasspy``.  Coiled may
be able to reproduce the active local environment automatically; consult its
current `software environment documentation
<https://docs.coiled.io/user_guide/software/index.html>`__ if imports fail on
a worker.

Files indexed on your local computer are not automatically available to cloud
workers.  Store waveform data in storage accessible to every worker, or use
MongoDB GridFS.  See :ref:`Input and Output in MsPASS <io>`.

Close the client and cluster when the work finishes so cloud instances do not
continue to incur charges:

.. code-block:: python

   client.close()
   cluster.close()

For current cluster options and troubleshooting, use the
`Coiled documentation <https://docs.coiled.io/>`__.
