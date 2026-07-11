.. _deploy_mspass_with_conda_and_coiled:

Deploy MsPASS with Conda and Coiled
=======================================

Overview
-------------
This section provides a concise summary of the steps required to run
MsPASS using Conda and Coiled. Coiled runs the Dask scheduler and workers
in your cloud account (AWS, GCP, or Azure), but it does not provide MongoDB.
You can also use only Conda to install MsPASS locally without Coiled (step 3).
If you are starting with a new environment, complete step 3 before steps 1
and 2.

1. Install Coiled
---------------------
Fetch and install Coiled following instructions on the
`Coiled web site <https://docs.coiled.io/user_guide/setup/index.html>`__.

Install the Coiled client Python library in the same environment as MsPASS.

.. code-block::

    conda install --override-channels \
        --channel mspass \
        --channel conda-forge \
        coiled
    coiled login

This will redirect you to the Coiled website to authenticate your computer.
The command stores a token in your local Coiled configuration; do not commit
that token to source control.


2. Connect to your cloud
---------------------------------------------
Next grant Coiled permission to run in your cloud account (AWS, GCP, or Azure).
Coiled creates the IAM policies and network configuration for your account,
asking you for permission at each step.  Run only the command for the cloud
provider you intend to use.

.. code-block::

    coiled setup aws
    coiled setup gcp
    coiled setup azure

You can configure Coiled with a custom network configuration by following the
provider-specific instructions in the `Coiled setup guide
<https://docs.coiled.io/user_guide/setup/index.html>`__.

Cloud resources incur charges while they are running.  Review Coiled's
`cost guidance <https://docs.coiled.io/user_guide/costs/index.html>`__ and
your cloud provider's quotas and pricing before creating a cluster.

3. Get MsPASS Conda package
-------------------------------------------
If you have not run MsPASS before you will need to get the
`Conda package <https://anaconda.org/mspass/mspasspy>`__
from our standard repository.

We strongly advise you create a separate environment for MsPASS
to avoid breaking any existing Python packages you may have
installed previously.  Enter

.. code-block::

    conda create --name mspass

The name ``mspass`` is not special and can be changed if you
prefer something else.  You should then make the new
environment current with the standard Conda command:

.. code-block::

    conda activate mspass

Then install MsPASS in this environment without changing your user-wide
channel configuration:

.. code-block::

    conda install --override-channels \
        --channel mspass \
        --channel conda-forge \
        mspasspy

Return to step 1 to install and authenticate Coiled in this environment.
See :ref:`Install the MsPASS Conda package <deploy_mspass_with_conda>` for
platform availability and installation verification.

4. Run MsPASS
-------------------------
After installing, Coiled's default `package sync
<https://docs.coiled.io/user_guide/software/sync.html>`__ picks up the Conda
and Python packages installed locally and recreates them on your cluster.
It does not sync system packages installed with tools such as ``apt`` or
``brew``, and changing the local environment does not update a running
cluster.  For running a script on one cloud VM, use
``coiled run python your_code.py`` (or follow the `Coiled CLI jobs examples
<https://docs.coiled.io/user_guide/cli-jobs.html>`__).


For example, to connect to MongoDB using Atlas, first create a database user
and allow network access from the Coiled workers as described in the
`Atlas connection prerequisites
<https://www.mongodb.com/docs/atlas/connect-to-database-deployment/>`__.
Prefer a private endpoint or VPC/VNet peering for production, and do not open
Atlas to all IP addresses merely to make this example work.

Store the complete connection string outside the script, for example:

.. code-block::

    export MSPASS_MONGODB_URI='mongodb://<encoded-credentials>@<host-list>/?<options>'

Use a standard ``mongodb://`` connection string, including any required TLS
and replica-set options.  The current MsPASS Dask worker plug-in does not
preserve the ``mongodb+srv://`` URI form when it creates worker-local clients.
Percent-encode reserved characters in credentials, and never commit the URI.
On shared or production systems, load it from a secret manager rather than
leaving it in shell history.

To use Dask:

.. code-block::

    import os
    from coiled import Cluster
    from mspasspy.client import Client

    cluster = Cluster(
        n_workers=2,
        idle_timeout="10 minutes",
        shutdown_on_close=True,
    )
    dask_client = cluster.get_client()
    mspass_client = Client(
        database_host=os.environ["MSPASS_MONGODB_URI"],
        database_name="mspass",
        scheduler="dask",
        dask_client=dask_client,
    )
    db = mspass_client.get_database()
    assert db.client.admin.command("ping")["ok"] == 1

MsPASS registers a database client on each Dask worker.  The ping above tests
only the connection from the machine launching the cluster, so verify worker
network access before starting a large workflow.

Calling ``db.index_mseed_file`` on the launching machine records an absolute
local file path.  Cloud workers cannot read it unless the same storage is
mounted at the same path on every worker.  Use worker-accessible storage or
MongoDB GridFS for remote processing; see :ref:`Input and Output in MsPASS
<io>`.

Once you have a Dask cluster you can then run Python code on that cluster.
Here is a simple code you could run:

.. code-block::

    def inc(x):
        return x + 1

    future = dask_client.submit(inc, 10)
    future.result() # returns 11

Close both handles as soon as the work is finished:

.. code-block::

    dask_client.close()
    cluster.close()

Coiled also has automatic shutdown policies, but explicit cleanup is the
clearest way to avoid paying for an unintentionally running cluster.  See the
`automatic shutdown documentation
<https://docs.coiled.io/user_guide/shutdown.html>`__ for current defaults.

You can find more useful examples in Coiled documentation and reach out to
Coiled team (support@coiled.io) for any usage questions.
