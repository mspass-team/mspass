.. _deploy_mspass_on_geolab:

Using MsPASS on EarthScope GeoLab
=================================

This page explains how to use MsPASS in EarthScope GeoLab.

The short version is: open a GeoLab server with the MsPASS image,
``ghcr.io/mspass-team/mspass:geolab``, create a normal MsPASS ``Client``, and
use it as you would in a local notebook.  The image starts the services MsPASS
needs automatically.

Quick start
-----------

Start a GeoLab server with the MsPASS GeoLab image, then open a notebook and
run:

.. code-block:: python

    from mspasspy.client import Client

    mspass_client = Client()

Check that the database and scheduler are available:

.. code-block:: python

    print(mspass_client.get_database_client().admin.command({"ping": 1}))
    print(mspass_client.get_scheduler().scheduler_info())

A healthy setup should show:

* a MongoDB ping result like ``{"ok": 1.0}``;
* a Dask scheduler address like ``tcp://127.0.0.1:8786``;
* four single-threaded local Dask workers by default.

What the GeoLab image starts automatically
------------------------------------------

When the MsPASS GeoLab image starts in JupyterHub, it starts:

* a local MongoDB server;
* one local Dask scheduler;
* four local Dask workers;
* the JupyterHub single-user server.

These services run inside the same GeoLab notebook pod.  Users normally do not
need to start MongoDB or Dask manually.

The default runtime layout is:

.. code-block:: text

    HOME=/home/jovyan
    MSPASS_WORKDIR=/home/jovyan
    MSPASS_DB_DIR=/home/jovyan/db
    MONGO_DATA_DIR=/home/jovyan/db/data
    MSPASS_LOG_DIR=/home/jovyan/logs
    MSPASS_WORKER_DIR=/home/jovyan/work

The default service addresses are:

.. code-block:: text

    MSPASS_DB_ADDRESS=127.0.0.1
    MONGODB_PORT=27017
    MSPASS_SCHEDULER=dask
    MSPASS_SCHEDULER_ADDRESS=127.0.0.1
    DASK_SCHEDULER_PORT=8786

The default worker layout is controlled by:

.. code-block:: bash

    MSPASS_DASK_WORKER_COUNT=4
    MSPASS_DASK_WORKER_THREADS=1
    MSPASS_DASK_WORKER_MEMORY_LIMIT=0

Use multiple single-threaded worker processes on GeoLab to avoid Python GIL
contention.  These values must be set before the JupyterHub single-user server
starts.

This means ``Client()`` connects to the already-running local services.  It
should not create a new hidden Dask ``LocalCluster`` for each notebook.

Check the running services
--------------------------

In a GeoLab terminal, run:

.. code-block:: bash

    ps -ef | egrep 'jupyter|ipykernel|dask|distributed|mongo' | grep -v grep

A healthy server should show processes similar to:

.. code-block:: text

    mongod --port 27017 --dbpath /home/jovyan/db/data ...
    dask scheduler --host 127.0.0.1 --port 8786
    dask worker ... tcp://127.0.0.1:8786
    jupyterhub-singleuser
    ipykernel_launcher

Check MongoDB directly:

.. code-block:: bash

    mongosh --host 127.0.0.1 --port 27017 --eval 'db.adminCommand({ping: 1})'

Expected output:

.. code-block:: text

    { ok: 1 }

Check the main environment variables:

.. code-block:: bash

    echo "MSPASS_WORKDIR=$MSPASS_WORKDIR"
    echo "MSPASS_DB_DIR=$MSPASS_DB_DIR"
    echo "MONGO_DATA_DIR=$MONGO_DATA_DIR"
    echo "MSPASS_LOG_DIR=$MSPASS_LOG_DIR"
    echo "MSPASS_WORKER_DIR=$MSPASS_WORKER_DIR"
    echo "MSPASS_DB_ADDRESS=$MSPASS_DB_ADDRESS"
    echo "MSPASS_SCHEDULER=$MSPASS_SCHEDULER"
    echo "MSPASS_SCHEDULER_ADDRESS=$MSPASS_SCHEDULER_ADDRESS"

Use the default local Dask scheduler
------------------------------------

The recommended default on GeoLab is to use the local Dask scheduler started by
the image:

.. code-block:: python

    from mspasspy.client import Client

    mspass_client = Client()
    scheduler = mspass_client.get_scheduler()

    print(scheduler.scheduler_info())

To confirm that a Dask worker can access the MsPASS database:

.. code-block:: python

    def check_worker_db():
        from dask.distributed import get_worker

        worker = get_worker()
        dbclient = worker.data["dbclient"]
        return dbclient.admin.command({"ping": 1})

    print(scheduler.submit(check_worker_db).result())

Expected output:

.. code-block:: text

    {'ok': 1.0}

This test confirms that the MsPASS Dask worker plugin is installed and that the
local Dask worker can connect to MongoDB.

Database persistence and reset
------------------------------

By default, the MongoDB data directory is:

.. code-block:: text

    /home/jovyan/db/data

This directory is inside the GeoLab user workspace, so it can persist across
server restarts.

To reset the local MongoDB database, stop the GeoLab server and start it again
with:

.. code-block:: bash

    MSPASS_RESET_MONGO_DB=true

Only use this when you intentionally want to remove the selected MongoDB data
directory before startup.  The startup script does not delete the database by
default.

Dask Gateway
------------

EarthScope GeoLab also provides Dask Gateway.  The MsPASS GeoLab image can be
used with Dask Gateway, and Gateway workers can import ``mspasspy``.  However,
Gateway is not the default scheduler path for MsPASS on GeoLab.

The reason is practical: live GeoLab tests showed that Gateway worker pods
cannot connect back to arbitrary ports on the notebook pod.  In particular,
Gateway workers could not connect to the notebook-local MongoDB service.
Therefore, DB-backed MsPASS workflows should use the default local Dask
scheduler unless a worker-reachable MongoDB endpoint is available.

You can still create a Gateway cluster manually:

.. code-block:: python

    from dask_gateway import Gateway

    gateway = Gateway()
    cluster = gateway.new_cluster()
    cluster.scale(1)

    gateway_client = cluster.get_client()
    gateway_client.wait_for_workers(1, timeout="120s")

    print(gateway_client.scheduler_info())

To check that Gateway workers can import the MsPASS runtime:

.. code-block:: python

    def check_worker():
        import os
        import socket
        import sys

        import dask
        import dask_gateway
        import distributed
        import mspasspy

        return {
            "host": socket.gethostname(),
            "python": sys.executable,
            "home": os.environ.get("HOME"),
            "cwd": os.getcwd(),
            "dask": dask.__version__,
            "distributed": distributed.__version__,
            "dask_gateway": dask_gateway.__version__,
            "mspasspy": mspasspy.__file__,
        }

    print(gateway_client.submit(check_worker).result())

Using a Gateway client with MsPASS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MsPASS can accept an externally created Dask client:

.. code-block:: python

    from dask_gateway import Gateway
    from mspasspy.client import Client

    gateway = Gateway()
    cluster = gateway.new_cluster()
    cluster.scale(2)

    gateway_client = cluster.get_client()
    gateway_client.wait_for_workers(2, timeout="120s")

    mspass_client = Client(scheduler="dask", dask_client=gateway_client)

Keep the ``cluster`` object alive while using ``mspass_client``.

This pattern is useful only when the Gateway workers can reach the MongoDB
endpoint used by MsPASS.  The default notebook-local MongoDB address
``127.0.0.1:27017`` is not valid from Gateway worker pods.

For DB-backed MsPASS workflows with Gateway, use a MongoDB endpoint that is
reachable from both the notebook pod and Gateway worker pods.

Disable local Dask
------------------

The local Dask scheduler and worker can be disabled before the JupyterHub
single-user server starts:

.. code-block:: bash

    MSPASS_ENABLE_LOCAL_DASK=false
    MSPASS_SCHEDULER=none

With these settings, the image still starts local MongoDB, but it does not start
a local Dask scheduler or worker.  In that mode, use:

.. code-block:: python

    from mspasspy.client import Client

    mspass_client = Client(scheduler="none")
    print(mspass_client.get_database_client().admin.command({"ping": 1}))
    print(mspass_client.get_scheduler())

Expected scheduler output:

.. code-block:: text

    None

Troubleshooting
---------------

``Client()`` cannot connect to MongoDB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check that MongoDB is running:

.. code-block:: bash

    ps -ef | grep mongod | grep -v grep
    mongosh --host 127.0.0.1 --port 27017 --eval 'db.adminCommand({ping: 1})'
    tail -100 "$MONGO_LOG"

If ``mongod`` is not running, the log file usually explains why startup failed.

``Client()`` creates the wrong Dask scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check the scheduler-related variables:

.. code-block:: bash

    echo "$MSPASS_SCHEDULER"
    echo "$MSPASS_SCHEDULER_ADDRESS"
    echo "$DASK_SCHEDULER_PORT"

The default GeoLab values should be:

.. code-block:: text

    dask
    127.0.0.1
    8786

Then check that the local scheduler exists:

.. code-block:: bash

    ps -ef | grep 'dask scheduler' | grep -v grep

Gateway workers cannot access MongoDB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is expected with the default notebook-local MongoDB service.  Gateway
workers run in separate pods and may not be able to connect to services running
inside the notebook pod.

Use the default local Dask scheduler for DB-backed MsPASS workflows unless your
GeoLab deployment provides a MongoDB endpoint reachable from Gateway workers.

The Dask dashboard shows ``0 B`` memory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The local worker starts with:

.. code-block:: text

    --memory-limit=0

In Dask this means no explicit memory limit is set.  It does not mean the
worker has no memory available.

Notebook-local Python modules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The GeoLab image starts Dask scheduler and worker processes before the
notebook kernel starts.  Therefore workers do not automatically inherit
per-notebook ``sys.path`` changes or helper modules that are only importable
from the notebook's current directory.

For durable code, package the helper module and install it into the image.
For prototype notebook-local code, upload it to the active Dask workers before
submitting tasks:

.. code-block:: python

    from pathlib import Path

    dask_client = mspass_client.get_scheduler()
    dask_client.upload_file(str(Path("s3_client_plugin.py").resolve()))
