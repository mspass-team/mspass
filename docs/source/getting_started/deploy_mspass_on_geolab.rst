.. _deploy_mspass_on_geolab:

Using MsPASS With EarthScope GeoLab
===================================

The MsPASS GeoLab image is built from the same GeoLab/Pangeo-style base image
contract as the official EarthScope image, with MsPASS installed into the
``/srv/conda/envs/notebook`` environment.  Non-JupyterHub commands pass through
the entrypoint directly so EarthScope Dask Gateway can still start scheduler
and worker pods from the image.

Live GeoLab network tests show that Gateway worker pods cannot reach arbitrary
ports on the notebook pod, including a notebook-local MongoDB service.  For
that reason, the default MsPASS GeoLab startup path uses notebook-local MongoDB
and notebook-local Dask in the same pod.  Dask Gateway remains available for
manual use, pure Dask workflows, and future DB-backed workflows where MongoDB
is reachable from Gateway workers.

Default notebook runtime
------------------------

When the JupyterHub single-user server starts, the GeoLab image starts local
MongoDB plus one local Dask scheduler and worker.  The scheduler binds to
``127.0.0.1`` on ``DASK_SCHEDULER_PORT`` and notebook kernels default to:

.. code-block:: bash

    HOME=/home/jovyan
    NB_HOME=/home/jovyan
    MSPASS_WORK_DIR=/home/jovyan
    MSPASS_WORKDIR=/home/jovyan
    MSPASS_DB_DIR=/home/jovyan/db
    MSPASS_LOG_DIR=/home/jovyan/logs
    MSPASS_WORKER_DIR=/home/jovyan/work
    MONGO_DATA_DIR=/home/jovyan/db/data
    MONGO_LOG=/home/jovyan/logs/mongo_log
    MSPASS_DB_ADDRESS=127.0.0.1
    MSPASS_SCHEDULER=dask
    MSPASS_SCHEDULER_ADDRESS=127.0.0.1

This means the usual notebook client constructor attaches to the pre-started
local scheduler instead of creating a hidden ``LocalCluster``:

.. code-block:: python

    from mspasspy.client import Client

    mspass_client = Client(scheduler="dask")
    print(mspass_client.get_database_client().admin.command({"ping": 1}))
    print(mspass_client.get_scheduler().scheduler_info())

The MongoDB worker plugin should also be able to reach the local MongoDB
service from the local Dask worker:

.. code-block:: python

    scheduler = mspass_client.get_scheduler()

    def check_worker_db():
        from dask.distributed import get_worker

        worker = get_worker()
        dbclient = worker.data["dbclient"]
        return dbclient.admin.command({"ping": 1})

    print(scheduler.submit(check_worker_db).result())

Resetting MongoDB data is always explicit.  Set
``MSPASS_RESET_MONGO_DB=true`` only when you intentionally want
``MONGO_DATA_DIR`` removed before startup.  The startup script does not delete
the database directory by default.

Disabling local Dask
--------------------

Local in-pod Dask can be disabled before the JupyterHub single-user server
starts:

.. code-block:: bash

    MSPASS_ENABLE_LOCAL_DASK=false
    MSPASS_SCHEDULER=none

With those settings the startup script still starts notebook-local MongoDB, but
does not start a local Dask scheduler or worker.  ``Client(scheduler="none")``
or ``MSPASS_SCHEDULER=none`` leaves the MsPASS scheduler unset.  Setting
``MSPASS_ENABLE_LOCAL_DASK=false`` by itself also changes the default local
``dask`` scheduler setting to ``none`` unless an external scheduler address is
configured.

Dask Gateway
------------

EarthScope GeoLab provides distributed Dask through Dask Gateway.  Installing
``dask-gateway`` manually inside a running notebook pod is not sufficient
because Gateway-created scheduler and worker pods are started from the image,
not from the notebook pod's mutated runtime filesystem.  The MsPASS GeoLab
image keeps ``dask``, ``distributed``, ``dask_gateway``, and ``mspasspy``
importable in notebook, scheduler, and worker pods.

A Gateway cluster starts with no workers until it is scaled or configured for
adaptive scaling:

.. code-block:: python

    from dask_gateway import Gateway

    gateway = Gateway()
    cluster = gateway.new_cluster()
    cluster.scale(1)

    dask_client = cluster.get_client()
    dask_client.wait_for_workers(1, timeout="120s")
    print(dask_client.scheduler_info())

To verify that a Gateway worker was created from an image containing the MsPASS
runtime packages:

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

    print(dask_client.submit(check_worker).result())

MsPASS can accept an externally-created Dask client, including one returned by
Dask Gateway:

.. code-block:: python

    from dask_gateway import Gateway
    from mspasspy.client import Client

    gateway = Gateway()
    cluster = gateway.new_cluster()
    cluster.scale(2)

    dask_client = cluster.get_client()
    dask_client.wait_for_workers(2, timeout="120s")

    mspass_client = Client(scheduler="dask", dask_client=dask_client)

Keep the ``cluster`` object alive while using ``mspass_client``.  MsPASS uses
the provided Dask client but does not own or shut down the Gateway cluster.

DB-backed MsPASS workflows through Gateway need a MongoDB endpoint that Gateway
workers can reach.  Passing a Gateway client to ``Client`` still registers the
MongoDB worker plugin, so notebook-local ``127.0.0.1`` MongoDB is not a valid
Gateway worker database address.

Rebuilt image smoke checks
--------------------------

In a fresh GeoLab server using the rebuilt MsPASS GeoLab image, verify the
workspace and runtime environment:

.. code-block:: bash

    whoami
    pwd
    echo $HOME
    echo $NB_HOME
    echo $MSPASS_WORK_DIR
    echo $MSPASS_WORKDIR
    echo $MSPASS_DB_ADDRESS
    echo $MSPASS_SCHEDULER
    echo $MSPASS_SCHEDULER_ADDRESS
    echo $MSPASS_DB_DIR
    echo $MONGO_DATA_DIR
    echo $MSPASS_LOG_DIR
    echo $MSPASS_WORKER_DIR
    mongosh --host 127.0.0.1 --port 27017 --eval 'db.adminCommand({ping: 1})'
    tail -50 "$MONGO_LOG"

Expected values are ``/home/jovyan`` for the workspace and home variables,
``127.0.0.1`` for local MongoDB and local Dask addresses, and runtime
directories under ``/home/jovyan``.  The expected notebook processes are
``mongod``, ``dask scheduler``, ``dask worker``, ``jupyterhub-singleuser``, and
notebook kernel processes.

To verify installed package versions:

.. code-block:: python

    import dask
    import distributed
    import dask_gateway
    import mspasspy

    print(dask.__version__)
    print(distributed.__version__)
    print(dask_gateway.__version__)
    print(mspasspy.__file__)
