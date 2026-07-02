.. _deploy_mspass_on_geolab:

Using MsPASS With EarthScope GeoLab
===================================

EarthScope GeoLab provides distributed Dask through Dask Gateway.  The MsPASS
GeoLab image is built from the same GeoLab/Pangeo-style base image contract as
the official EarthScope image, with MsPASS installed into the
``/srv/conda/envs/notebook`` environment.  This keeps the notebook, scheduler,
and worker pods on the same Dask Gateway runtime while adding the MsPASS Python
and compiled extension packages.

Gateway-created scheduler and worker pods run arbitrary commands supplied by
the Gateway server.  The MsPASS GeoLab image therefore uses a pass-through
entrypoint for non-JupyterHub commands, matching the official GeoLab startup
style.  MsPASS-specific initialization, including workspace directories and the
notebook-local MongoDB service, runs only for ``jupyterhub-singleuser`` notebook
pods.

Installing ``dask-gateway`` manually inside a running notebook pod is not
sufficient because Gateway-created scheduler and worker pods are started from
the image, not from the notebook pod's mutated runtime filesystem.

A Gateway cluster starts with no workers until it is scaled or configured for
adaptive scaling.  A typical MsPASS setup is:

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

Rebuilt image smoke checks
--------------------------

In a fresh GeoLab server using the MsPASS GeoLab image, first verify Gateway can
create a cluster:

.. code-block:: python

    from dask_gateway import Gateway

    gateway = Gateway()
    cluster = gateway.new_cluster()
    cluster.scale(1)

    dask_client = cluster.get_client()
    dask_client.wait_for_workers(1, timeout="120s")
    print(dask_client.scheduler_info())

Then verify that the Gateway worker pod was created from an image containing
the MsPASS and Gateway runtime packages:

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

If ``gateway.new_cluster()`` still fails after rebuilding the image, the next
diagnostic is the EarthScope/2i2c Dask Gateway server-side logs for the failed
cluster name.

Finally verify that MsPASS can use the Gateway client:

.. code-block:: python

    from mspasspy.client import Client

    mspass_client = Client(scheduler="dask", dask_client=dask_client)
    print(mspass_client.get_scheduler().scheduler_info())

The notebook pod should use ``/home/jovyan`` as the workspace:

.. code-block:: bash

    whoami
    pwd
    echo $HOME
    echo $NB_HOME
    echo $MSPASS_WORKDIR
    echo $MSPASS_DB_ADDRESS
    echo $MSPASS_DB_DIR
    echo $MSPASS_LOG_DIR
    echo $MSPASS_WORKER_DIR
    mongosh --host 127.0.0.1 --port 27017 --eval 'db.adminCommand({ping: 1})'
    tail -50 "$MSPASS_LOG_DIR/mongo_log"

The expected workspace and home directory is ``/home/jovyan``.  The MsPASS
runtime directories should keep notebook logs and work files under
``/home/jovyan/logs`` and ``/home/jovyan/work``.  MongoDB data defaults to an
ephemeral notebook-local directory, ``/tmp/mspass-mongo/db/data``, to avoid
persisted database version conflicts, stale locks, and NFS storage issues.
``MSPASS_DB_ADDRESS`` is computed from the pod address so Dask Gateway workers
do not receive a localhost MongoDB URL.

The notebook-local MongoDB process should be active, not defunct, and the
``mongosh`` ping should succeed.  To verify MsPASS can use MongoDB without a
scheduler:

.. code-block:: python

    from mspasspy.client import Client

    mspass_client = Client(scheduler="none")
    print(mspass_client.get_database_client().admin.command({"ping": 1}))
    print(mspass_client.get_scheduler())

To verify that the MongoDB worker plugin can initialize on Gateway workers:

.. code-block:: python

    from dask_gateway import Gateway
    from mspasspy.client import Client

    gateway = Gateway()
    cluster = gateway.new_cluster()
    cluster.scale(1)

    dask_client = cluster.get_client()
    dask_client.wait_for_workers(1, timeout="120s")

    mspass_client = Client(scheduler="dask", dask_client=dask_client)

    def check_worker_db():
        from dask.distributed import get_worker

        worker = get_worker()
        dbclient = worker.data["dbclient"]
        return dbclient.admin.command({"ping": 1})

    print(dask_client.submit(check_worker_db).result())

Persistent MongoDB data under ``/home/jovyan`` is opt-in.  Set
``MSPASS_PERSISTENT_MONGO=true`` or ``MSPASS_DB_PATH=home`` before the
JupyterHub single-user server starts to use ``/home/jovyan/db``.  Persistent
MongoDB data can break across MongoDB major-version upgrades.  The GeoLab image
keeps the same MongoDB version as the other MsPASS container tags, but data
written by a different MongoDB major version may still require an explicit
MongoDB upgrade or downgrade procedure.  The startup script never deletes
persistent data by default.  Set ``MSPASS_RESET_MONGO_DB=true`` only when you
intentionally want the selected MongoDB data directory removed before startup.

GeoLab local Dask fallback
--------------------------

The GeoLab image defaults to ``MSPASS_SCHEDULER=none`` so that
``Client()`` does not silently create an in-pod Dask ``LocalCluster``.  This is
separate from Dask Gateway, which is the normal distributed Dask path on
GeoLab.  Dask Gateway scheduler and worker commands pass directly through the
image entrypoint and do not run MsPASS startup or local MongoDB startup.

For fallback or debugging only, local in-pod Dask can be enabled before the
JupyterHub single-user server starts:

.. code-block:: bash

    MSPASS_ENABLE_LOCAL_DASK=true

When this flag is set in the GeoLab/JupyterHub command path, the startup script
starts the local Dask scheduler and worker and exports ``MSPASS_SCHEDULER=dask``
for notebook kernels.
