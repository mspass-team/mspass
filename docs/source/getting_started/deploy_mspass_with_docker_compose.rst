.. _deploy_mspass_with_docker_compose:

Deploy MsPASS with Docker Compose
=================================

Prerequisites
-------------
Docker Compose is a tool to deploy and coordinate multiple Docker containers. 
To install Docker Compose on machines that you have root access, please refer to the guide `here <https://docs.docker.com/compose/install/>`__ and follow the instructions for your specific platform. 
Docker Compose uses YAML files to run multiple containers.
Please refer to its `documentation <https://docs.docker.com/compose/>`__ for more details on using the tool.

Configure MsPASS Containers
---------------------------
The MsPASS runtime environment is composed of multiple components each serves a specific functionality. 
They are: ``frontend``, ``scheduler``, ``worker``, ``db``, ``dbmanager``, and ``shard``.
The MsPASS container can be launched as one of these roles by specifying the ``MSPASS_ROLE`` environment variable.
The options include:

- ``frontend``: Utilizing Jupyter Notebook to provide users an interactive development environment that connects to the other components.
- ``scheduler``: This is the Dask scheduler or the Spark master that coordinates all its corresponding workers.
- ``worker``: This is the Dask worker or Spark worker that does the computation in parallel.
- ``db``: This role runs a standalone MongoDB daemon process that manages data access. 
- ``dbmanager``: This role runs MongoDB's config server and router server to provide access to a sharded MongoDB cluster.
- ``shard``: Each shard contains a subset of the sharded data to provide horizontal scaling of the database.
- ``all``: This role is equivalent to ``frontend`` + ``scheduler`` + ``worker`` + ``db``. 
  Note that this is the default role, and it is how the container is launched in the :ref:`previous section <run_mspass_with_docker>`.

The following environment variables also need be set for the different roles to communicate:

- ``MSPASS_SCHEDULER``: User can use ``dask`` or ``spark`` to run parallel computations. 
  ``dask`` is the default.
- ``MSPASS_SCHEDULER_ADDRESS``: This is the IP address or hostname of the ``scheduler``. 
  ``worker`` and ``frontend`` rely on this to communicate with the ``scheduler``.
- ``MSPASS_DB_ADDRESS``: This is the IP address or hostname of the ``db`` or ``dbmanager``. 
  ``frontend`` rely on this to access the database.
- ``MSPASS_SHARD_LIST``: This is a space delimited string of format ``$HOSTNAME/$HOSTNAME:$MONGODB_PORT`` for all the ``shard``. 
  ``dbmanager`` rely on this to build the sharded database cluster.
- ``MSPASS_SHARD_ID``: This is used to assign each ``shard`` a unique name such that it can write to its own ``data_shard_${MSPASS_SHARD_ID}`` directory under the */db* directory (in case the shards run on a shared filesystem). 
- ``MSPASS_JUPYTER_PWD``: In the ``frontend``, user can optionally set a password for Jupyter Notebook access. 
  If set to an empty string, jupyter can be accessed with no password which may cause security issues.
  If unset, the Jupyter Notebook will generate a random token and print to stdout, which is the default behavior.

User may change the default ports of all the underlying components by setting the following variables:

- ``JUPYTER_PORT``: The default is ``8888``.
- ``DASK_SCHEDULER_PORT``: The default is ``8786``.
- ``SPARK_MASTER_PORT``: The default is ``7077``.
- ``MONGODB_PORT``: The default is ``27017``.

These variables are for experienced users only. The deployment can break if mismatching ports are set. 


Deploy MsPASS Containers
------------------------
Docker Compose can deploy multiple MsPASS Containers of different roles, which simulates a distributed environment.
Below, we provide two exemplary Docker Compose configurations that have distributed setup for both the computation (with Dask or Spark) and the database (with MongoDB).

.. literalinclude:: ../../../docker-compose.yml
   :language: yaml
   :linenos:
   :caption: Docker Compose example that configures a MongoDB cluster of two shards and a Dask cluster of one worker.

.. literalinclude:: ../../../docker-compose_spark.yml
   :language: yaml
   :linenos:
   :caption: Docker Compose example that configures a MongoDB cluster of two shards and a Spark cluster of one worker.

To test out the multi-container setup, we can use the ``docker-compose`` command, which will deploy all the components locally.
First, save the content of one of the two code blocks above to a file called ``docker-compose.yml``, and make sure that you are running in a directory where you want to keep the files created by the containers (i.e., the db, logs, and work directories).
Then, run the following command to start all the containers:

.. code-block:: 

    docker-compose -f docker-compose.yml up -d

This command will start all the containers as services running in the background and you will see all the containers started correctly with outputs like this:

.. code-block:: console

  $ docker-compose -f docker-compose.yml up -d
  Creating network "mspass_default" with the default driver
  Creating mspass_mspass-shard-1_1   ... done
  Creating mspass_mspass-scheduler_1 ... done
  Creating mspass_mspass-shard-0_1   ... done
  Creating mspass_mspass-worker_1    ... done
  Creating mspass_mspass-dbmanager_1 ... done
  Creating mspass_mspass-frontend_1  ... done

You can then open ``http://127.0.0.1:8888/`` in your browser to access the Jupyter Notebook frontend. 
Note that it may take a minute for the frontend to be ready.
You can check the status of the frontend with this command:

.. code-block:: 

  docker-compose -f docker-compose.yml logs mspass-frontend

The notebook will ask for a password for access, just type in ``mspass`` there as you can tell that we have set the ``MSPASS_JUPYTER_PWD`` environment variable for the ``mspass-frontend`` service in the ``docker-compose.yml`` file. 

When you are done with MsPASS, you can bring down the containers with:

.. code-block:: 

    docker-compose -f docker-compose.yml down

You should see similar outputs to the following indicating all the containers are correctly cleaned up:

.. code-block:: console

  $ docker-compose -f docker-compose.yml down
  Stopping mspass_mspass-frontend_1  ... done
  Stopping mspass_mspass-dbmanager_1 ... done
  Stopping mspass_mspass-worker_1    ... done
  Stopping mspass_mspass-shard-0_1   ... done
  Stopping mspass_mspass-scheduler_1 ... done
  Stopping mspass_mspass-shard-1_1   ... done
  Removing mspass_mspass-frontend_1  ... done
  Removing mspass_mspass-dbmanager_1 ... done
  Removing mspass_mspass-worker_1    ... done
  Removing mspass_mspass-shard-0_1   ... done
  Removing mspass_mspass-scheduler_1 ... done
  Removing mspass_mspass-shard-1_1   ... done
  Removing network mspass_default

