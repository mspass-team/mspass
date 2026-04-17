"""
Database utility functions for parallel processing support.

This module provides utilities for handling database connections in both
serial and parallel (Dask worker) contexts.
"""

from dask.distributed import WorkerPlugin, get_worker

from mspasspy.db.database import Database
from mspasspy.db.client import DBClient


def fetch_dbhandle(dbname_or_handle):
    """
    Fetch a Database handle from dbname_or_handle.

    This function provides a unified way to get a Database instance.  In parallel mode
    (Dask worker), it fetches the DBClient from the worker plugin to avoid serializing
    Database/DBClient objects.  In serial mode, it returns the provided Database instance.

    :param dbname_or_handle: Either a string (database name) or a Database instance.
        If a string is provided, the function assumes it's being called in a Dask worker
        context and will get the DBClient from worker.data["dbclient"].
    :type dbname_or_handle: str or :class:`mspasspy.db.database.Database`
    :return: :class:`mspasspy.db.database.Database` instance
    :raises: ValueError if dbname_or_handle is neither a string nor a Database instance,
        or if called with a string outside of a Dask worker context.
    """

    if isinstance(dbname_or_handle, str):
        try:
            worker = get_worker()
        except Exception as e:
            raise ValueError(
                "fetch_dbhandle: when called with a string argument, this function must be "
                "executed within a Dask worker context so that get_worker() succeeds and a "
                "DBClient is available via a worker plugin."
            ) from e
        try:
            dbclient = worker.data["dbclient"]
        except KeyError as e:
            raise ValueError(
                "fetch_dbhandle: Dask worker does not contain a 'dbclient' entry in "
                "worker.data. Make sure the MongoDBWorker (or an equivalent WorkerPlugin) "
                "is registered so that a DBClient is created per worker."
            ) from e
        db = dbclient.get_database(dbname_or_handle)
    elif isinstance(dbname_or_handle, Database):
        db = dbname_or_handle
    else:
        message = "fetch_dbhandle: illegal type for arg1={}\n".format(
            type(dbname_or_handle)
        )
        message += "Must be str defining a db name or a Database object"
        raise ValueError(message)
    return db


class MongoDBWorker(WorkerPlugin):
    """
    Dask worker plugin to manage MongoDB client per worker.

    Creates a DBClient instance for each worker and stores it in worker.data.
    This ensures each worker has its own database connection to prevent
    connection leaks when Database objects are serialized to workers.

    :param mspass_client: MSPASS :class:`~mspasspy.client.Client` (or any object
        with ``get_database_client()`` returning a :class:`~mspasspy.db.client.DBClient`).
        The worker uses that client's ``_mspass_db_host`` for per-worker connections.
    :param dbclient_key: Key under ``worker.data`` for the created :class:`~mspasspy.db.client.DBClient`.
    """

    def __init__(self, mspass_client, dbclient_key="dbclient"):
        self.dbclient_key = dbclient_key
        db_client = mspass_client.get_database_client()
        url = getattr(db_client, "_mspass_db_host", None)
        if url is None:
            url = "mongodb://localhost:27017/"
        elif isinstance(url, str) and not url.startswith("mongodb://"):
            url = "mongodb://{}".format(url)
        self.connection_url = url

    def __getstate__(self):
        return {
            "dbclient_key": self.dbclient_key,
            "connection_url": self.connection_url,
        }

    def __setstate__(self, state):
        self.dbclient_key = state["dbclient_key"]
        self.connection_url = state["connection_url"]

    def setup(self, worker):
        """Called when worker starts - create DBClient for this worker."""
        if self.connection_url is None:
            dbclient = DBClient()
        else:
            dbclient = DBClient(self.connection_url)
        worker.data[self.dbclient_key] = dbclient

    def teardown(self, worker):
        """Called when worker shuts down - cleanup if needed."""
        dbclient = worker.data.get(self.dbclient_key)
        if dbclient:
            dbclient.close()
