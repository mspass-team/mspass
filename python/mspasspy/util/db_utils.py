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
        worker = get_worker()
        dbclient = worker.data["dbclient"]
        db = dbclient.get_database(dbname_or_handle)
    elif isinstance(dbname_or_handle, Database):
        db = dbname_or_handle
    else:
        message = "fetch_dbhandle: illegal type for arg1={}\n".format(type(dbname_or_handle))
        message += "Must be str defining a db name or a Database object"
        raise ValueError(message)
    return db


class MongoDBWorker(WorkerPlugin):
    """
    Dask worker plugin to manage MongoDB client per worker.

    Creates a DBClient instance for each worker and stores it in worker.data.
    This ensures each worker has its own database connection to prevent
    connection leaks when Database objects are serialized to workers.
    """

    def __init__(self, dbname, url="mongodb://localhost:27017/", dbclient_key="dbclient"):
        self.dbname = dbname
        self.connection_url = url
        self.dbclient_key = dbclient_key

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

