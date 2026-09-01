"""Internal guards for MongoDB handles embedded in Dask tasks."""

import sys


def is_dask_serializing():
    """Return True while Dask Distributed is pickling a task payload."""
    frame = sys._getframe(1)
    while frame is not None:
        if frame.f_globals.get("__name__") == "distributed.protocol.pickle":
            return True
        frame = frame.f_back
    return False


def reject_dask_serialization(value):
    """Reject live MongoDB handles before Dask can recreate their clients."""
    if is_dask_serializing():
        raise TypeError(
            f"{type(value).__name__} objects cannot be serialized into Dask tasks. "
            "Register MongoDBWorker, pass the database name, and call "
            "fetch_dbhandle inside the worker instead."
        )
