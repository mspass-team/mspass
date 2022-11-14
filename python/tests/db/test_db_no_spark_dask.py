import sys
import pytest
from unittest import mock
with mock.patch.dict(sys.modules, {'pyspark': None, 'dask': None, 'dask.dataframe': None}):
    from mspasspy.db.database import Database, read_distributed_data
    from mspasspy.db.client import DBClient

    def test_read_distributed_data():
        client = DBClient("localhost")
        client.drop_database("mspasspy_test_db")
        db = Database(client, "mspasspy_test_db")
        cursors = db["wf_TimeSeries"].find({})
        with pytest.raises(
            TypeError,
            match="Only spark and dask are supported",
        ):
            dask_list = read_distributed_data(
                db,
                cursors,
                mode="cautious",
                normalize=["source", "site", "channel"],
            )
