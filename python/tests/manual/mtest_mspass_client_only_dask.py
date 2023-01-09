from unittest import mock
from dask.distributed import Client as DaskClient

import sys


sys.path.append("python/tests")


with mock.patch.dict(sys.modules, {"pyspark": None}):
    from mspasspy.client import Client

    class TestMsPASSClient2:
        def setup_class(self):
            self.client = Client()

        def test_get_scheduler(self):
            assert isinstance(self.client.get_scheduler(), DaskClient)
