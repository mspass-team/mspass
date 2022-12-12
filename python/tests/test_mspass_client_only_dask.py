from unittest import mock
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    DoubleVector,
)
from mspasspy.ccore.utility import MsPASSError
from mspasspy.global_history.manager import GlobalHistoryManager
from mspasspy.util import logging_helper
from mspasspy.db.client import DBClient

import gridfs
import numpy as np
import obspy
import sys
import re

import pymongo
import pytest

from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")


with mock.patch.dict(sys.modules, {"pyspark": None}):
    from mspasspy.client import Client
    from dask.distributed import Client as DaskClient

    class TestMsPASSClient:
        def setup_class(self):
            self.client = Client()

        def test_get_scheduler(self):
            assert isinstance(self.client.get_scheduler(), DaskClient)
