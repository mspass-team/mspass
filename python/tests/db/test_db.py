import mongomock
from mongomock.gridfs import enable_gridfs_integration
import numpy as np
import pymongo
import pytest
import sys

from mspasspy.ccore.seismic import Seismogram, TimeSeries
from mspasspy.ccore.utility import dmatrix, ErrorSeverity
from mspasspy.util.seispp import index_data
from bson.objectid import ObjectId

sys.path.append("python/tests")
sys.path.append("python/mspasspy/db/")
from database import Database
from client import Client
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble)

class TestClient():
    
    def setup_class(self):
        self.c1 = Client('mongodb://localhost/my_database')
        self.c2 = Client('localhost')

    def test_init(self):
        assert self.c1._Client__default_database_name == 'my_database'

    def test_getitem(self):
        assert self.c1['my_database'].name == 'my_database'
        assert self.c2['my_db'].name == 'my_db'

    def test_get_default_database(self):
        assert self.c1.get_default_database().name == 'my_database'
        with pytest.raises(pymongo.errors.ConfigurationError, match = 'No default database'):
            self.c2.get_default_database()

    def test_get_database(self):
        assert self.c1.get_database().name == 'my_database'
        assert self.c2.get_database('my_db').name == 'my_db'
        with pytest.raises(pymongo.errors.ConfigurationError, match = 'No default database'):
            self.c2.get_database()

class TestDatabase():

    def setup_class(self):
        enable_gridfs_integration()
        client = mongomock.MongoClient('localhost')
        Database.__bases__ = (mongomock.database.Database,)
        self.db = Database(client, 'dbtest', codec_options=client._codec_options, _store = client._store['dbtest'])
        # index_data("python/tests/data/sample", self.db)

        npts = 255
        sampling_rate = 20.0
        s1 = Seismogram()
        s1.data = dmatrix(3, npts)
        for i in range(3):
            for j in range(npts):
                s1.data[i, j] = np.random.rand()
        s1.live = True
        s1.dt = 1/sampling_rate
        s1.t0 = 0
        s1.npts = npts
        s1.put('net', 'IU')

        s2 = Seismogram(s1)
        s2.put('net', 'AK')

        di = 'python/tests/tmp'
        dfile = 'tmp_db_Database.test'
        s1_file = Seismogram(s1)
        s1_file.put('dir', di)
        s1_file.put('dfile', dfile)

        self.npts = npts
        self.sampling_rate = sampling_rate
        self.dir = di
        self.dfile = dfile
        self.s1 = s1
        self.s1_file = s1_file
        self.s2 = s2

    def test_save_elogs(self):
        tmp_ts = get_live_timeseries()
        tmp_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        tmp_ts.elog.log_error("alg2", str("message2"), ErrorSeverity.Informational) # multi elogs fix me
        errs = tmp_ts.elog.get_error_log()
        self.db._save_elog(tmp_ts)
        assert len(tmp_ts['elog_ids']) != 0
        for err, id in zip(errs, tmp_ts['elog_ids']):
            res = self.db['error_logs'].find_one({'_id': ObjectId(id)})
            assert res['algorithm'] == err.algorithm
            assert res['error_message'] == err.message

        # empty logs
        tmp_ts = get_live_timeseries()
        errs = tmp_ts.elog.get_error_log()
        self.db._save_elog(tmp_ts)
        assert len(tmp_ts['elog_ids']) == 0

    def test_save_and_read_data(self):
        tmp_seis = get_live_seismogram()
        tmp_seis['dir'] = 'python/tests/data/'
        tmp_seis['dfile'] = 'test_db_output'
        self.db._save_data_to_dfile(tmp_seis)
        tmp_seis_2 = Seismogram()
        tmp_seis_2.npts = 255
        tmp_seis_2['dir'] = tmp_seis['dir']
        tmp_seis_2['dfile'] = tmp_seis['dfile']
        tmp_seis_2['foff'] = tmp_seis['foff']
        self.db._read_data_from_dfile(tmp_seis_2)
        assert all(a.any() == b.any() for a,b in zip(tmp_seis.data, tmp_seis_2.data))

    def test_save_and_read_gridfs(self):
        tmp_seis = get_live_seismogram()
        self.db._save_data_to_gridfs(tmp_seis)
        tmp_seis_2 = Seismogram()
        tmp_seis_2.npts = 255
        tmp_seis_2['gridfs_id'] = tmp_seis['gridfs_id']
        self.db._read_data_from_gridfs(tmp_seis_2)
        assert all(a.any() == b.any() for a, b in zip(tmp_seis.data, tmp_seis_2.data))

    def tearup(self):
        pass

if __name__ == '__main__':
    pass
