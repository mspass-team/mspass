import os

import gridfs
import mongomock
from mongomock.gridfs import enable_gridfs_integration
import numpy as np
import pymongo
import pytest
import sys

from mspasspy.ccore.seismic import Seismogram, TimeSeries
from mspasspy.ccore.utility import dmatrix, ErrorSeverity, MetadataDefinitions, Metadata
from mspasspy.util.seispp import index_data
from bson.objectid import ObjectId
from datetime import datetime

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
        self.metadata_def = MetadataDefinitions()

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
        assert 'elog_ids' not in tmp_ts

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

        tmp_ts = get_live_timeseries()
        tmp_ts['dir'] = 'python/tests/data/'
        tmp_ts['dfile'] = 'test_db_output'
        self.db._save_data_to_dfile(tmp_ts)
        tmp_ts_2 = TimeSeries()
        tmp_ts_2.npts = 255
        tmp_ts_2['dir'] = tmp_ts['dir']
        tmp_ts_2['dfile'] = tmp_ts['dfile']
        tmp_ts_2['foff'] = tmp_ts['foff']
        self.db._read_data_from_dfile(tmp_ts_2)
        assert all(a == b for a, b in zip(tmp_ts.data, tmp_ts_2.data))

        tmp_ts.clear('dir')
        with pytest.raises(KeyError) as err:
            self.db._read_data_from_dfile(tmp_ts)
            assert err == KeyError("one of the keys: dir, dfile, foff is missing")
        with pytest.raises(KeyError) as err:
            self.db._save_data_to_dfile(tmp_ts)
            assert err == KeyError("one of dir, dfile is missing")

    def test_save_and_read_gridfs(self):
        tmp_seis = get_live_seismogram()
        self.db._save_data_to_gridfs(tmp_seis)
        tmp_seis_2 = Seismogram()
        tmp_seis_2.npts = 255
        tmp_seis_2['gridfs_id'] = tmp_seis['gridfs_id']
        self.db._read_data_from_gridfs(tmp_seis_2)
        assert all(a.any() == b.any() for a, b in zip(tmp_seis.data, tmp_seis_2.data))

        with pytest.raises(KeyError) as err:
            tmp_seis_2.clear('npts')
            self.db._read_data_from_gridfs(tmp_seis_2)
            assert err == KeyError("npts is not defined")

        with pytest.raises(ValueError) as err:
            tmp_seis_2.npts = 254
            self.db._read_data_from_gridfs(tmp_seis_2)
            assert str(err.value) == "ValueError: Size mismatch in sample data. " \
                                     "Number of points in gridfs file = 765 but expected 762"

        tmp_ts = get_live_timeseries()
        self.db._save_data_to_gridfs(tmp_ts)
        tmp_ts_2 = TimeSeries()
        tmp_ts_2.npts = 255
        tmp_ts_2['gridfs_id'] = tmp_ts['gridfs_id']
        self.db._read_data_from_gridfs(tmp_ts_2)
        assert all(a == b for a, b in zip(tmp_ts.data, tmp_ts_2.data))

        id = ObjectId(tmp_ts['gridfs_id'])
        assert id
        gfsh = gridfs.GridFS(self.db)
        assert gfsh.exists(id)
        self.db._save_data_to_gridfs(tmp_ts)
        assert not gfsh.exists(id)

        with pytest.raises(KeyError) as err:
            tmp_ts_2.clear('gridfs_id')
            self.db._read_data_from_gridfs(tmp_ts_2)
            assert err == KeyError("gridfs_id is not defined")

    def test_mspass_type_helper(self):
        assert type([1.1]) == self.db._mspass_type_helper(self.metadata_def.type('tmatrix'))
        assert type(1) == self.db._mspass_type_helper(self.metadata_def.type('npts'))
        assert type(1.1) == self.db._mspass_type_helper(self.metadata_def.type('delta'))
        assert type('1') == self.db._mspass_type_helper(self.metadata_def.type('site_id'))

    def test_update_metadata(self):
        ts = get_live_timeseries()
        ts['test'] = ' ' # empty key
        ts['extra1'] = 'extra1'
        ts['extra2'] = 'extra2' # exclude
        ts.clear('starttime')
        ts['t0'] = datetime.utcnow().timestamp()
        exclude = ['extra2']
        self.db.update_metadata(ts, update_all=True, exclude=exclude)
        res = self.db['wf'].find_one({'_id': ObjectId(ts['wf_id'])})
        assert res['starttime'] == ts['t0']
        assert 'test' not in res
        assert 'extra2' not in res
        assert res['extra1'] == 'extra1'
        assert 'net' not in res
        assert 'wf_id' in ts

        ts['extra1'] = 'extra1+'
        self.db.update_metadata(ts, update_all=True, exclude=exclude)
        res = self.db['wf'].find_one({'_id': ObjectId(ts['wf_id'])})
        assert res['extra1'] == 'extra1+'

        with pytest.raises(TypeError) as err:
            ts.put_string('npts', '121')
            self.db.update_metadata(ts)
        assert str(err.value) == "npts has type <class 'str'>, forbidden by definition"

    def test_save_read_data(self):
        pass

    def teardown_class(self):
        os.remove('python/tests/data/test_db_output')

if __name__ == '__main__':
    pass
    # ts = get_live_timeseries()
    # ts['t0'] = datetime.utcnow()
    # ts.clear('starttime')
    # me = Metadata(ts)
    # print(me)
    # meta = MetadataDefinitions()
    # meta.clear_aliases(me)
    # print(me)
