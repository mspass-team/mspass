import copy
import os
import pickle

import gridfs
import mongomock
from mongomock.gridfs import enable_gridfs_integration
import numpy as np
import pymongo
import pytest
import sys

from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble
from mspasspy.ccore.utility import dmatrix, ErrorSeverity, Metadata

from mspasspy.db.schema import MetadataSchema
from mspasspy.util import logging_helper
from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")
sys.path.append("python/mspasspy/db/")
from database import Database, read_distributed_data
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
        self.metadata_def = MetadataSchema()

        self.test_ts = get_live_timeseries()
        self.test_ts['test'] = ' '  # empty key
        self.test_ts['extra1'] = 'extra1'
        self.test_ts['extra2'] = 'extra2'  # exclude
        self.test_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_ts.clear('starttime')
        self.test_ts['t0'] = datetime.utcnow().timestamp()

        self.test_seis = get_live_seismogram()
        self.test_seis['test'] = ' '  # empty key
        self.test_seis['extra1'] = 'extra1'
        self.test_seis['extra2'] = 'extra2'  # exclude
        self.test_seis.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_seis.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_seis.clear('starttime')
        self.test_seis['t0'] = datetime.utcnow().timestamp()
        self.test_seis['tmatrix'] = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]

        site_id = ObjectId()
        channel_id = ObjectId()
        source_id = ObjectId()
        self.db['site'].insert_one({'_id': site_id, 'net': 'net', 'sta': 'sta', 'loc': 'loc', 'lat': 1.0, 'lon': 1.0,
                                    'elev': 2.0, 'starttime': datetime.utcnow().timestamp(),
                                    'endtime': datetime.utcnow().timestamp()})
        self.db['channel'].insert_one({'_id': channel_id, 'net': 'net1', 'sta': 'sta1', 'loc': 'loc1', 'chan': 'chan',
                                       'lat': 1.1, 'lon': 1.1, 'elev': 2.1, 'starttime': datetime.utcnow().timestamp(),
                                    'endtime': datetime.utcnow().timestamp(), 'edepth': 3.0, 'vang': 1.0, 'hang': 1.0})
        self.db['source'].insert_one({'_id': source_id, 'lat': 1.2, 'lon': 1.2, 'time': datetime.utcnow().timestamp(),
                                      'depth': 3.1, 'magnitude': 1.0})

        self.test_seis['site_id'] = site_id
        self.test_seis['source_id'] = source_id
        self.test_ts['site_id'] = site_id
        self.test_ts['source_id'] = source_id
        self.test_ts['channel_id'] = channel_id


    def test_save_elogs(self):
        tmp_ts = get_live_timeseries()
        tmp_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        tmp_ts.elog.log_error("alg2", str("message2"), ErrorSeverity.Informational) # multi elogs fix me
        errs = tmp_ts.elog.get_error_log()
        self.db._save_elog(tmp_ts)
        assert len(tmp_ts['elog_id']) != 0
        for err, id in zip(errs, tmp_ts['elog_id']):
            res = self.db['error_logs'].find_one({'_id': id})
            assert res['algorithm'] == err.algorithm
            assert res['error_message'] == err.message
            assert "wf_TImeSeries_id" not in res

        # empty logs
        tmp_ts = get_live_timeseries()
        errs = tmp_ts.elog.get_error_log()
        self.db._save_elog(tmp_ts)
        assert len(tmp_ts['elog_id']) == 0

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

        id = tmp_ts['gridfs_id']
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
        schema = self.metadata_def.Seismogram
        assert type([1.0, 1.2]) == schema.type('tmatrix')
        assert type(1) == schema.type('npts')
        assert type(1.1) == schema.type('delta')

    def test_save_load_history(self):
        ts = get_live_timeseries()
        logging_helper.info(ts, 'dummy_func', '1')
        logging_helper.info(ts, 'dummy_func_2', '2')
        nodes = ts.get_nodes()
        assert ts.number_of_stages() == 2
        self.db._save_history(ts)
        res = self.db['history_object'].find_one({'_id': ts['history_object_id']})
        assert res

        ts_2 = TimeSeries()
        ts_2['history_object_id'] = ts['history_object_id']
        self.db._load_history(ts_2)
        loaded_nodes = ts_2.get_nodes()
        assert str(nodes) == str(loaded_nodes)

        with pytest.raises(KeyError) as err:
            ts_2.clear('history_object_id')
            self.db._load_history(ts_2)
            assert err == KeyError("history_object_id not found")

    def test_update_metadata(self):
        ts = copy.deepcopy(self.test_ts)
        exclude = ['extra2']
        self.db.update_metadata(ts, update_all=True, exclude=exclude)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res
        assert res['starttime'] == ts['t0']
        assert 'test' not in res
        assert 'extra2' not in res
        assert res['extra1'] == 'extra1'
        assert 'net' not in res
        assert '_id' in ts
        assert 'history_object_id' in ts
        res = self.db['history_object'].find_one({'_id': ts['history_object_id']})
        assert res
        assert ts['elog_id']
        for id in ts['elog_id']:
            res = self.db['error_logs'].find_one({'_id': id})
            assert res['wf_TimeSeries_id'] == ts['_id']

        ts['extra1'] = 'extra1+'
        self.db.update_metadata(ts, update_all=True, exclude=exclude)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res['extra1'] == 'extra1+'

        with pytest.raises(TypeError) as err:
            ts.put_string('npts', '121')
            self.db.update_metadata(ts)
        assert str(err.value) == "npts has type <class 'str'>, forbidden by definition"

    def test_save_read_data(self):
        # new object
        # read data
        with pytest.raises(KeyError) as err:
            seis2 = self.db.read_data(ObjectId(), 'wrong')
            assert err == KeyError("only wf_TimeSeries and wf_Seismogram are supported")

        seis2 = self.db.read_data(ObjectId(), 'wf_Seismogram')
        assert not seis2

        seis = copy.deepcopy(self.test_seis)
        self.db.save_data(seis, storage_mode='gridfs', update_all=True, exclude=['extra2'])
        seis2 = self.db.read_data(seis['_id'], 'wf_Seismogram')

        wf_keys = ['_id', 'npts', 'delta', 'sampling_rate', 'calib', 'starttime', 'dtype', 'site_id', 'channel_id',
                   'source_id', 'storage_mode', 'dir', 'dfile', 'foff', 'gridfs_id', 'url', 'elog_id', 'history_object_id',
                   'time_standard', 'tmatrix']
        for key in wf_keys:
            if key in seis:
                assert seis[key] == seis2[key]
        assert 'test' not in seis2
        assert 'extra2' not in seis2
        assert seis['channel_id'] == seis2['channel_id']

        res = self.db['site'].find_one({'_id': seis['site_id']})
        assert seis2['site_lat'] == res['lat']
        assert seis2['site_lon'] == res['lon']
        assert seis2['site_elev'] == res['elev']
        assert seis2['site_starttime'] == res['starttime']
        assert seis2['site_endtime'] == res['endtime']
        assert seis2['net'] == res['net']
        assert seis2['sta'] == res['sta']
        assert seis2['loc'] == res['loc']

        res = self.db['source'].find_one({'_id': seis['source_id']})
        assert seis2['source_lat'] == res['lat']
        assert seis2['source_lon'] == res['lon']
        assert seis2['source_depth'] == res['depth']
        assert seis2['source_time'] == res['time']
        assert seis2['source_magnitude'] == res['magnitude']

        ts = copy.deepcopy(self.test_ts)
        self.db.save_data(ts, storage_mode='gridfs', update_all=True, exclude=['extra2'])
        ts2 = self.db.read_data(ts['_id'], 'wf_TimeSeries')

        for key in wf_keys:
            if key in ts:
                assert ts[key] == ts2[key]
        assert 'test' not in ts2
        assert 'extra2' not in ts2

        res = self.db['site'].find_one({'_id': ts['site_id']})
        assert ts2['site_lat'] == res['lat']
        assert ts2['site_lon'] == res['lon']
        assert ts2['site_elev'] == res['elev']
        assert ts2['site_starttime'] == res['starttime']
        assert ts2['site_endtime'] == res['endtime']
        assert ts2['net'] == res['net']
        assert ts2['sta'] == res['sta']
        assert ts2['loc'] == res['loc']

        res = self.db['source'].find_one({'_id': ts['source_id']})
        assert ts2['source_lat'] == res['lat']
        assert ts2['source_lon'] == res['lon']
        assert ts2['source_depth'] == res['depth']
        assert ts2['source_time'] == res['time']
        assert ts2['source_magnitude'] == res['magnitude']

        res = self.db['channel'].find_one({'_id': ts['channel_id']})
        assert ts2['chan'] == res['chan']
        assert ts2['channel_hang'] == res['hang']
        assert ts2['channel_vang'] == res['vang']
        assert ts2['channel_lat'] == res['lat']
        assert ts2['channel_lon'] == res['lon']
        assert ts2['channel_elev'] == res['elev']
        assert ts2['channel_edepth'] == res['edepth']
        assert ts2['channel_starttime'] == res['starttime']
        assert ts2['channel_endtime'] == res['endtime']

        # save data
        # gridfs
        assert seis2['storage_mode'] == 'gridfs'
        assert all(a.any() == b.any() for a, b in zip(seis.data, seis2.data))

        # file
        self.db.save_data(seis, storage_mode='file', dir='./python/tests/data/', dfile='test_db_output',
                          update_all=True, exclude=['extra2'])
        seis2 = self.db.read_data(seis['_id'], 'wf_Seismogram')
        assert seis2['storage_mode'] == 'file'
        assert all(a.any() == b.any() for a, b in zip(seis.data, seis2.data))

    def test_detele_wf(self):
        id = self.db['wf'].insert_one({'test': 'test'}).inserted_id
        res = self.db['wf'].find_one({'_id': id})
        assert res
        self.db.detele_wf(id, 'wf')
        res = self.db['wf'].find_one({'_id': id})
        assert not res

    def test_delete_gridfs(self):
        ts = copy.deepcopy(self.test_ts)
        self.db.save_data(ts, storage_mode='gridfs', update_all=True, exclude=['extra2'])
        gfsh = gridfs.GridFS(self.db)
        assert gfsh.exists(ts['gridfs_id'])
        self.db.delete_gridfs(ts['gridfs_id'])
        assert not gfsh.exists(ts['gridfs_id'])

    def test_update_ensemble_metadata(self):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        self.db.save_data(ts1, 'gridfs')
        self.db.save_data(ts2, 'gridfs')
        self.db.save_data(ts3, 'gridfs')

        time = datetime.utcnow().timestamp()
        ts1['t0'] = time
        ts1['tst'] = time
        ts2['t0'] = time
        ts3['t0'] = time
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)

        self.db.update_ensemble_metadata(ts_ensemble, update_all=True, exclude_objects=[2])
        res = self.db['wf_TimeSeries'].find_one({'_id': ts1['_id']})
        assert res['starttime'] == time
        res = self.db['wf_TimeSeries'].find_one({'_id': ts2['_id']})
        assert res['starttime'] == time
        res = self.db['wf_TimeSeries'].find_one({'_id': ts3['_id']})
        assert res['starttime'] != time

        time_new = datetime.utcnow().timestamp()
        ts_ensemble.member[0]['tst'] = time + 1
        ts_ensemble.member[0]['starttime'] = time_new
        self.db.update_ensemble_metadata(ts_ensemble, update_all=True, exclude_keys=['tst'])
        res = self.db['wf_TimeSeries'].find_one({'_id': ts1['_id']})
        assert res['tst'] != time + 1
        assert res['starttime'] == time_new

        # using seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        self.db.save_data(seis1, 'gridfs')
        self.db.save_data(seis2, 'gridfs')
        self.db.save_data(seis3, 'gridfs')
        time = datetime.utcnow().timestamp()
        seis1['t0'] = time
        seis1['tst'] = time
        seis2['t0'] = time
        seis3['t0'] = time
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)

        self.db.update_ensemble_metadata(seis_ensemble, update_all=True, exclude_objects=[2])
        res = self.db['wf_Seismogram'].find_one({'_id': seis1['_id']})
        assert res['starttime'] == time
        res = self.db['wf_Seismogram'].find_one({'_id': seis2['_id']})
        assert res['starttime'] == time
        res = self.db['wf_Seismogram'].find_one({'_id': seis3['_id']})
        assert res['starttime'] != time

        time_new = datetime.utcnow().timestamp()
        seis_ensemble.member[0]['tst'] = time + 1
        seis_ensemble.member[0]['starttime'] = time_new
        self.db.update_ensemble_metadata(seis_ensemble, update_all=True, exclude_keys=['tst'])
        res = self.db['wf_Seismogram'].find_one({'_id': seis1['_id']})
        assert res['tst'] != time + 1
        assert res['starttime'] == time_new


    def test_save_ensemble_data(self):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)
        dfile_list = ['test_db_output', 'test_db_output']
        dir_list = ['python/tests/data/', 'python/tests/data/']
        self.db.save_ensemble_data(ts_ensemble, 'file', dfile_list=dfile_list, dir_list=dir_list, exclude_objects=[1])
        res = self.db.read_data(ts_ensemble.member[0]['_id'], 'wf_TimeSeries')
        assert np.isclose(ts_ensemble.member[0].data, res.data).all()
        res = self.db.read_data(ts_ensemble.member[2]['_id'], 'wf_TimeSeries')
        assert np.isclose(ts_ensemble.member[2].data, res.data).all()
        assert '_id' not in ts_ensemble.member[1]

        self.db.save_ensemble_data(ts_ensemble, 'gridfs', exclude_objects=[1])
        res = self.db.read_data(ts_ensemble.member[0]['_id'], 'wf_TimeSeries')
        assert np.isclose(ts_ensemble.member[0].data, res.data).all()
        assert '_id' not in ts_ensemble.member[1]
        res = self.db.read_data(ts_ensemble.member[2]['_id'], 'wf_TimeSeries')
        assert np.isclose(ts_ensemble.member[2].data, res.data).all()

        # using seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)
        self.db.save_ensemble_data(seis_ensemble, 'file', dfile_list=dfile_list, dir_list=dir_list, exclude_objects=[1])
        res = self.db.read_data(seis_ensemble.member[0]['_id'], 'wf_Seismogram')
        assert np.isclose(seis_ensemble.member[0].data, res.data).all()
        res = self.db.read_data(seis_ensemble.member[2]['_id'], 'wf_Seismogram')
        assert np.isclose(seis_ensemble.member[2].data, res.data).all()
        assert '_id' not in seis_ensemble.member[1]

        self.db.save_ensemble_data(seis_ensemble, 'gridfs', exclude_objects=[1])
        res = self.db.read_data(seis_ensemble.member[0]['_id'], 'wf_Seismogram')
        assert np.isclose(seis_ensemble.member[0].data, res.data).all()
        assert '_id' not in seis_ensemble.member[1]
        res = self.db.read_data(seis_ensemble.member[2]['_id'], 'wf_Seismogram')
        assert np.isclose(seis_ensemble.member[2].data, res.data).all()

    def test_read_ensemble_data(self):
        with pytest.raises(KeyError) as err:
            self.db.read_ensemble_data([], 'other_collection', )
            assert err == KeyError("only wf_TimeSeries and wf_Seismogram are supported")

        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)
        self.db.save_ensemble_data(ts_ensemble, 'gridfs')
        res = self.db.read_ensemble_data([ts_ensemble.member[0]['_id'], ts_ensemble.member[1]['_id'],
                                    ts_ensemble.member[2]['_id']], 'wf_TimeSeries')
        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, ts_ensemble.member[i].data).all()

        # using seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)
        self.db.save_ensemble_data(seis_ensemble, 'gridfs')
        res = self.db.read_ensemble_data([seis_ensemble.member[0]['_id'], seis_ensemble.member[1]['_id'],
                                          seis_ensemble.member[2]['_id']], 'wf_Seismogram')
        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, seis_ensemble.member[i].data).all()

    def test_read_distributed_data(self, spark_context):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        self.db.save_data(ts1, 'gridfs')
        self.db.save_data(ts2, 'gridfs')
        self.db.save_data(ts3, 'gridfs')
        cursors = self.db['wf_TimeSeries'].find({})
        spark_list = read_distributed_data('localhost', 'dbtest', cursors, 'wf_TimeSeries', spark_context=spark_context)
        list = spark_list.collect()
        # assert len(list) == 3
        # for l in list:
        #     assert l
        #     assert np.isclose(l.data, ts1.data).all()

    def teardown_class(self):
        os.remove('python/tests/data/test_db_output')

if __name__ == '__main__':
    pass
