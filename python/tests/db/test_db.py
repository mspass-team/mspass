import copy
import os

import gridfs
import numpy as np
import pytest
import sys

from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble
from mspasspy.ccore.utility import dmatrix, ErrorSeverity, Metadata, MsPASSError

from mspasspy.db.schema import MetadataSchema
from mspasspy.util import logging_helper
from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")

from mspasspy.db.database import Database, read_distributed_data
from mspasspy.db.client import Client
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble)



class TestDatabase():

    def setup_class(self):
        client = Client('localhost')
        self.db = Database(client, 'dbtest')
        self.metadata_def = MetadataSchema()

        self.test_ts = get_live_timeseries()
        self.test_ts['test'] = ' '  # empty key
        self.test_ts['extra1'] = 'extra1'
        self.test_ts['extra2'] = 'extra2'  # exclude
        self.test_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_ts.erase('starttime')
        self.test_ts['t0'] = datetime.utcnow().timestamp()

        self.test_seis = get_live_seismogram()
        self.test_seis['test'] = ' '  # empty key
        self.test_seis['extra1'] = 'extra1'
        self.test_seis['extra2'] = 'extra2'  # exclude
        self.test_seis.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_seis.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_seis.erase('starttime')
        self.test_seis['t0'] = datetime.utcnow().timestamp()
        self.test_seis['tmatrix'] = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]

        site_id = ObjectId()
        channel_id = ObjectId()
        source_id = ObjectId()
        self.db['site'].insert_one({'_id': site_id, 'net': 'net1', 'sta': 'sta1', 'loc': 'loc', 'lat': 1.0, 'lon': 1.0,
                                    'elev': 2.0, 'starttime': datetime.utcnow().timestamp(),
                                    'endtime': datetime.utcnow().timestamp()})
        self.db['channel'].insert_one({'_id': channel_id, 'net': 'net1', 'sta': 'sta1', 'loc': 'loc1', 'chan': 'chan',
                                       'lat': 1.1, 'lon': 1.1, 'elev': 2.1, 'starttime': datetime.utcnow().timestamp(),
                                       'endtime': datetime.utcnow().timestamp(), 'edepth': 3.0, 'vang': 1.0,
                                       'hang': 1.0})
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
        tmp_ts.elog.log_error("alg2", str("message2"), ErrorSeverity.Informational)  # multi elogs fix me
        errs = tmp_ts.elog.get_error_log()
        elog_id = self.db._save_elog(tmp_ts)
        assert len(elog_id) != 0
        for err, id in zip(errs, elog_id):
            res = self.db['elog'].find_one({'_id': id})
            assert res['algorithm'] == err.algorithm
            assert res['error_message'] == err.message
            assert "wf_TImeSeries_id" not in res

        # empty logs
        tmp_ts = get_live_timeseries()
        errs = tmp_ts.elog.get_error_log()
        elog_id = self.db._save_elog(tmp_ts)
        assert len(elog_id) == 0

    def test_save_and_read_data(self):
        tmp_seis = get_live_seismogram()
        dir = 'python/tests/data/'
        dfile = 'test_db_output'
        foff = self.db._save_data_to_dfile(tmp_seis, dir, dfile)
        tmp_seis_2 = Seismogram()
        tmp_seis_2.npts = 255
        self.db._read_data_from_dfile(tmp_seis_2, dir, dfile, foff)
        assert all(a.any() == b.any() for a, b in zip(tmp_seis.data, tmp_seis_2.data))

        tmp_ts = get_live_timeseries()
        foff = self.db._save_data_to_dfile(tmp_ts, dir, dfile)
        tmp_ts_2 = TimeSeries()
        tmp_ts_2.npts = 255
        self.db._read_data_from_dfile(tmp_ts_2, dir, dfile, foff)
        assert all(a == b for a, b in zip(tmp_ts.data, tmp_ts_2.data))

    def test_save_and_read_gridfs(self):
        tmp_seis = get_live_seismogram()
        gridfs_id = self.db._save_data_to_gridfs(tmp_seis)
        tmp_seis_2 = Seismogram()
        tmp_seis_2.npts = 255
        self.db._read_data_from_gridfs(tmp_seis_2, gridfs_id)
        assert all(a.any() == b.any() for a, b in zip(tmp_seis.data, tmp_seis_2.data))

        with pytest.raises(KeyError, match='npts is not defined'):
            tmp_seis_2.erase('npts')
            self.db._read_data_from_gridfs(tmp_seis_2, gridfs_id)

        with pytest.raises(ValueError) as err:
            tmp_seis_2.npts = 254
            self.db._read_data_from_gridfs(tmp_seis_2, gridfs_id)
            assert str(err.value) == "ValueError: Size mismatch in sample data. " \
                                     "Number of points in gridfs file = 765 but expected 762"

        tmp_ts = get_live_timeseries()
        gridfs_id = self.db._save_data_to_gridfs(tmp_ts)
        tmp_ts_2 = TimeSeries()
        tmp_ts_2.npts = 255
        self.db._read_data_from_gridfs(tmp_ts_2, gridfs_id)
        assert all(a == b for a, b in zip(tmp_ts.data, tmp_ts_2.data))

        gfsh = gridfs.GridFS(self.db)
        assert gfsh.exists(gridfs_id)
        self.db._save_data_to_gridfs(tmp_ts, gridfs_id)
        assert not gfsh.exists(gridfs_id)

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
        history_object_id = self.db._save_history(ts)
        res = self.db['history_object'].find_one({'_id': history_object_id})
        assert res

        ts_2 = TimeSeries()
        self.db._load_history(ts_2, history_object_id)
        loaded_nodes = ts_2.get_nodes()
        assert str(nodes) == str(loaded_nodes)

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
        assert 'history_object_id' in res

        history_res = self.db['history_object'].find_one({'_id': res['history_object_id']})
        assert history_res

        assert res['elog_id']
        for id in res['elog_id']:
            elog_res = self.db['elog'].find_one({'_id': id})
            assert elog_res['wf_TimeSeries_id'] == ts['_id']

        ts['extra1'] = 'extra1+'
        self.db.update_metadata(ts, update_all=True, exclude=exclude)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res['extra1'] == 'extra1+'

        with pytest.raises(MsPASSError, match='Failure attempting to convert'):
            ts.put_string('npts', 'xyz')
            self.db.update_metadata(ts)

    def test_save_read_data(self):
        # new object
        # read data
        with pytest.raises(KeyError, match='only TimeSeries and Seismogram are supported'):
            seis2 = self.db.read_data(ObjectId(), 'wrong')

        seis2 = self.db.read_data(ObjectId(), 'Seismogram')
        assert not seis2

        seis = copy.deepcopy(self.test_seis)
        self.db.save_data(seis, storage_mode='gridfs', update_all=True, exclude=['extra2'])
        seis2 = self.db.read_data(seis['_id'], 'Seismogram')

        wf_keys = ['_id', 'npts', 'delta', 'sampling_rate', 'calib', 'starttime', 'dtype', 'site_id', 'channel_id',
                   'source_id', 'storage_mode', 'dir', 'dfile', 'foff', 'gridfs_id', 'url', 'elog_id',
                   'history_object_id', 'time_standard', 'tmatrix']
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
        ts2 = self.db.read_data(ts['_id'], 'TimeSeries')
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
        assert ts2['loc'] == res['loc']

        # save data
        # gridfs
        res = self.db['wf_Seismogram'].find_one({'_id': seis['_id']})
        assert res['storage_mode'] == 'gridfs'
        assert all(a.any() == b.any() for a, b in zip(seis.data, seis2.data))

        # file
        self.db.save_data(seis, storage_mode='file', dir='./python/tests/data/', dfile='test_db_output',
                          update_all=True, exclude=['extra2'])
        seis2 = self.db.read_data(seis['_id'], 'Seismogram')
        res = self.db['wf_Seismogram'].find_one({'_id': seis['_id']})
        assert res['storage_mode'] == 'file'
        assert all(a.any() == b.any() for a, b in zip(seis.data, seis2.data))

        with pytest.raises(ValueError, match='dir or dfile is not specified in data object'):
            self.db.save_data(seis2, storage_mode='file', update_all=True)
        seis2['dir'] = '/'
        seis2['dfile'] = 'test_db_output'
        with pytest.raises(PermissionError, match='No write permission to the save directory'):
            self.db.save_data(seis2, storage_mode='file', update_all=True)

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
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        gfsh = gridfs.GridFS(self.db)
        assert gfsh.exists(res['gridfs_id'])
        self.db.delete_gridfs(res['gridfs_id'])
        assert not gfsh.exists(res['gridfs_id'])

    def test_update_ensemble_metadata(self):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        self.db.save_data(ts1, 'gridfs')
        self.db.save_data(ts2, 'gridfs')
        self.db.save_data(ts3, 'gridfs')

        time = datetime.utcnow().timestamp()
        ts1.t0 = time
        ts1['tst'] = time
        ts2.t0 = time
        ts3.t0 = time
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
        ts_ensemble.member[0].t0 = time_new
        self.db.update_ensemble_metadata(ts_ensemble, update_all=True, exclude_keys=['tst'])
        res = self.db['wf_TimeSeries'].find_one({'_id': ts1['_id']})
        assert res['tst'] == time
        assert res['starttime'] == time_new

        # using seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        self.db.save_data(seis1, 'gridfs')
        self.db.save_data(seis2, 'gridfs')
        self.db.save_data(seis3, 'gridfs')
        time = datetime.utcnow().timestamp()
        seis1.t0 = time
        seis1['tst'] = time
        seis2.t0 = time
        seis3.t0 = time
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
        seis_ensemble.member[0].t0 = time_new
        self.db.update_ensemble_metadata(seis_ensemble, update_all=True, exclude_keys=['tst'])
        res = self.db['wf_Seismogram'].find_one({'_id': seis1['_id']})
        assert res['tst'] == time
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
        res = self.db.read_data(ts_ensemble.member[0]['_id'], 'TimeSeries')
        assert np.isclose(ts_ensemble.member[0].data, res.data).all()
        res = self.db.read_data(ts_ensemble.member[2]['_id'], 'TimeSeries')
        assert np.isclose(ts_ensemble.member[2].data, res.data).all()
        assert '_id' not in ts_ensemble.member[1]

        self.db.save_ensemble_data(ts_ensemble, 'gridfs', exclude_objects=[1])
        res = self.db.read_data(ts_ensemble.member[0]['_id'], 'TimeSeries')
        assert np.isclose(ts_ensemble.member[0].data, res.data).all()
        assert '_id' not in ts_ensemble.member[1]
        res = self.db.read_data(ts_ensemble.member[2]['_id'], 'TimeSeries')
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
        res = self.db.read_data(seis_ensemble.member[0]['_id'], 'Seismogram')
        assert np.isclose(seis_ensemble.member[0].data, res.data).all()
        res = self.db.read_data(seis_ensemble.member[2]['_id'], 'Seismogram')
        assert np.isclose(seis_ensemble.member[2].data, res.data).all()
        assert '_id' not in seis_ensemble.member[1]

        self.db.save_ensemble_data(seis_ensemble, 'gridfs', exclude_objects=[1])
        res = self.db.read_data(seis_ensemble.member[0]['_id'], 'Seismogram')
        assert np.isclose(seis_ensemble.member[0].data, res.data).all()
        assert '_id' not in seis_ensemble.member[1]
        res = self.db.read_data(seis_ensemble.member[2]['_id'], 'Seismogram')
        assert np.isclose(seis_ensemble.member[2].data, res.data).all()

    def test_read_ensemble_data(self):
        with pytest.raises(KeyError, match='only TimeSeriesEnsemble and SeismogramEnsemble are supported'):
            self.db.read_ensemble_data([], 'other_collection', )

        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)
        self.db.save_ensemble_data(ts_ensemble, 'gridfs')
        res = self.db.read_ensemble_data([ts_ensemble.member[0]['_id'], ts_ensemble.member[1]['_id'],
                                          ts_ensemble.member[2]['_id']], 'TimeSeriesEnsemble')
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
                                          seis_ensemble.member[2]['_id']], 'SeismogramEnsemble')
        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, seis_ensemble.member[i].data).all()


    def teardown_class(self):
        os.remove('python/tests/data/test_db_output')
        client = Client('localhost')
        client.drop_database('dbtest')

def test_read_distributed_data(spark_context):
    client = Client('localhost')
    client.drop_database('mspasspy_test_db')

    test_ts = get_live_timeseries()

    client = Client('localhost')
    db = Database(client, 'mspasspy_test_db')

    site_id = ObjectId()
    channel_id = ObjectId()
    source_id = ObjectId()
    db['site'].insert_one({'_id': site_id, 'net': 'net', 'sta': 'sta', 'loc': 'loc', 'lat': 1.0, 'lon': 1.0,
                           'elev': 2.0, 'starttime': datetime.utcnow().timestamp(),
                           'endtime': datetime.utcnow().timestamp()})
    db['channel'].insert_one({'_id': channel_id, 'net': 'net1', 'sta': 'sta1', 'loc': 'loc1', 'chan': 'chan',
                              'lat': 1.1, 'lon': 1.1, 'elev': 2.1, 'starttime': datetime.utcnow().timestamp(),
                              'endtime': datetime.utcnow().timestamp(), 'edepth': 3.0, 'vang': 1.0,
                              'hang': 1.0})
    db['source'].insert_one({'_id': source_id, 'lat': 1.2, 'lon': 1.2, 'time': datetime.utcnow().timestamp(),
                             'depth': 3.1, 'magnitude': 1.0})
    test_ts['site_id'] = site_id
    test_ts['source_id'] = source_id
    test_ts['channel_id'] = channel_id

    ts1 = copy.deepcopy(test_ts)
    ts2 = copy.deepcopy(test_ts)
    ts3 = copy.deepcopy(test_ts)

    db.save_data(ts1, 'gridfs')
    db.save_data(ts2, 'gridfs')
    db.save_data(ts3, 'gridfs')
    cursors = db['wf_TimeSeries'].find({})

    spark_list = read_distributed_data('localhost', 'mspasspy_test_db', cursors, 'TimeSeries', spark_context=spark_context)
    list = spark_list.collect()
    assert len(list) == 3
    for l in list:
        assert l
        assert np.isclose(l.data, test_ts.data).all()

    client = Client('localhost')
    client.drop_database('mspasspy_test_db')


def test_read_distributed_data_dask():
    client = Client('localhost')
    client.drop_database('mspasspy_test_db')

    test_ts = get_live_timeseries()

    client = Client('localhost')
    db = Database(client, 'mspasspy_test_db')

    site_id = ObjectId()
    channel_id = ObjectId()
    source_id = ObjectId()
    db['site'].insert_one({'_id': site_id, 'net': 'net', 'sta': 'sta', 'loc': 'loc', 'lat': 1.0, 'lon': 1.0,
                           'elev': 2.0, 'starttime': datetime.utcnow().timestamp(),
                           'endtime': datetime.utcnow().timestamp()})
    db['channel'].insert_one({'_id': channel_id, 'net': 'net1', 'sta': 'sta1', 'loc': 'loc1', 'chan': 'chan',
                              'lat': 1.1, 'lon': 1.1, 'elev': 2.1, 'starttime': datetime.utcnow().timestamp(),
                              'endtime': datetime.utcnow().timestamp(), 'edepth': 3.0, 'vang': 1.0,
                              'hang': 1.0})
    db['source'].insert_one({'_id': source_id, 'lat': 1.2, 'lon': 1.2, 'time': datetime.utcnow().timestamp(),
                             'depth': 3.1, 'magnitude': 1.0})
    test_ts['site_id'] = site_id
    test_ts['source_id'] = source_id
    test_ts['channel_id'] = channel_id

    ts1 = copy.deepcopy(test_ts)
    ts2 = copy.deepcopy(test_ts)
    ts3 = copy.deepcopy(test_ts)

    db.save_data(ts1, 'gridfs')
    db.save_data(ts2, 'gridfs')
    db.save_data(ts3, 'gridfs')
    cursors = db['wf_TimeSeries'].find({})

    dask_list = read_distributed_data('localhost', 'mspasspy_test_db', cursors, 'TimeSeries', format='dask')
    list = dask_list.compute()
    assert len(list) == 3
    for l in list:
        assert l
        assert np.isclose(l.data, test_ts.data).all()

    client = Client('localhost')
    client.drop_database('mspasspy_test_db')


if __name__ == '__main__':
    pass
