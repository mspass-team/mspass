import copy
import os

import dask.bag
import gridfs
import numpy as np
import pytest
import sys

from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble
from mspasspy.ccore.utility import dmatrix, ErrorSeverity, Metadata, MsPASSError

from mspasspy.db.schema import DatabaseSchema, MetadataSchema
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
        self.db2 = Database(client, 'dbtest')
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

    def test_init(self):
        db_schema = DatabaseSchema("mspass_lite.yaml")
        md_schema = MetadataSchema("mspass_lite.yaml")
        client = Client('localhost')
        db = Database(client, 'dbtest', db_schema=db_schema, md_schema=md_schema)
        with pytest.raises(AttributeError, match='no attribute'):
            dummy = db.database_schema.site
        with pytest.raises(MsPASSError, match='not defined'):
            dummy = db.database_schema['source']
        db2 = Database(client, 'dbtest', db_schema="mspass_lite.yaml", md_schema="mspass_lite.yaml")
        with pytest.raises(AttributeError, match='no attribute'):
            dummy = db.database_schema.site
        with pytest.raises(MsPASSError, match='not defined'):
            dummy = db.database_schema['source']

    def test_save_elogs(self):
        tmp_ts = get_live_timeseries()
        tmp_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        tmp_ts.elog.log_error("alg2", str("message2"), ErrorSeverity.Informational)  # multi elogs fix me
        errs = tmp_ts.elog.get_error_log()
        elog_id = self.db._save_elog(tmp_ts)
        assert isinstance(elog_id, ObjectId)
        elog_doc = self.db['elog'].find_one({'_id': elog_id})
        for err, res in zip(errs, elog_doc['logdata']):
            assert res['algorithm'] == err.algorithm
            assert res['error_message'] == err.message
            assert "wf_TImeSeries_id" not in res

        # empty logs
        tmp_ts = get_live_timeseries()
        errs = tmp_ts.elog.get_error_log()
        elog_id = self.db._save_elog(tmp_ts)
        assert elog_id is None

        # save with a dead object
        tmp_ts = get_live_timeseries()
        tmp_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        tmp_ts.live = False
        elog_id = self.db._save_elog(tmp_ts)
        elog_doc = self.db['elog'].find_one({'_id': elog_id})
        assert elog_doc['gravestone'] == dict(tmp_ts)

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

        logging_helper.info(ts, 'dummy_func_3', '3')
        new_history_object_id = self.db._save_history(ts, history_object_id)
        res = self.db['history_object'].find_one({'_id': history_object_id})
        assert not res
        res = self.db['history_object'].find_one({'_id': new_history_object_id})
        assert res

        ts_2 = TimeSeries()
        self.db._load_history(ts_2, new_history_object_id)
        loaded_nodes = ts_2.get_nodes()
        nodes = ts.get_nodes()
        assert str(nodes) == str(loaded_nodes)

        with pytest.raises(MsPASSError, match="The history object to be saved has a duplicate uuid"):
            self.db._save_history(ts)

    def test_update_metadata(self):
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        exclude = ['extra2']
        self.db.update_metadata(ts, include_undefined=True, exclude_keys=exclude)
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
        elog_res = self.db['elog'].find_one({'_id': res['elog_id']})
        assert elog_res['wf_TimeSeries_id'] == ts['_id']

        ts['extra1'] = 'extra1+'
        self.db.update_metadata(ts, include_undefined=True, exclude_keys=exclude)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res['extra1'] == 'extra1+'

        with pytest.raises(MsPASSError, match='Failure attempting to convert'):
            ts.put_string('npts', 'xyz')
            self.db.update_metadata(ts)
        
        # make sure the elog entry do not have duplicates from the two updates
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert len(self.db['elog'].find_one({'_id': res['elog_id']})['logdata']) == 2

        # save with a dead object
        ts.live = False
        # Nothing should be saved here, otherwise it will cause error converting 'npts':'xyz'
        self.db.update_metadata(ts)
        assert ts.elog.get_error_log()[-1].message == "Skipped updating the metadata of a dead object"
        elog_doc = self.db['elog'].find_one({'wf_TimeSeries_id': ts['_id'], 'gravestone': {'$exists': True}})
        assert elog_doc['gravestone'] == dict(ts)

    def test_save_read_data(self):
        # new object
        # read data

        seis2 = self.db.read_data(ObjectId())
        assert not seis2

        seis = copy.deepcopy(self.test_seis)
        logging_helper.info(seis, 'deepcopy', '1')

        self.db.save_data(seis, storage_mode='gridfs', include_undefined=True, exclude_keys=['extra2'])
        self.db.database_schema.set_default('wf_Seismogram', 'wf')
        seis2 = self.db.read_data(seis['_id'])
        seis3 = self.db.read_data(seis['_id'], exclude_keys=[
                                  '_id', 'channel_id', 'source_depth'])
        
        # test for read exclude
        assert '_id' in seis2
        assert 'channel_id' in seis2
        assert 'source_depth' in seis2
        assert '_id' not in seis3
        assert 'channel_id' not in seis3
        assert 'source_depth' not in seis3

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
        logging_helper.info(ts, 'deepcopy', '1')
        self.db.save_data(ts, storage_mode='gridfs', include_undefined=True, exclude_keys=['extra2'])
        self.db.database_schema.set_default('wf_TimeSeries', 'wf')
        ts2 = self.db.read_data(ts['_id'])
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
                          include_undefined=True, exclude_keys=['extra2'])
        self.db.database_schema.set_default('wf_Seismogram', 'wf')
        seis2 = self.db.read_data(seis['_id'])
        res = self.db['wf_Seismogram'].find_one({'_id': seis['_id']})
        assert res['storage_mode'] == 'file'
        assert all(a.any() == b.any() for a, b in zip(seis.data, seis2.data))

        with pytest.raises(ValueError, match='dir or dfile is not specified in data object'):
            self.db.save_data(seis2, storage_mode='file', include_undefined=True)
        seis2['dir'] = '/'
        seis2['dfile'] = 'test_db_output'
        with pytest.raises(PermissionError, match='No write permission to the save directory'):
            self.db.save_data(seis2, storage_mode='file', include_undefined=True)
        
        # save with a dead object
        seis.live = False
        self.db.save_data(seis)
        assert seis.elog.get_error_log()[-1].message == "Skipped saving dead object"
        elog_doc = self.db['elog'].find_one({'wf_Seismogram_id': seis['_id'], 'gravestone': {'$exists': True}})
        assert elog_doc['gravestone'] == dict(seis)

        # save to a different collection
        seis = copy.deepcopy(self.test_seis)
        logging_helper.info(seis, 'deepcopy', '1')
        db_schema = copy.deepcopy(self.db2.database_schema)
        md_schema = copy.deepcopy(self.db2.metadata_schema)
        wf_test = copy.deepcopy(self.db2.database_schema.wf_Seismogram)
        db_schema['wf_test'] = wf_test
        md_schema.Seismogram.swap_collection('wf_Seismogram', 'wf_test')
        self.db2.set_database_schema(db_schema)
        self.db2.set_metadata_schema(md_schema)
        self.db2.save_data(seis, storage_mode='gridfs', collection='wf_test')
        seis2 = self.db2.read_data(seis['_id'], collection='wf_test')
        assert all(a.any() == b.any() for a, b in zip(seis.data, seis2.data))
        with pytest.raises(MsPASSError, match='is not defined'):
            self.db2.read_data(seis['_id'], collection='wf_test2')

    # def test_delete_wf(self):
    #     id = self.db['wf'].insert_one({'test': 'test'}).inserted_id
    #     res = self.db['wf'].find_one({'_id': id})
    #     assert res
    #     self.db.delete_wf(id)
    #     res = self.db['wf'].find_one({'_id': id})
    #     assert not res

    #     ts = copy.deepcopy(self.test_ts)
    #     logging_helper.info(ts, 'deepcopy', '1')
    #     self.db.save_data(ts, storage_mode='gridfs', include_undefined=True, exclude_keys=['extra2'])
    #     res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
    #     assert res
    #     self.db.delete_wf(ts['_id'])
    #     res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
    #     assert not res

    # def test_delete_gridfs(self):
    #     ts = copy.deepcopy(self.test_ts)
    #     logging_helper.info(ts, 'deepcopy', '1')
    #     self.db.save_data(ts, storage_mode='gridfs', include_undefined=True, exclude_keys=['extra2'])
    #     res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
    #     gfsh = gridfs.GridFS(self.db)
    #     assert gfsh.exists(res['gridfs_id'])
    #     self.db.delete_gridfs(res['gridfs_id'])
    #     assert not gfsh.exists(res['gridfs_id'])

    def test_update_ensemble_metadata(self):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, 'deepcopy', '1')
        logging_helper.info(ts2, 'deepcopy', '1')
        logging_helper.info(ts3, 'deepcopy', '1')
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

        self.db.update_ensemble_metadata(ts_ensemble, include_undefined=True, exclude_objects=[2])
        res = self.db['wf_TimeSeries'].find_one({'_id': ts1['_id']})
        assert res['starttime'] == time
        res = self.db['wf_TimeSeries'].find_one({'_id': ts2['_id']})
        assert res['starttime'] == time
        res = self.db['wf_TimeSeries'].find_one({'_id': ts3['_id']})
        assert res['starttime'] != time

        time_new = datetime.utcnow().timestamp()
        ts_ensemble.member[0]['tst'] = time + 1
        ts_ensemble.member[0].t0 = time_new
        self.db.update_ensemble_metadata(ts_ensemble, include_undefined=True, exclude_keys=['tst'])
        res = self.db['wf_TimeSeries'].find_one({'_id': ts1['_id']})
        assert res['tst'] == time
        assert res['starttime'] == time_new

        # make sure the elog entry do not have duplicates from the two updates
        res1 = self.db['wf_TimeSeries'].find_one({'_id': ts1['_id']})
        res2 = self.db['wf_TimeSeries'].find_one({'_id': ts2['_id']})
        res3 = self.db['wf_TimeSeries'].find_one({'_id': ts3['_id']})
        assert len(self.db['elog'].find_one({'_id': res1['elog_id']})['logdata']) == 2
        assert len(self.db['elog'].find_one({'_id': res2['elog_id']})['logdata']) == 2
        assert len(self.db['elog'].find_one({'_id': res3['elog_id']})['logdata']) == 2

        # using seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        logging_helper.info(seis1, 'deepcopy', '1')
        logging_helper.info(seis2, 'deepcopy', '1')
        logging_helper.info(seis3, 'deepcopy', '1')
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

        self.db.update_ensemble_metadata(seis_ensemble, include_undefined=True, exclude_objects=[2])
        res = self.db['wf_Seismogram'].find_one({'_id': seis1['_id']})
        assert res['starttime'] == time
        res = self.db['wf_Seismogram'].find_one({'_id': seis2['_id']})
        assert res['starttime'] == time
        res = self.db['wf_Seismogram'].find_one({'_id': seis3['_id']})
        assert res['starttime'] != time

        time_new = datetime.utcnow().timestamp()
        seis_ensemble.member[0]['tst'] = time + 1
        seis_ensemble.member[0].t0 = time_new
        self.db.update_ensemble_metadata(seis_ensemble, include_undefined=True, exclude_keys=['tst'])
        res = self.db['wf_Seismogram'].find_one({'_id': seis1['_id']})
        assert res['tst'] == time
        assert res['starttime'] == time_new

    def test_save_ensemble_data(self):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, 'deepcopy', '1')
        logging_helper.info(ts2, 'deepcopy', '1')
        logging_helper.info(ts3, 'deepcopy', '1')
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)
        dfile_list = ['test_db_output', 'test_db_output']
        dir_list = ['python/tests/data/', 'python/tests/data/']
        self.db.save_ensemble_data(ts_ensemble, 'file', dfile_list=dfile_list, dir_list=dir_list, exclude_objects=[1])
        self.db.database_schema.set_default('wf_TimeSeries', 'wf')
        res = self.db.read_data(ts_ensemble.member[0]['_id'])
        assert np.isclose(ts_ensemble.member[0].data, res.data).all()
        res = self.db.read_data(ts_ensemble.member[2]['_id'])
        assert np.isclose(ts_ensemble.member[2].data, res.data).all()
        assert '_id' not in ts_ensemble.member[1]

        self.db.save_ensemble_data(ts_ensemble, 'gridfs', exclude_objects=[1])
        res = self.db.read_data(ts_ensemble.member[0]['_id'])
        assert np.isclose(ts_ensemble.member[0].data, res.data).all()
        assert '_id' not in ts_ensemble.member[1]
        res = self.db.read_data(ts_ensemble.member[2]['_id'])
        assert np.isclose(ts_ensemble.member[2].data, res.data).all()

        # using seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        logging_helper.info(seis1, 'deepcopy', '1')
        logging_helper.info(seis2, 'deepcopy', '1')
        logging_helper.info(seis3, 'deepcopy', '1')
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)
        self.db.save_ensemble_data(seis_ensemble, 'file', dfile_list=dfile_list, dir_list=dir_list, exclude_objects=[1])
        self.db.database_schema.set_default('wf_Seismogram', 'wf')
        res = self.db.read_data(seis_ensemble.member[0]['_id'])
        assert np.isclose(seis_ensemble.member[0].data, res.data).all()
        res = self.db.read_data(seis_ensemble.member[2]['_id'])
        assert np.isclose(seis_ensemble.member[2].data, res.data).all()
        assert '_id' not in seis_ensemble.member[1]

        self.db.save_ensemble_data(seis_ensemble, 'gridfs', exclude_objects=[1])
        res = self.db.read_data(seis_ensemble.member[0]['_id'])
        assert np.isclose(seis_ensemble.member[0].data, res.data).all()
        assert '_id' not in seis_ensemble.member[1]
        res = self.db.read_data(seis_ensemble.member[2]['_id'])
        assert np.isclose(seis_ensemble.member[2].data, res.data).all()

    def test_read_ensemble_data(self):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, 'deepcopy', '1')
        logging_helper.info(ts2, 'deepcopy', '1')
        logging_helper.info(ts3, 'deepcopy', '1')
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)
        self.db.database_schema.set_default('wf_TimeSeries', 'wf')
        self.db.save_ensemble_data(ts_ensemble, 'gridfs')
        res = self.db.read_ensemble_data([ts_ensemble.member[0]['_id'], ts_ensemble.member[1]['_id'],
                                          ts_ensemble.member[2]['_id']])
        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, ts_ensemble.member[i].data).all()

        # using seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        logging_helper.info(seis1, 'deepcopy', '1')
        logging_helper.info(seis2, 'deepcopy', '1')
        logging_helper.info(seis3, 'deepcopy', '1')
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)
        self.db.database_schema.set_default('wf_Seismogram', 'wf')
        self.db.save_ensemble_data(seis_ensemble, 'gridfs')
        res = self.db.read_ensemble_data([seis_ensemble.member[0]['_id'], seis_ensemble.member[1]['_id'],
                                          seis_ensemble.member[2]['_id']])
        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, seis_ensemble.member[i].data).all()


    def teardown_class(self):
        try:
            os.remove('python/tests/data/test_db_output')
        except OSError:
            pass
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
    logging_helper.info(ts1, 'deepcopy', '1')
    logging_helper.info(ts2, 'deepcopy', '1')
    logging_helper.info(ts3, 'deepcopy', '1')

    ts_list = [ts1, ts2, ts3]
    ts_list_rdd = spark_context.parallelize(ts_list)
    ts_list_rdd.foreach(lambda d, database=db: database.save_data(d, 'gridfs'))
    cursors = db['wf_TimeSeries'].find({})

    spark_list = read_distributed_data('localhost', 'mspasspy_test_db', cursors, collection='wf_TimeSeries', spark_context=spark_context)
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
    logging_helper.info(ts1, 'deepcopy', '1')
    logging_helper.info(ts2, 'deepcopy', '1')
    logging_helper.info(ts3, 'deepcopy', '1')

    ts_list = [ts1, ts2, ts3]
    ts_list_dbg = dask.bag.from_sequence(ts_list)
    ts_list_dbg.map(db.save_data, 'gridfs').compute()
    cursors = db['wf_TimeSeries'].find({})

    dask_list = read_distributed_data('localhost', 'mspasspy_test_db', cursors, collection='wf_TimeSeries', format='dask')
    list = dask_list.compute()
    assert len(list) == 3
    for l in list:
        assert l
        assert np.isclose(l.data, test_ts.data).all()

    client = Client('localhost')
    client.drop_database('mspasspy_test_db')


if __name__ == '__main__':
    pass
