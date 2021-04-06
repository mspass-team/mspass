import copy
import os

import dask.bag
import gridfs
import numpy as np
import obspy 
import pytest
import sys
import re

from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble
from mspasspy.ccore.utility import dmatrix, ErrorSeverity, Metadata, MsPASSError, ProcessingHistory

from mspasspy.db.schema import DatabaseSchema, MetadataSchema
from mspasspy.util import logging_helper
from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")

from mspasspy.db.database import Database, read_distributed_data
from mspasspy.db.client import DBClient
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble)



class TestDatabase():

    def setup_class(self):
        client = DBClient('localhost')
        self.db = Database(client, 'dbtest')
        self.db2 = Database(client, 'dbtest')
        self.metadata_def = MetadataSchema()
        # clean up the database locally
        for col_name in self.db.list_collection_names():
            self.db[col_name].delete_many({})

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

        # save inventory to db
        inv = obspy.read_inventory('python/tests/data/TA.035A.xml')
        self.db.save_inventory(inv)

    def test_init(self):
        db_schema = DatabaseSchema("mspass_lite.yaml")
        md_schema = MetadataSchema("mspass_lite.yaml")
        client = DBClient('localhost')
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
        assert 'wf_TimeSeries_id' not in res
        assert res['alg_name'] == 'dummy_func_2'
        assert res['alg_id'] == '2'

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
        assert 'wf_TimeSeries_id' not in res
        assert res['alg_name'] == 'dummy_func_3'
        assert res['alg_id'] == '3'

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
        # test promiscuous, exclude_keys, clear aliases, empty value
        non_fatal_error_cnt = self.db.update_metadata(ts, mode='promiscuous', exclude_keys=exclude)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res
        assert res['starttime'] == ts['t0']
        assert 'test' not in res
        assert 'extra2' not in res
        assert res['extra1'] == 'extra1'
        assert 'net' not in res
        assert '_id' in ts
        assert non_fatal_error_cnt == 0
        
        assert 'history_object_id' in res
        history_res = self.db['history_object'].find_one({'_id': res['history_object_id']})
        assert history_res

        assert 'elog_id' in res
        elog_res = self.db['elog'].find_one({'_id': res['elog_id']})
        assert elog_res['wf_TimeSeries_id'] == ts['_id']
        assert len(self.db['elog'].find_one({'_id': res['elog_id']})['logdata']) == 2

        # test read-only attribute
        ts['net'] = 'Asia'
        non_fatal_error_cnt = self.db.update_metadata(ts, mode='promiscuous', exclude_keys=exclude)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert 'net' not in res
        assert 'READONLYERROR_net' in res
        assert res['READONLYERROR_net'] == 'Asia'
        assert non_fatal_error_cnt == 1
        assert ts.elog.get_error_log()[-1].message == "attribute net is read only and cannot be updated, but the attribute is saved as READONLYERROR_net"
        assert len(ts.elog.get_error_log()) == 3
        ts.erase('net')
        
        # test promiscuous
        ts['extra1'] = 'extra1+'
        non_fatal_error_cnt = self.db.update_metadata(ts, mode='promiscuous', exclude_keys=exclude)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res['extra1'] == 'extra1+'

        # test cautious
        old_npts = ts['npts']
        ts.put_string('npts', 'xyz')
        non_fatal_error_cnt = self.db.update_metadata(ts, mode='cautious')
        # required attribute update fail -> fatal error causes dead
        assert non_fatal_error_cnt == -1
        assert not ts.live
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        # attr value remains the same
        assert res['npts'] == old_npts
        assert ts.elog.get_error_log()[-2].message == "cautious mode: Required attribute npts has type <class 'str'>, forbidden by definition and unable to convert"
        assert ts.elog.get_error_log()[-1].message == "Skipped updating the metadata of a dead object"
        assert len(ts.elog.get_error_log()) == 5
        elog_doc = self.db['elog'].find_one({'wf_TimeSeries_id': ts['_id'], 'gravestone': {'$exists': True}})
        assert elog_doc['gravestone'] == dict(ts)

        # test pedantic
        ts.set_live()
        # sampling rate is optional constraint
        old_sampling_rate = ts['sampling_rate']
        ts.put_string('sampling_rate', 'xyz')
        non_fatal_error_cnt = self.db.update_metadata(ts, mode='pedantic')
        # required attribute update fail -> fatal error causes dead
        assert non_fatal_error_cnt == -1
        assert not ts.live
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        # attr value remains the same
        assert res['sampling_rate'] == old_sampling_rate
        # add two more log error to the elog(both npts and sampling rate are mismatch)
        assert ts.elog.get_error_log()[-3].message == "pedantic mode: attribute npts has type <class 'str'>, forbidden by definition"
        assert ts.elog.get_error_log()[-2].message == "pedantic mode: attribute sampling_rate has type <class 'str'>, forbidden by definition"
        assert ts.elog.get_error_log()[-1].message == "Skipped updating the metadata of a dead object"
        assert len(ts.elog.get_error_log()) == 8
        dead_elog_doc_list = self.db['elog'].find({'wf_TimeSeries_id': ts['_id'], 'gravestone': {'$exists': True}})
        assert len(list(dead_elog_doc_list)) == 2

        # save with a dead object
        ts.live = False
        # Nothing should be saved here, otherwise it will cause error converting 'npts':'xyz'
        self.db.update_metadata(ts)
        assert ts.elog.get_error_log()[-1].message == "Skipped updating the metadata of a dead object"
        dead_elog_doc_list = self.db['elog'].find({'wf_TimeSeries_id': ts['_id'], 'gravestone': {'$exists': True}})
        assert len(list(dead_elog_doc_list)) == 3

    def test_save_read_data(self):
        # new object
        # read data

        fail_seis = self.db.read_data(ObjectId(), mode='cautious', normalize=['site', 'source'])
        assert not fail_seis

        # tests for Seismogram
        promiscuous_seis = copy.deepcopy(self.test_seis)
        cautious_seis = copy.deepcopy(self.test_seis)
        pedantic_seis = copy.deepcopy(self.test_seis)
        logging_helper.info(promiscuous_seis, 'deepcopy', '1')
        logging_helper.info(cautious_seis, 'deepcopy', '1')
        logging_helper.info(pedantic_seis, 'deepcopy', '1')

        save_res_code = self.db.save_data(promiscuous_seis, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        assert promiscuous_seis.live
        
        cautious_seis.put_string('npts', 'xyz')
        save_res_code = self.db.save_data(cautious_seis, mode='cautious', storage_mode='gridfs')
        assert save_res_code == -1
        assert not cautious_seis.live

        pedantic_seis.put_string('sampling_rate', 'xyz')
        save_res_code = self.db.save_data(pedantic_seis, mode='pedantic', storage_mode='gridfs')
        assert save_res_code == -1
        assert not pedantic_seis.live
        
        
        self.db.database_schema.set_default('wf_Seismogram', 'wf')
        promiscuous_seis2 = self.db.read_data(promiscuous_seis['_id'], mode='promiscuous', normalize=['site', 'source'])
        no_source_seis2 = self.db.read_data(promiscuous_seis['_id'], mode='promiscuous', normalize=['site'])
        exclude_promiscuous_seis2 = self.db.read_data(promiscuous_seis['_id'], mode='promiscuous', normalize=['site', 'source'], exclude_keys=[
                                  '_id', 'channel_id', 'source_depth'])
        cautious_seis2 = self.db.read_data(promiscuous_seis['_id'], mode='cautious', normalize=['site', 'source'])

        # test extra key
        assert 'extra1' in promiscuous_seis2
        assert 'extra1' not in cautious_seis2

        # test normalize parameter
        assert 'source_lat' not in no_source_seis2
        assert 'source_lon' not in no_source_seis2
        assert 'source_depth' not in no_source_seis2
        assert 'source_time' not in no_source_seis2
        assert 'source_magnitude' not in no_source_seis2

        # test cautious read
        cautious_seis.set_live()
        save_res_code = self.db.save_data(cautious_seis, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        assert cautious_seis.live
        cautious_seis2 = self.db.read_data(cautious_seis['_id'], mode='cautious', normalize=['site', 'source'])
        assert cautious_seis2.elog.get_error_log()[-1].message == "cautious mode: Required attribute npts has type <class 'str'>, forbidden by definition and unable to convert"
        elog_doc = self.db['elog'].find_one({'wf_Seismogram_id': cautious_seis2['_id'], 'gravestone': {'$exists': True}})
        assert 'data' not in cautious_seis2

        # test pedantic read
        pedantic_seis.set_live()
        save_res_code = self.db.save_data(pedantic_seis, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        assert pedantic_seis.live
        pedantic_seis2 = self.db.read_data(pedantic_seis['_id'], mode='pedantic', normalize=['site', 'source'])
        assert pedantic_seis2.elog.get_error_log()[-1].message == "pedantic mode: sampling_rate has type <class 'str'>, forbidden by definition"
        elog_doc = self.db['elog'].find_one({'wf_Seismogram_id': pedantic_seis['_id'], 'gravestone': {'$exists': True}})
        assert 'data' not in pedantic_seis2

        # test read exclude parameter
        assert '_id' in promiscuous_seis2
        assert 'channel_id' in promiscuous_seis2
        assert 'source_depth' in promiscuous_seis2
        assert '_id' not in exclude_promiscuous_seis2
        assert 'channel_id' not in exclude_promiscuous_seis2
        assert 'source_depth' not in exclude_promiscuous_seis2

        wf_keys = ['_id', 'npts', 'delta', 'sampling_rate', 'calib', 'starttime', 'dtype', 'site_id', 'channel_id',
                   'source_id', 'storage_mode', 'dir', 'dfile', 'foff', 'gridfs_id', 'url', 'elog_id',
                   'history_object_id', 'time_standard', 'tmatrix']
        for key in wf_keys:
            if key in promiscuous_seis:
                assert promiscuous_seis[key] == promiscuous_seis2[key]
        assert 'test' not in promiscuous_seis2
        assert 'extra2' not in promiscuous_seis2
        assert promiscuous_seis['channel_id'] == promiscuous_seis2['channel_id']

        res = self.db['site'].find_one({'_id': promiscuous_seis['site_id']})
        assert promiscuous_seis2['site_lat'] == res['lat']
        assert promiscuous_seis2['site_lon'] == res['lon']
        assert promiscuous_seis2['site_elev'] == res['elev']
        assert promiscuous_seis2['site_starttime'] == res['starttime']
        assert promiscuous_seis2['site_endtime'] == res['endtime']
        assert promiscuous_seis2['net'] == res['net']
        assert promiscuous_seis2['sta'] == res['sta']
        assert promiscuous_seis2['loc'] == res['loc']

        res = self.db['source'].find_one({'_id': promiscuous_seis['source_id']})
        assert promiscuous_seis2['source_lat'] == res['lat']
        assert promiscuous_seis2['source_lon'] == res['lon']
        assert promiscuous_seis2['source_depth'] == res['depth']
        assert promiscuous_seis2['source_time'] == res['time']
        assert promiscuous_seis2['source_magnitude'] == res['magnitude']

        # tests for TimeSeries
        # not testing promiscuous/cautious/pedantic save->read here because it's coveraged by the tests above
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'], data_tag='tag1')
        self.db.database_schema.set_default('wf_TimeSeries', 'wf')
        # test mismatch data_tag
        assert not self.db.read_data(ts['_id'], mode='promiscuous', normalize=['site', 'source', 'channel'], data_tag='tag2')
        ts2 = self.db.read_data(ts['_id'], mode='promiscuous', normalize=['site', 'source', 'channel'], data_tag='tag1')
        for key in wf_keys:
            if key in ts:
                assert ts[key] == ts2[key]
        assert 'test' not in ts2
        assert 'extra2' not in ts2
        assert 'data_tag' in ts2 and ts2['data_tag'] == 'tag1'
        # dummy ts without data_tag
        dummy_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(dummy_ts, 'deepcopy', '1')
        self.db.save_data(dummy_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert not self.db.read_data(dummy_ts['_id'], mode='promiscuous', normalize=['site', 'source', 'channel'], data_tag='tag1')

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

        # test save data with different storage mode
        # gridfs
        res = self.db['wf_Seismogram'].find_one({'_id': promiscuous_seis['_id']})
        assert res['storage_mode'] == 'gridfs'
        assert all(a.any() == b.any() for a, b in zip(promiscuous_seis.data, promiscuous_seis2.data))

        # file
        self.db.save_data(promiscuous_seis, mode='promiscuous', storage_mode='file', dir='./python/tests/data/', dfile='test_db_output', exclude_keys=['extra2'])
        self.db.database_schema.set_default('wf_Seismogram', 'wf')
        promiscuous_seis2 = self.db.read_data(promiscuous_seis['_id'], mode='cautious', normalize=['site', 'source'])

        res = self.db['wf_Seismogram'].find_one({'_id': promiscuous_seis['_id']})
        assert res['storage_mode'] == 'file'
        assert all(a.any() == b.any() for a, b in zip(promiscuous_seis.data, promiscuous_seis2.data))

        with pytest.raises(ValueError, match='dir or dfile is not specified in data object'):
            self.db.save_data(promiscuous_seis2, mode='promiscuous', storage_mode='file')
        promiscuous_seis2['dir'] = '/'
        promiscuous_seis2['dfile'] = 'test_db_output'
        with pytest.raises(PermissionError, match='No write permission to the save directory'):
            self.db.save_data(promiscuous_seis2, mode='promiscuous', storage_mode='file')
        
        # save with a dead object
        promiscuous_seis.live = False
        self.db.save_data(promiscuous_seis, mode='promiscuous')
        assert promiscuous_seis.elog.get_error_log()[-1].message == "Skipped saving dead object"
        elog_doc = self.db['elog'].find_one({'wf_Seismogram_id': promiscuous_seis['_id'], 'gravestone': {'$exists': True}})
        assert elog_doc['gravestone'] == dict(promiscuous_seis)

        # save to a different collection
        promiscuous_seis = copy.deepcopy(self.test_seis)
        logging_helper.info(promiscuous_seis, 'deepcopy', '1')
        db_schema = copy.deepcopy(self.db2.database_schema)
        md_schema = copy.deepcopy(self.db2.metadata_schema)
        wf_test = copy.deepcopy(self.db2.database_schema.wf_Seismogram)
        db_schema['wf_test'] = wf_test
        md_schema.Seismogram.swap_collection('wf_Seismogram', 'wf_test')
        self.db2.set_database_schema(db_schema)
        self.db2.set_metadata_schema(md_schema)
        self.db2.save_data(promiscuous_seis, mode='promiscuous', storage_mode='gridfs', collection='wf_test')
        promiscuous_seis2 = self.db2.read_data(promiscuous_seis['_id'], mode='cautious', normalize=['site', 'source'], collection='wf_test')
        assert all(a.any() == b.any() for a, b in zip(promiscuous_seis.data, promiscuous_seis2.data))
        with pytest.raises(MsPASSError, match='is not defined'):
            self.db2.read_data(promiscuous_seis['_id'], mode='cautious', normalize=['site', 'source'], collection='wf_test2')

    def test_delete_wf(self):
        # clear all the wf collection documents
        # self.db['wf_TimeSeries'].delete_many({})

        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert save_res_code == 0
        assert ts.live

        nonexistent_id = ObjectId()
        # only delete waveform data
        with pytest.raises(TypeError, match='only TimeSeries and Seismogram are supported'):
            self.db.delete_data(nonexistent_id, 'site')
        # can not find the document with given _id
        with pytest.raises(MsPASSError, match='Could not find document in wf collection by _id: {}.'.format(nonexistent_id)):
            self.db.delete_data(nonexistent_id, 'TimeSeries')
        
        # check gridfs exist
        gfsh = gridfs.GridFS(self.db)
        assert gfsh.exists(res['gridfs_id'])
        # insert a dummy elog document with wf_TimeSeries_id equals to ts['_id']
        self.db['elog'].insert_one({'_id': ObjectId(), 'wf_TimeSeries_id': ts['_id']})
        
        # grid_fs delete(clear_history, clear_elog)
        self.db.delete_data(ts['_id'], 'TimeSeries')
        assert not self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert not gfsh.exists(res['gridfs_id'])
        assert not self.db['history_object'].find_one({'_id': res['history_object_id']})
        assert not self.db['elog'].find_one({'_id': res['elog_id']})
        assert self.db['elog'].count_documents({'wf_TimeSeries_id': ts['_id']}) == 0

        # file delete(not remove_unreferenced_files, clear_history, clear_elog)
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='file', dir='./python/tests/data/', dfile='test_db_output_1', exclude_keys=['extra2'])
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        
        assert save_res_code == 0
        self.db.delete_data(ts['_id'], 'TimeSeries')
        assert not self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert not self.db['history_object'].find_one({'_id': res['history_object_id']})
        assert not self.db['elog'].find_one({'_id': res['elog_id']})
        assert self.db['elog'].count_documents({'wf_TimeSeries_id': ts['_id']}) == 0
        # file still exists
        fname = os.path.join(res['dir'], res['dfile'])
        assert os.path.exists(fname)

        # file delete(remove_unreferenced_files, clear_history, clear_elog), with 2 wf doc using same file dir/dfile
        ts = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        logging_helper.info(ts2, 'deepcopy', '1')
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='file', dir='./python/tests/data/', dfile='test_db_output_1', exclude_keys=['extra2'])
        save_res_code2 = self.db.save_data(ts2, mode='promiscuous', storage_mode='file', dir='./python/tests/data/', dfile='test_db_output_1', exclude_keys=['extra2'])
        
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert save_res_code == 0
        self.db.delete_data(ts['_id'], 'TimeSeries', remove_unreferenced_files=True)
        assert not self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert not self.db['history_object'].find_one({'_id': res['history_object_id']})
        assert not self.db['elog'].find_one({'_id': res['elog_id']})
        assert self.db['elog'].count_documents({'wf_TimeSeries_id': ts['_id']}) == 0
        # file still exists, because another wf doc is using it
        fname = os.path.join(res['dir'], res['dfile'])
        assert os.path.exists(fname)

        res2 = self.db['wf_TimeSeries'].find_one({'_id': ts2['_id']})
        assert save_res_code2 == 0
        self.db.delete_data(ts2['_id'], 'TimeSeries', remove_unreferenced_files=True)
        assert not self.db['wf_TimeSeries'].find_one({'_id': ts2['_id']})
        assert not self.db['history_object'].find_one({'_id': res2['history_object_id']})
        assert not self.db['elog'].find_one({'_id': res2['elog_id']})
        assert self.db['elog'].count_documents({'wf_TimeSeries_id': ts2['_id']}) == 0
        # file not exists
        fname = os.path.join(res2['dir'], res2['dfile'])
        assert not os.path.exists(fname)

    def test_clean_collection(self):
        # clear all the wf collection documents
        self.db['wf_TimeSeries'].delete_many({})

        # test non exist document in the database
        fixed_cnt = self.db.clean_collection('wf_TimeSeries', query={'_id': ObjectId()})
        assert not fixed_cnt

        # test fixed_out
        self.db.database_schema.set_default('wf_TimeSeries', 'wf')
        ts1 = copy.deepcopy(self.test_ts)
        ts1['starttime_shift'] = 1.0
        ts2 = copy.deepcopy(self.test_ts)
        ts2['starttime_shift'] = 1.0
        logging_helper.info(ts1, 'deepcopy', '1')
        logging_helper.info(ts2, 'deepcopy', '1')
        ts1['npts'] = '123'
        ts2['delta'] = '12'
        ts2['starttime'] = '123'

        save_res_code = self.db.save_data(ts1, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        save_res_code = self.db.save_data(ts2, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        
        fixed_cnt = self.db.clean_collection('wf_TimeSeries')
        assert fixed_cnt == {'npts':1, 'delta':1}

    def test_clean(self, capfd):
        # clear all the wf collection documents
        self.db['wf_TimeSeries'].delete_many({})

        self.db.database_schema.set_default('wf_TimeSeries', 'wf')
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')

        # invalid parameters
        with pytest.raises(MsPASSError, match="verbose_keys should be a list , but <class 'str'> is requested."):
            self.db.clean(ObjectId(), verbose_keys="123")
        with pytest.raises(MsPASSError, match="rename_undefined should be a dict , but <class 'str'> is requested."):
            self.db.clean(ObjectId(), rename_undefined="123")
        with pytest.raises(MsPASSError, match="check_xref should be a list , but <class 'str'> is requested."):
            self.db.clean(ObjectId(), check_xref="123")

        # erase a required field in TimeSeries
        ts.erase('npts')
        ts['starttime_shift'] = 1.0
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        assert ts.live
        
        # test nonexist document
        nonexist_id = ObjectId()
        fixes_cnt = self.db.clean(nonexist_id, verbose=True)
        assert not fixes_cnt
        out, err = capfd.readouterr()
        assert out == "collection wf_TimeSeries document _id: {}, is not found\n".format(nonexist_id)

        # test verbose_keys and delete required fields missing document if delete_missing_required is True
        fixes_cnt = self.db.clean(ts['_id'], verbose_keys=['delta'], verbose=True, delete_missing_required=True)
        assert len(fixes_cnt) == 0
        # test if it is deleted
        assert not self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        out, err = capfd.readouterr()
        assert out == "collection wf_TimeSeries document _id: {}, delta: {}, required attribute: npts are missing. the document is deleted.\n".format(ts['_id'], ts['delta'])

        # test check_xref and delete required xref_keys missing document
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts['starttime_shift'] = 1.0
        ts.erase('site_id')
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        fixes_cnt = self.db.clean(ts['_id'], verbose=True, check_xref=['site_id'], delete_missing_xref=True)
        assert len(fixes_cnt) == 0
        assert not self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        out, err = capfd.readouterr()
        assert out == "collection wf_TimeSeries document _id: {}, required xref keys: site_id are missing. the document is deleted.\n".format(ts['_id'])

        # test conversion success
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        # npts has type str, should convert to int
        ts['npts'] = "123"
        ts['starttime_shift'] = 1.0
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        fixes_cnt = self.db.clean(ts['_id'], verbose=True)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res
        assert res['_id'] == ts['_id']
        assert 'npts' in res and res['npts'] == 123
        assert len(fixes_cnt) == 1
        assert fixes_cnt == {"npts": 1}
        out, err = capfd.readouterr()
        assert out == "collection wf_TimeSeries document _id: {}, attribute npts conversion from 123 to <class 'int'> is done.\n".format(ts['_id'])

        # test conversion fail
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        # npts has type str, but unable to convert to int
        ts['npts'] = "xyz"
        ts['starttime_shift'] = 1.0
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        fixes_cnt = self.db.clean(ts['_id'], verbose=True)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res
        assert 'npts' not in res
        # can not be fixed
        assert len(fixes_cnt) == 0
        out, err = capfd.readouterr()
        assert out == "collection wf_TimeSeries document _id: {}, attribute npts conversion from xyz to <class 'int'> cannot be done.\n".format(ts['_id'])

        # test removing aliases
        test_source_id = ObjectId()
        self.db['source'].insert_one({'_id': test_source_id, 'EVLA': 1.2, 'lon': 1.2, 'time': datetime.utcnow().timestamp(),
                                      'depth': 3.1, 'MAG': 1.0})
        fixes_cnt = self.db.clean(test_source_id, collection='source', verbose=True)
        res = self.db['source'].find_one({'_id': test_source_id})
        assert res
        assert 'EVLA' not in res
        assert 'MAG' not in res
        assert 'lat' in res
        assert 'magnitude' in res
        assert len(fixes_cnt) == 0
        self.db['source'].delete_one({'_id': test_source_id})
        out, err = capfd.readouterr()
        assert not out

        # test undefined key-value pair and delete_undefined
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts['starttime_shift'] = 1.0
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res
        assert 'extra1' in res
        fixes_cnt = self.db.clean(ts['_id'], verbose=True, delete_undefined=True)
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res
        assert 'extra1' not in res
        assert len(fixes_cnt) == 0
        out, err = capfd.readouterr()
        assert not out

        # test rename attributes
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts['starttime_shift'] = 1.0
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res
        assert 'extra1' in res
        val = res['extra1']
        fixes_cnt = self.db.clean(ts['_id'], verbose=True, rename_undefined={'extra1': 'rename_extra'})
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res
        assert 'extra1' not in res
        assert 'rename_extra' in res
        assert res['rename_extra'] == val
        assert len(fixes_cnt) == 0
        out, err = capfd.readouterr()
        assert not out

    def test_verify(self):
        # clear all documents
        self.db['wf_TimeSeries'].delete_many({})
        self.db.database_schema.set_default('wf_TimeSeries', 'wf')
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts['starttime_shift'] = 1.0
        # xref_key doc not found
        ts['site_id'] = ObjectId()
        # undefined required key
        ts.erase('npts')
        # mismatch type
        ts['delta'] = '123'
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert not 'npts' in res
        assert res['delta'] == '123'

        # test doc that does not exist
        problematic_keys = self.db.verify(ObjectId(), 'wf_TimeSeries', tests=['xref', 'type', 'undefined'])
        assert not problematic_keys

        # test xref, undefined and type
        problematic_keys = self.db.verify(ts['_id'], 'wf_TimeSeries', tests=['xref', 'type', 'undefined'])
        assert len(problematic_keys) == 3
        assert 'site_id' in problematic_keys and problematic_keys['site_id'] == 'xref'
        assert 'npts' in problematic_keys and problematic_keys['npts'] == 'undefined'
        assert 'delta' in problematic_keys and problematic_keys['delta'] == 'type'

    def test_check_xref_key(self):
        bad_xref_key_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_xref_key_ts, 'deepcopy', '1')
        bad_xref_key_ts['site_id'] = ObjectId()
        bad_wf_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_wf_ts, 'deepcopy', '1')

        save_res_code = self.db.save_data(bad_xref_key_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        save_res_code = self.db.save_data(bad_wf_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2','site_id'])
        assert save_res_code == 0

        bad_xref_key_doc = self.db['wf_TimeSeries'].find_one({'_id': bad_xref_key_ts['_id']})
        bad_wf_doc = self.db['wf_TimeSeries'].find_one({'_id': bad_wf_ts['_id']})

        # if xref_key is not defind -> not checking
        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(bad_xref_key_doc, 'wf_TimeSeries', 'xxx_id')
        assert not is_bad_xref_key
        assert not is_bad_wf

        # if xref_key is not a xref_key -> not checking
        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(bad_xref_key_doc, 'wf_TimeSeries', 'npts')
        assert not is_bad_xref_key
        assert not is_bad_wf
        # aliases
        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(bad_xref_key_doc, 'wf_TimeSeries', 'dt')
        assert not is_bad_xref_key
        assert not is_bad_wf

        # can't find normalized document
        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(bad_xref_key_doc, 'wf_TimeSeries', 'site_id')
        assert is_bad_xref_key
        assert not is_bad_wf

        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(bad_wf_doc, 'wf_TimeSeries', 'site_id')
        assert not is_bad_xref_key
        assert is_bad_wf

    def test_check_undefined_keys(self):
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts.erase('npts')

        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2', 'starttime'])
        assert save_res_code == 0
        self.db['wf_TimeSeries'].update_one({'_id': ts['_id']}, {'$set': {'t0':1.0}})
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert 'starttime' not in res
        assert 'npts' not in res
        assert 't0' in res

        undefined_keys = self.db._check_undefined_keys(res, 'wf_TimeSeries')
        assert len(undefined_keys) == 2
        assert 'npts' in undefined_keys
        assert 'starttime_shift' in undefined_keys

    def test_check_mismatch_key(self):
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts['npts'] = 'xyz'

        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2', 'starttime'])
        assert save_res_code == 0
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})

        assert self.db._check_mismatch_key(res, 'wf_TimeSeries', 'npts')
        assert self.db._check_mismatch_key(res, 'wf_TimeSeries', 'nsamp')
        assert not self.db._check_mismatch_key(res, 'wf_TimeSeries', 'xxx')
        assert not self.db._check_mismatch_key(res, 'wf_TimeSeries', 'delta')

    def test_delete_attributes(self):
        # clear all documents
        self.db['wf_TimeSeries'].delete_many({})
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts['starttime_shift'] = 1.0
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert 'delta' in res and 'sampling_rate' in res and 'starttime' in res
        counts = self.db._delete_attributes('wf_TimeSeries', ['delta', 'sampling_rate', 'starttime'])
        assert len(counts) == 3 and counts == {'delta':1,'sampling_rate':1,'starttime':1}
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert not 'delta' in res and not 'sampling_rate' in res and not 'starttime' in res
    
    def test_rename_attributes(self):
        # clear all documents
        self.db['wf_TimeSeries'].delete_many({})
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts['starttime_shift'] = 1.0
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert 'delta' in res and 'sampling_rate' in res and 'starttime' in res
        delta_val = res['delta']
        sampling_rate_val = res['sampling_rate']
        starttime_val = res['starttime']
        counts = self.db._rename_attributes('wf_TimeSeries', {'delta':'dt', 'sampling_rate':'sr', 'starttime':'st'})
        assert len(counts) == 3 and counts == {'delta':1,'sampling_rate':1,'starttime':1}
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert not 'delta' in res and not 'sampling_rate' in res and not 'starttime' in res
        assert 'dt' in res and 'sr' in res and 'st' in res
        assert res['dt'] == delta_val and res['sr'] == sampling_rate_val and res['st'] == starttime_val

    def test_fix_attribute_types(self):
        # clear all documents
        self.db['wf_TimeSeries'].delete_many({})
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, 'deepcopy', '1')
        ts['npts'] = 'xyz'
        ts['delta'] = '123'
        ts['sampling_rate'] = '123'
        save_res_code = self.db.save_data(ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res['npts'] == 'xyz' and res['delta'] == '123' and res['sampling_rate'] == '123'
        counts = self.db._fix_attribute_types('wf_TimeSeries')
        assert len(counts) == 2 and counts == {'delta':1,'sampling_rate':1}
        res = self.db['wf_TimeSeries'].find_one({'_id': ts['_id']})
        assert res['npts'] == 'xyz' and res['delta'] == 123.0 and res['sampling_rate'] == 123.0

    def test_check_links(self):
        # clear all documents
        self.db['wf_TimeSeries'].delete_many({})
        missing_site_id_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(missing_site_id_ts, 'deepcopy', '1')
        missing_site_id_ts.erase('site_id')

        bad_site_id_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_site_id_ts, 'deepcopy', '1')
        bad_site_id_ts['site_id'] = ObjectId()

        bad_source_id_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_source_id_ts, 'deepcopy', '1')
        bad_source_id_ts['source_id'] = ObjectId()

        bad_channel_id_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_channel_id_ts, 'deepcopy', '1')
        bad_channel_id_ts['channel_id'] = ObjectId()

        save_res_code = self.db.save_data(missing_site_id_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        save_res_code = self.db.save_data(bad_site_id_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        save_res_code = self.db.save_data(bad_source_id_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        save_res_code = self.db.save_data(bad_channel_id_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0

        # undefined collection name
        with pytest.raises(MsPASSError, match='check_links:  collection xxx is not defined in database schema'):
            self.db._check_links(collection='xxx')

        # undefined xref keys
        with pytest.raises(MsPASSError, match='check_links:  illegal value for normalize arg=npts'):
            self.db._check_links(xref_key='npts', collection='wf')

        # no documents found
        wfquery={'_id': ObjectId()}
        with pytest.raises(MsPASSError, match=re.escape('checklinks:  wf_TimeSeries collection has no data matching query={}'.format(str(wfquery)))):
            self.db._check_links(collection='wf', wfquery=wfquery)

        # check with default, all xref keys
        (bad_id_list, missing_id_list) = self.db._check_links(collection='wf')
        assert len(bad_id_list) == 3
        assert set(bad_id_list) == set([bad_site_id_ts['_id'], bad_source_id_ts['_id'], bad_channel_id_ts['_id']])
        assert len(missing_id_list) == 1
        assert missing_id_list == [missing_site_id_ts['_id']]

        # check with a single xref key
        (bad_id_list, missing_id_list) = self.db._check_links(xref_key='site_id', collection='wf')
        assert len(bad_id_list) == 1
        assert bad_id_list == [bad_site_id_ts['_id']]
        assert len(missing_id_list) == 1
        assert missing_id_list == [missing_site_id_ts['_id']]

        # check with a user specified xref keys
        (bad_id_list, missing_id_list) = self.db._check_links(xref_key=['site_id','source_id'], collection='wf')
        assert len(bad_id_list) == 2
        assert set(bad_id_list) == set([bad_site_id_ts['_id'], bad_source_id_ts['_id']])
        assert len(missing_id_list) == 1
        assert missing_id_list == [missing_site_id_ts['_id']]

    def test_check_attribute_types(self):
        # clear all documents
        self.db['wf_TimeSeries'].delete_many({})
        bad_type_docs_ts = copy.deepcopy(self.test_ts)
        undefined_key_docs_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_type_docs_ts, 'deepcopy', '1')
        logging_helper.info(undefined_key_docs_ts, 'deepcopy', '1')
        bad_type_docs_ts['npts'] = 'xyz'

        save_res_code = self.db.save_data(bad_type_docs_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        save_res_code = self.db.save_data(undefined_key_docs_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0

        # test empty matched documents
        query_dict = {'_id': ObjectId()}
        with pytest.raises(MsPASSError, match=re.escape("check_attribute_types:  query={} yields zero matching documents".format(str(query_dict)))):
            (bad_type_docs, undefined_key_docs) = self.db._check_attribute_types(collection='wf_TimeSeries', query=query_dict)
        
        # test bad_type_docs and undefined_key_docs
        (bad_type_docs, undefined_key_docs) = self.db._check_attribute_types(collection='wf_TimeSeries')
        assert len(bad_type_docs) == 1
        assert bad_type_docs == {bad_type_docs_ts['_id']: {'npts': 'xyz'}}
        assert len(undefined_key_docs) == 2
        assert undefined_key_docs == {bad_type_docs_ts['_id']: {'extra1': 'extra1'}, undefined_key_docs_ts['_id']: {'extra1': 'extra1'}}

    def test_check_required(self):
        # clear all documents
        self.db['wf_TimeSeries'].delete_many({})
        wrong_types_ts = copy.deepcopy(self.test_ts)
        undef_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(wrong_types_ts, 'deepcopy', '1')
        logging_helper.info(undef_ts, 'deepcopy', '1')
        wrong_types_ts['npts'] = 'xyz'

        save_res_code = self.db.save_data(wrong_types_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0
        save_res_code = self.db.save_data(undef_ts, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        assert save_res_code == 0

        # test empty matched documents
        query_dict = {'_id': ObjectId()}
        with pytest.raises(MsPASSError, match=re.escape("check_required:  query={} yields zero matching documents".format(str(query_dict)))):
            (wrong_types, undef) = self.db._check_required(collection='wf_TimeSeries', keys=['npts','delta','starttime'], query=query_dict)
        
        # test undefined keys
        with pytest.raises(MsPASSError, match="check_required:  schema has no definition for key=undefined_key"):
            (wrong_types, undef) = self.db._check_required(collection='wf_TimeSeries', keys=['undefined_key'])

        # test bad_type_docs and undefined_key_docs
        (wrong_types, undef) = self.db._check_required(collection='wf_TimeSeries', keys=['npts','delta','starttime_shift'])
        assert len(wrong_types) == 1
        assert wrong_types == {wrong_types_ts['_id']: {'npts': 'xyz'}}
        assert len(undef) == 2
        assert undef == {wrong_types_ts['_id']: ['starttime_shift'], undef_ts['_id']: ['starttime_shift']}

    def test_update_ensemble_metadata(self):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, 'deepcopy', '1')
        logging_helper.info(ts2, 'deepcopy', '1')
        logging_helper.info(ts3, 'deepcopy', '1')
        self.db.save_data(ts1, storage_mode='gridfs')
        self.db.save_data(ts2, storage_mode='gridfs')
        self.db.save_data(ts3, storage_mode='gridfs')

        time = datetime.utcnow().timestamp()
        ts1.t0 = time
        ts1['tst'] = time
        ts2.t0 = time
        ts3.t0 = time
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)

        self.db.update_ensemble_metadata(ts_ensemble, mode='promiscuous', exclude_objects=[2])
        res = self.db['wf_TimeSeries'].find_one({'_id': ts1['_id']})
        assert res['starttime'] == time
        res = self.db['wf_TimeSeries'].find_one({'_id': ts2['_id']})
        assert res['starttime'] == time
        res = self.db['wf_TimeSeries'].find_one({'_id': ts3['_id']})
        assert res['starttime'] != time

        time_new = datetime.utcnow().timestamp()
        ts_ensemble.member[0]['tst'] = time + 1
        ts_ensemble.member[0].t0 = time_new
        self.db.update_ensemble_metadata(ts_ensemble, mode='promiscuous', exclude_keys=['tst'])
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
        self.db.save_data(seis1, storage_mode='gridfs')
        self.db.save_data(seis2, storage_mode='gridfs')
        self.db.save_data(seis3, storage_mode='gridfs')
        time = datetime.utcnow().timestamp()
        seis1.t0 = time
        seis1['tst'] = time
        seis2.t0 = time
        seis3.t0 = time
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)

        self.db.update_ensemble_metadata(seis_ensemble, mode='promiscuous', exclude_objects=[2])
        res = self.db['wf_Seismogram'].find_one({'_id': seis1['_id']})
        assert res['starttime'] == time
        res = self.db['wf_Seismogram'].find_one({'_id': seis2['_id']})
        assert res['starttime'] == time
        res = self.db['wf_Seismogram'].find_one({'_id': seis3['_id']})
        assert res['starttime'] != time

        time_new = datetime.utcnow().timestamp()
        seis_ensemble.member[0]['tst'] = time + 1
        seis_ensemble.member[0].t0 = time_new
        self.db.update_ensemble_metadata(seis_ensemble, mode='promiscuous', exclude_keys=['tst'])
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
        self.db.save_ensemble_data(ts_ensemble, mode="promiscuous", storage_mode='file', dfile_list=dfile_list, dir_list=dir_list, exclude_objects=[1])
        self.db.database_schema.set_default('wf_TimeSeries', 'wf')
        res = self.db.read_data(ts_ensemble.member[0]['_id'], mode='promiscuous', normalize=['site', 'source', 'channel'])
        assert np.isclose(ts_ensemble.member[0].data, res.data).all()
        res = self.db.read_data(ts_ensemble.member[2]['_id'], mode='promiscuous', normalize=['site', 'source', 'channel'])
        assert np.isclose(ts_ensemble.member[2].data, res.data).all()
        assert '_id' not in ts_ensemble.member[1]

        self.db.save_ensemble_data(ts_ensemble, mode="promiscuous", storage_mode='gridfs', exclude_objects=[1])
        res = self.db.read_data(ts_ensemble.member[0]['_id'], mode='promiscuous', normalize=['site', 'source', 'channel'])
        assert np.isclose(ts_ensemble.member[0].data, res.data).all()
        assert '_id' not in ts_ensemble.member[1]
        res = self.db.read_data(ts_ensemble.member[2]['_id'], mode='promiscuous', normalize=['site', 'source', 'channel'])
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
        self.db.save_ensemble_data(seis_ensemble, mode="promiscuous", storage_mode='file', dfile_list=dfile_list, dir_list=dir_list, exclude_objects=[1])
        self.db.database_schema.set_default('wf_Seismogram', 'wf')
        res = self.db.read_data(seis_ensemble.member[0]['_id'], mode='promiscuous', normalize=['site', 'source'])
        assert np.isclose(seis_ensemble.member[0].data, res.data).all()
        res = self.db.read_data(seis_ensemble.member[2]['_id'], mode='promiscuous', normalize=['site', 'source'])
        assert np.isclose(seis_ensemble.member[2].data, res.data).all()
        assert '_id' not in seis_ensemble.member[1]

        self.db.save_ensemble_data(seis_ensemble, mode="promiscuous", storage_mode='gridfs', exclude_objects=[1])
        res = self.db.read_data(seis_ensemble.member[0]['_id'], mode='promiscuous', normalize=['site', 'source'])
        assert np.isclose(seis_ensemble.member[0].data, res.data).all()
        assert '_id' not in seis_ensemble.member[1]
        res = self.db.read_data(seis_ensemble.member[2]['_id'], mode='promiscuous', normalize=['site', 'source'])
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
        self.db.save_ensemble_data(ts_ensemble, mode="promiscuous", storage_mode='gridfs')
        res = self.db.read_ensemble_data([ts_ensemble.member[0]['_id'], ts_ensemble.member[1]['_id'],
                                          ts_ensemble.member[2]['_id']], mode='cautious', normalize=['source','site','channel'])
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
        self.db.save_ensemble_data(seis_ensemble, mode="promiscuous", storage_mode='gridfs')
        res = self.db.read_ensemble_data([seis_ensemble.member[0]['_id'], seis_ensemble.member[1]['_id'],
                                          seis_ensemble.member[2]['_id']])
        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, seis_ensemble.member[i].data).all()

    def test_get_response(self):
        inv = obspy.read_inventory('python/tests/data/TA.035A.xml')
        net = 'TA'
        sta = '035A'
        loc = ''
        time = 1263254400.0+100.0
        for chan in ['BHE', 'BHN', 'BHZ']:
            r = self.db.get_response(net, sta, chan, loc, time)
            r0 = inv.get_response("TA.035A..BHE", time)
            assert r == r0
        with pytest.raises(MsPASSError, match='missing one of required arguments'):
            self.db.get_response()
        assert self.db.get_response(net='TA', sta='036A', chan='BHE', time=time) is None

    def teardown_class(self):
        try:
            os.remove('python/tests/data/test_db_output')
        except OSError:
            pass
        client = DBClient('localhost')
        client.drop_database('dbtest')

    def test_load_source_site_channel_metadata(self):
        ts = copy.deepcopy(self.test_ts)

        # insert arbitrary site/channel/source data in db
        site_id = ObjectId()
        channel_id = ObjectId()
        source_id = ObjectId()
        ts['site_id'] = site_id
        ts['channel_id'] = channel_id
        ts['source_id'] = source_id
        self.db['site'].insert_one({'_id': site_id, 'net': 'net10', 'sta': 'sta10', 'loc': 'loc10', 'lat': 5.0, 'lon': 5.0,
                            'elev': 5.0, 'starttime': 1.0, 'endtime': 1.0})
        self.db['channel'].insert_one({'_id': channel_id, 'net': 'net10', 'sta': 'sta10', 'loc': 'loc10', 'chan': 'chan10',
                                'lat': 5.0, 'lon': 5.0, 'elev': 5.0, 'starttime': 1.0, 'endtime': 1.0, 'edepth': 5.0,
                                'vang': 5.0, 'hang': 5.0})
        self.db['source'].insert_one({'_id': source_id, 'lat': 5.0, 'lon': 5.0, 'time': 1.0, 'depth': 5.0, 'magnitude': 5.0})

        # load site/channel/source data into the mspass object
        self.db.load_site_metadata(ts, exclude_keys=['site_elev'], include_undefined=False)
        assert ts['net'] == 'net10'
        assert ts['sta'] == 'sta10'
        assert ts['site_lat'] == 5.0
        assert ts['site_lon'] == 5.0
        assert 'site_elev' not in ts
        assert ts['site_starttime'] == 1.0
        assert ts['site_endtime'] == 1.0

        self.db.load_source_metadata(ts, exclude_keys=['source_magnitude'], include_undefined=False)
        assert ts['source_lat'] == 5.0
        assert ts['source_lon'] == 5.0
        assert ts['source_depth'] == 5.0
        assert ts['source_time'] == 1.0
        assert 'source_magnitude' not in ts

        self.db.load_channel_metadata(ts, exclude_keys=['channel_edepth'], include_undefined=False)
        assert ts['chan'] == 'chan10'
        assert ts['loc'] == 'loc10'
        assert ts['channel_hang'] == 5.0
        assert ts['channel_vang'] == 5.0
        assert ts['channel_lat'] == 5.0
        assert ts['channel_lon'] == 5.0
        assert ts['channel_elev'] == 5.0
        assert 'channel_edepth' not in ts
        assert ts['channel_starttime'] == 1.0
        assert ts['channel_endtime'] == 1.0
    

def test_read_distributed_data(spark_context):
    client = DBClient('localhost')
    client.drop_database('mspasspy_test_db')

    test_ts = get_live_timeseries()

    client = DBClient('localhost')
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
    ts_list_rdd.foreach(lambda d, database=db: database.save_data(d, storage_mode='gridfs'))
    cursors = db['wf_TimeSeries'].find({})

    spark_list = read_distributed_data(db, cursors, mode='cautious', normalize=['source','site','channel'], format='spark', spark_context=spark_context)
    list = spark_list.collect()
    assert len(list) == 3
    for l in list:
        assert l
        assert np.isclose(l.data, test_ts.data).all()

    client = DBClient('localhost')
    client.drop_database('mspasspy_test_db')


def test_read_distributed_data_dask():
    client = DBClient('localhost')
    client.drop_database('mspasspy_test_db')

    test_ts = get_live_timeseries()

    client = DBClient('localhost')
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
    ts_list_dbg.map(db.save_data, storage_mode='gridfs').compute()
    cursors = db['wf_TimeSeries'].find({})

    dask_list = read_distributed_data(db, cursors, mode='cautious', normalize=['source','site','channel'])
    list = dask_list.compute()
    assert len(list) == 3
    for l in list:
        assert l
        assert np.isclose(l.data, test_ts.data).all()

    client = DBClient('localhost')
    client.drop_database('mspasspy_test_db')

if __name__ == '__main__':
    pass
