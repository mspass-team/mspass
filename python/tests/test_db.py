import pytest
import mongomock
from mongomock.gridfs import enable_gridfs_integration
import numpy as np
import pymongo
import gridfs
from bson.objectid import ObjectId

from mspasspy.ccore import (Seismogram,
                            dmatrix)
from mspasspy.db import (Client,
                         Database)

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

        ts_size = 255    
        sampling_rate = 20.0
        s1 = Seismogram()
        s1.u = dmatrix(3, ts_size)
        for i in range(3):
            for j in range(ts_size):
                s1.u[i, j] = np.random.rand()
        s1.live = True
        s1.dt = 1/sampling_rate
        s1.t0 = 0
        s1.ns = ts_size
        s1.put('net', 'IU')

        s2 = Seismogram(s1)
        s2.put('net', 'AK')

        self.ts_size = ts_size
        self.sampling_rate = sampling_rate
        self.s1 = s1
        self.s2 = s2

    def test_load3C(self):
        pass

    def test_save3C(self):
        # FIXME: When something threw an unexpected exception there is no garantee that 'wf_id' is already defined.
        assert self.db.save3C(self.s1, mmode='save', smode='gridfs') == 0
        # FIXME: '_id' should not be used in the find method below. Need to fix the schema.
        assert self.db.wf.count_documents({"_id": ObjectId(self.s1.get('wf_id'))}) == 1
        doc1 = self.db.wf.find({"_id": ObjectId(self.s1.get('wf_id'))})[0]
        assert doc1['npts'] == self.ts_size
        assert doc1['delta'] == 1/self.sampling_rate
        with pytest.raises(KeyError):
            doc1['net']
        
        assert self.db.save3C(self.s2, mmode='saveall', smode='gridfs') == 0
        assert self.db.wf.count_documents({"_id": ObjectId(self.s2.get('wf_id'))}) == 1
        doc2 = self.db.wf.find({"_id": ObjectId(self.s2.get('wf_id'))})[0]
        assert doc2['npts'] == self.ts_size
        assert doc2['delta'] == 1/self.sampling_rate
        assert doc2['net'] == self.s2.get('net')
