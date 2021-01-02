import pytest
from bson.objectid import ObjectId

from mspasspy.ccore.utility import MsPASSError
from mspasspy.ccore.seismic import Seismogram

from mspasspy.db.schema import DatabaseSchema, MetadataSchema, DBSchemaDefinition, MDSchemaDefinition


class TestSchema():
    def setup_class(self):
        self.mdschema = MetadataSchema()
        self.dbschema = DatabaseSchema()
        self.dbschema.wf_TimeSeries.add('test', {'type': 'bool', 'aliases':['test1']})

    def test_item(self):
        def_site = self.dbschema.site
        def_channel = self.dbschema.channel
        self.dbschema['site'] = def_channel
        assert self.dbschema['site'] == self.dbschema.channel
        del(self.dbschema['site'])
        with pytest.raises(MsPASSError, match = 'not defined'):
            del(self.dbschema['site'])
        with pytest.raises(MsPASSError, match = 'not defined'):
            dummy = self.dbschema['site']
        with pytest.raises(MsPASSError, match = 'not a DBSchemaDefinition'):
            self.dbschema['site'] = 1
        self.dbschema['site'] = def_site
        assert self.dbschema['site'] == self.dbschema.site

    def test_DatabaseSchema_default(self):
        assert self.dbschema.default_name('wf') == 'wf_TimeSeries'
        assert self.dbschema.default('wf') == self.dbschema.wf_TimeSeries
        assert self.dbschema.default_name('site') == 'site'
        assert self.dbschema.default('site') == self.dbschema.site
        with pytest.raises(MsPASSError, match = 'no default defined'):
            dummy = self.dbschema.default_name('wff')
        with pytest.raises(MsPASSError, match = 'no default defined'):
            dummy = self.dbschema.default('wff')
        self.dbschema.set_default('wf_Seismogram')
        assert self.dbschema.default_name('wf') == 'wf_Seismogram'
        self.dbschema.set_default('wf_Seismogram', default='test')
        with pytest.raises(MsPASSError, match = 'not a defined collection'):
            self.dbschema.set_default('dummy')
        assert self.dbschema.default_name('test') == 'wf_Seismogram'
        self.dbschema.unset_default('test')
        with pytest.raises(MsPASSError, match = 'no default defined'):
            dummy = self.dbschema.default_name('test')
        
    def test_add(self):
        self.dbschema.wf_TimeSeries.add('test', {'type': 'boolean', 'aliases':['test1']})
        assert self.dbschema.wf_TimeSeries.type('test') == bool
        assert self.dbschema.wf_TimeSeries.aliases('test') == ['test1']

    def test_add_alias(self):
        self.dbschema.wf_TimeSeries.add_alias('test', 'test2')
        assert self.dbschema.wf_TimeSeries.aliases('test') == ['test1', 'test2']

    def test_aliaes(self):
        assert self.dbschema.wf_TimeSeries.aliases('_id') is None

    def test_clear_aliases(self):
        ss = Seismogram()
        ss.erase('starttime')
        assert not ss.is_defined('starttime')
        ss['t0'] = 0
        self.mdschema.Seismogram.clear_aliases(ss)
        assert not ss.is_defined('t0')
        assert ss.is_defined('starttime')
        
    def test_concept(self):
        assert self.dbschema.wf_TimeSeries.concept('_id') == 'ObjectId used to define a data object'
        
    def test_has_alias(self):
        assert not self.dbschema.wf_TimeSeries.has_alias('_id')
        assert self.dbschema.wf_TimeSeries.has_alias('test')
        
    def test_is_alias(self):
        assert not self.dbschema.wf_TimeSeries.is_alias('_id')
        assert self.dbschema.wf_TimeSeries.is_alias('test1')

    def test_is_defined(self):
        assert self.dbschema.wf_TimeSeries.is_defined('delta') is True
        assert self.mdschema.TimeSeries.is_defined('dt') is True

    def test_keys(self):
        for k in self.dbschema.wf_TimeSeries.keys():
            if k != 'test':
                assert k in self.dbschema.wf_Seismogram.keys() 

    def test_type(self):
        assert self.mdschema.TimeSeries.type('_id') == ObjectId
        assert self.mdschema.TimeSeries.type('npts') == type(1)
        assert self.mdschema.TimeSeries.type('delta') == type(1.0)
        assert self.mdschema.TimeSeries.type('time_standard') == type('1')
        assert self.mdschema.TimeSeries.type('time_standard') == type('1')
        assert self.mdschema.Seismogram.type('tmatrix') == type([1])
        assert self.dbschema.history_object.type('nodedata') == bytes

    def test_unique_name(self):
        assert self.dbschema.channel.unique_name('CMPAZ') == 'hang'
        assert self.mdschema.TimeSeries.unique_name('CMPINC') == 'channel_vang'
        assert self.dbschema.channel.unique_name('hang') == 'hang'
        assert self.mdschema.TimeSeries.unique_name('channel_vang') == 'channel_vang'
        with pytest.raises(MsPASSError, match = 'not defined'):
            self.dbschema.channel.unique_name('test100')

    def test_DBSchemaDefinition_reference(self):
        assert self.dbschema.wf_TimeSeries.reference('site_id') == 'site'
        assert self.dbschema.wf_TimeSeries.reference('npts') == 'wf_TimeSeries'
        with pytest.raises(MsPASSError, match = 'not defined'):
            self.dbschema.wf_TimeSeries.reference('test100')

    def test_MDSchemaDefinition_collection(self):
        assert self.mdschema.TimeSeries.collection('sta') == 'site'
        assert self.mdschema.TimeSeries.collection('starttime') == 'wf_TimeSeries'

    def test_MDSchemaDefinition_readonly(self):
        assert self.mdschema.TimeSeries.readonly('net')
        assert not self.mdschema.TimeSeries.writeable('net')
        assert self.mdschema.TimeSeries.writeable('delta')
        assert not self.mdschema.TimeSeries.readonly('delta')

        self.mdschema.TimeSeries.set_readonly('delta')
        self.mdschema.TimeSeries.set_writeable('net')
        assert not self.mdschema.TimeSeries.readonly('net')
        assert self.mdschema.TimeSeries.writeable('net')
        assert not self.mdschema.TimeSeries.writeable('delta')
        assert self.mdschema.TimeSeries.readonly('delta')
        
        with pytest.raises(MsPASSError, match = 'not defined'):
            self.mdschema.TimeSeries.set_readonly('test100')
        with pytest.raises(MsPASSError, match = 'not defined'):
            self.mdschema.TimeSeries.set_writeable('test100')

