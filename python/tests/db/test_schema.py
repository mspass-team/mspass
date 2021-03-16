import pytest
from bson.objectid import ObjectId

from mspasspy.ccore.utility import MsPASSError
from mspasspy.ccore.seismic import TimeSeries, Seismogram

from mspasspy.db.schema import DatabaseSchema, MetadataSchema, DBSchemaDefinition, MDSchemaDefinition


class TestSchema():
    def setup_class(self):
        self.mdschema = MetadataSchema()
        self.dbschema = DatabaseSchema()
        self.dbschema.wf_TimeSeries.add('test', {'type': 'bool', 'aliases':['test1']})

    def test_init(self):
        dbschema = DatabaseSchema("mspass_lite.yaml")
        with pytest.raises(AttributeError, match='no attribute'):
            dummy = dbschema.site
        with pytest.raises(MsPASSError, match='not defined'):
            dummy = dbschema['source']
        with pytest.raises(MsPASSError, match='Cannot open schema definition file'):
            dbschema = DatabaseSchema("dummy")

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
        # test for __contains__
        assert 'site' in self.dbschema
        assert 'dummy' not in self.dbschema
        assert '_raw' not in self.dbschema

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
        assert self.dbschema.wf_TimeSeries.constraint('test') == 'normal'
        self.dbschema.channel.add('test', {'type': 'int', 'constraint': 'required', 'reference':'test2'})
        assert self.dbschema.channel.constraint('test') == 'required'
        assert self.dbschema.channel.is_required('test')
        assert self.dbschema.channel.reference('test') == 'test2'
        self.mdschema.TimeSeries.add('test', {'type': 'int', 'constraint': 'xref_key', 'collection':'test2'})
        assert self.mdschema.TimeSeries.constraint('test') == 'xref_key'
        assert self.mdschema.TimeSeries.is_xref_key('test')
        assert self.mdschema.TimeSeries.collection('test') == 'test2'
        self.mdschema.Seismogram.add('test', {'type': 'int', 'collection':'test2'})
        assert self.mdschema.Seismogram.constraint('test') == 'normal'

    def test_add_remove_alias(self):
        self.dbschema.wf_TimeSeries.add_alias('test', 'test2')
        assert self.dbschema.wf_TimeSeries.aliases('test') == ['test1', 'test2']
        self.dbschema.wf_TimeSeries.remove_alias('test2')
        assert self.dbschema.wf_TimeSeries.aliases('test') == ['test1']

    def test_aliaes(self):
        assert self.dbschema.wf_TimeSeries.aliases('_id') is None

    def test_apply_aliases(self):
        ss = Seismogram()
        alias_dic = {'delta': 'd', 'npts': 'n', 'starttime': 's'}
        self.mdschema.Seismogram.apply_aliases(ss, alias_dic)
        assert not ss.is_defined('delta')
        assert not ss.is_defined('npts')
        assert not ss.is_defined('starttime')
        assert ss.is_defined('d')
        assert ss.is_defined('n')
        assert ss.is_defined('s')
        assert self.mdschema.Seismogram.unique_name('d') == 'delta'
        assert self.mdschema.Seismogram.unique_name('n') == 'npts'
        assert self.mdschema.Seismogram.unique_name('s') == 'starttime'
        self.mdschema.Seismogram.clear_aliases(ss)
        assert ss.is_defined('delta')
        assert ss.is_defined('npts')
        assert ss.is_defined('starttime')
        self.mdschema.Seismogram.apply_aliases(ss, 'python/tests/data/alias.yaml')
        assert not ss.is_defined('delta')
        assert not ss.is_defined('npts')
        assert not ss.is_defined('starttime')
        assert ss.is_defined('dd')
        assert ss.is_defined('nn')
        assert ss.is_defined('ss')
        with pytest.raises(MsPASSError, match='is not recognized'):
            self.mdschema.Seismogram.apply_aliases(ss, 123)

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

    def test_constraint(self):
        assert self.dbschema.wf_TimeSeries.constraint('_id') == 'required'

    def test_is_required(self):
        assert self.dbschema.wf_TimeSeries.is_required('_id')

    def test_is_required(self):
        assert self.dbschema.wf_TimeSeries.is_required('_id')

    def test_is_xref_key(self):
        assert self.dbschema.wf_TimeSeries.is_xref_key('site_id')

    def test_is_normal(self):
        assert self.dbschema.wf_TimeSeries.is_normal('calib')

    def test_is_optional(self):
        assert self.dbschema.wf_TimeSeries.is_optional('dir')

    def test_has_alias(self):
        assert not self.dbschema.wf_TimeSeries.has_alias('_id')
        assert self.dbschema.wf_TimeSeries.has_alias('test')

    def test_is_alias(self):
        assert not self.dbschema.wf_TimeSeries.is_alias('_id')
        assert self.dbschema.wf_TimeSeries.is_alias('test1')
        assert self.dbschema.site.is_alias('site_lat')
        assert not self.mdschema.TimeSeries.is_alias('site_lat')

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
        with pytest.raises(MsPASSError, match='not defined'):
            self.dbschema.channel.unique_name('test100')

    def test_DBSchemaDefinition_reference(self):
        assert self.dbschema.wf_TimeSeries.reference('site_id') == 'site'
        assert self.dbschema.wf_TimeSeries.reference('npts') == 'wf_TimeSeries'
        with pytest.raises(MsPASSError, match = 'not defined'):
            self.dbschema.wf_TimeSeries.reference('test100')

    def test_DBSchemaDefinition_data_type(self):
        assert self.dbschema.wf_TimeSeries.data_type() == TimeSeries
        assert self.dbschema.wf_Seismogram.data_type() == Seismogram
        assert self.dbschema.site.data_type() is None
        assert self.dbschema.source.data_type() is None

    def test_DBSchemaDefinition_required_keys(self):
        assert self.dbschema.wf_TimeSeries.required_keys() == ['_id','npts','delta','starttime','starttime_shift','utc_convertible','time_standard','storage_mode']
        assert self.dbschema.wf_Seismogram.required_keys() == ['_id','npts','delta','starttime','starttime_shift','utc_convertible','time_standard','storage_mode','tmatrix']
        assert self.dbschema.site.required_keys() == ['_id','lat','lon','elev']
        assert self.dbschema.source.required_keys() == ['_id','lat','lon','depth','time']

    def test_DBSchemaDefinition_xref_keys(self):
        assert self.dbschema.wf_TimeSeries.xref_keys() == ['site_id','channel_id','source_id','history_object_id','elog_id']
        assert self.dbschema.wf_Seismogram.xref_keys() == ['site_id','channel_id','source_id','history_object_id','elog_id']
        assert self.dbschema.site.xref_keys() == []
        assert self.dbschema.source.xref_keys() == []

    def test_MDSchemaDefinition_collection(self):
        assert self.mdschema.TimeSeries.collection('sta') == 'site'
        assert self.mdschema.TimeSeries.collection('starttime') == 'wf_TimeSeries'

    def test_MDSchemaDefinition_set_collection(self):
        self.mdschema.TimeSeries.set_collection('channel_id', 'dummy')
        assert self.mdschema.TimeSeries.collection('channel_id') == 'dummy'

        assert self.mdschema.TimeSeries.type('channel_id') == ObjectId
        self.mdschema.TimeSeries.set_collection('channel_id', 'wf_Seismogram', self.dbschema)
        assert self.mdschema.TimeSeries.collection('channel_id') == 'wf_Seismogram'
        assert self.mdschema.TimeSeries.type('channel_id') == list
        
        with pytest.raises(MsPASSError, match='not defined'):
            self.mdschema.TimeSeries.set_collection('test100','wf_Seismogram')

    def test_MDSchemaDefinition_swap_collection(self):
        self.mdschema.TimeSeries.swap_collection('wf_TimeSeries', 'dummy')
        assert self.mdschema.TimeSeries.collection('_id') == 'dummy'
        assert self.mdschema.TimeSeries.collection('calib') == 'dummy'

        alias = self.mdschema.TimeSeries.aliases('npts').copy()
        for k in alias:
            self.mdschema.TimeSeries.remove_alias(k)
        self.mdschema.TimeSeries.swap_collection('dummy', 'wf_TimeSeries', self.dbschema)
        assert self.mdschema.TimeSeries.collection('_id') == 'wf_TimeSeries'
        assert self.mdschema.TimeSeries.collection('calib') == 'wf_TimeSeries'
        assert self.mdschema.TimeSeries.aliases('npts') == alias

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
