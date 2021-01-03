import pymongo
import pytest

from mspasspy.db.client import Client

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
        with pytest.raises(pymongo.errors.ConfigurationError, match='No default database'):
            self.c2.get_default_database()

    def test_get_database(self):
        assert self.c1.get_database().name == 'my_database'
        assert self.c2.get_database('my_db').name == 'my_db'
        with pytest.raises(pymongo.errors.ConfigurationError, match='No default database'):
            self.c2.get_database()