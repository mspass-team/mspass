import pymongo
import pytest

from mspasspy.db.client import DBClient


class TestDBClient:
    def setup_class(self):
        self.c1 = DBClient("mongodb://localhost/my_database")
        self.c2 = DBClient("localhost")

    def test_init(self):
        assert self.c1._DBClient__default_database_name == "my_database"

    def test_getitem(self):
        assert self.c1["my_database"].name == "my_database"
        assert self.c2["my_db"].name == "my_db"

    def test_get_default_database(self):
        assert self.c1.get_default_database().name == "my_database"
        with pytest.raises(
            pymongo.errors.ConfigurationError, match="No default database"
        ):
            self.c2.get_default_database()
        db1 = self.c1.get_default_database(schema="mspass_lite.yaml")
        with pytest.raises(
            KeyError, match="site"
        ):
            db1.database_schema._attr_dict['site']

    def test_get_database(self):
        assert self.c1.get_database().name == "my_database"
        assert self.c2.get_database("my_db").name == "my_db"
        with pytest.raises(
            pymongo.errors.ConfigurationError, match="No default database"
        ):
            self.c2.get_database()
        db1 = self.c1.get_database(schema="mspass_lite.yaml")
        with pytest.raises(
            KeyError, match="site"
        ):
            db1.database_schema._attr_dict['site']
