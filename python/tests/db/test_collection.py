import pickle
from datetime import datetime
from bson.objectid import ObjectId

from mspasspy.db.schema import MetadataSchema

from mspasspy.db.client import DBClient
from mspasspy.db.database import Database

# from mspasspy.db.collection import Collection


class TestDatabase:
    def setup_class(self):
        client = DBClient("localhost")
        self.db = Database(client, "dbtest")
        self.db2 = Database(client, "dbtest")
        self.metadata_def = MetadataSchema()
        # clean up the database locally
        for col_name in self.db.list_collection_names():
            self.db[col_name].delete_many({})

        site_id = ObjectId()
        channel_id = ObjectId()
        source_id = ObjectId()
        self.db["site"].insert_one(
            {
                "_id": site_id,
                "net": "net1",
                "sta": "sta1",
                "loc": "loc",
                "lat": 1.0,
                "lon": 1.0,
                "elev": 2.0,
                "starttime": datetime.utcnow().timestamp(),
                "endtime": datetime.utcnow().timestamp(),
            }
        )
        self.db["channel"].insert_one(
            {
                "_id": channel_id,
                "net": "net1",
                "sta": "sta1",
                "loc": "loc1",
                "chan": "chan",
                "lat": 1.1,
                "lon": 1.1,
                "elev": 2.1,
                "starttime": datetime.utcnow().timestamp(),
                "endtime": datetime.utcnow().timestamp(),
                "edepth": 3.0,
                "vang": 1.0,
                "hang": 1.0,
            }
        )
        self.db["source"].insert_one(
            {
                "_id": source_id,
                "lat": 1.2,
                "lon": 1.2,
                "time": datetime.utcnow().timestamp(),
                "depth": 3.1,
                "magnitude": 1.0,
            }
        )

    def teardown_class(self):
        client = DBClient("localhost")
        client.drop_database("dbtest")

    def test_collection(self):
        col1 = self.db.source
        col2 = self.db["source"]
        assert col1 == col2

        col11 = pickle.loads(pickle.dumps(col1))
        col21 = pickle.loads(pickle.dumps(col2))
        assert col11 == col21

        col11.test2.insert_one(
            {
                "lat": 1.9,
                "lon": 1.2,
                "time": datetime.utcnow().timestamp(),
            }
        )
        assert col11["test2"].find_one()["lat"] == 1.9
