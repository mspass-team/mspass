import os

import bson.json_util

from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.db.normalize import normalize_mseed


def restore_db(db, backup_db_dir):
    """Load the small normalization fixture database used by test_normalize."""
    for filename in os.listdir(backup_db_dir):
        jsonpath = os.path.join(backup_db_dir, filename)
        collection_name = filename.split(".")[0]
        with open(jsonpath, "rb") as jsonfile:
            rawdata = jsonfile.read()
            jsondata = bson.json_util.loads(rawdata)
            db[collection_name].insert_many(jsondata)


def test_normalize_mseed_updates_requested_waveform_collection():
    client = DBClient("localhost")
    dbname = "normalize_mseed_wf_col_test"
    client.drop_database(dbname)
    db = Database(client, dbname, db_schema="mspass.yaml", md_schema="mspass.yaml")

    try:
        restore_db(db, "./python/tests/data/db_dump")

        docs = list(db.wf_miniseed.find({}))
        assert docs
        for doc in docs:
            doc.pop("_id", None)
            doc.pop("channel_id", None)
            doc.pop("site_id", None)
        db.wf_custom.insert_many(docs)

        default_channel_ids = db.wf_miniseed.count_documents(
            {"channel_id": {"$exists": True}}
        )
        default_site_ids = db.wf_miniseed.count_documents({"site_id": {"$exists": True}})

        ret = normalize_mseed(db, wf_col="wf_custom")

        assert ret[0] == len(docs)
        assert ret[1] == db.wf_custom.count_documents(
            {"channel_id": {"$exists": True}}
        )
        assert ret[2] == db.wf_custom.count_documents({"site_id": {"$exists": True}})
        assert ret[1] > 0
        assert ret[2] > 0
        assert db.wf_miniseed.count_documents(
            {"channel_id": {"$exists": True}}
        ) == default_channel_ids
        assert db.wf_miniseed.count_documents(
            {"site_id": {"$exists": True}}
        ) == default_site_ids
    finally:
        client.drop_database(dbname)
