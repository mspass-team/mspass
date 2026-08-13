#!/usr/bin/env python3

import sys
import uuid

import gridfs

from mspasspy.ccore.seismic import TimeSeries
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.history import HistoryLogger


def main(uri):
    client = DBClient(uri, serverSelectionTimeoutMS=10000)
    server_version = client.server_info()["version"]
    if server_version != "8.0.29":
        raise RuntimeError(f"expected MongoDB 8.0.29, got {server_version}")

    database = Database(client, f"mspass_mongodb_8029_contract_{uuid.uuid4().hex}")
    try:
        database["crud"].insert_one({"_id": "record", "value": 1})
        database["crud"].update_one({"_id": "record"}, {"$set": {"value": 2}})
        if database["crud"].find_one({"_id": "record"})["value"] != 2:
            raise RuntimeError("MongoDB CRUD verification failed")
        index_name = database["crud"].create_index("value", unique=True)
        if index_name not in database["crud"].index_information():
            raise RuntimeError("MongoDB index verification failed")
        delete_result = database["crud"].delete_one({"_id": "record"})
        if (
            delete_result.deleted_count != 1
            or database["crud"].find_one({"_id": "record"}) is not None
        ):
            raise RuntimeError("MongoDB delete verification failed")

        fs = gridfs.GridFS(database)
        file_id = fs.put(b"mspass-gridfs-contract")
        if fs.get(file_id).read() != b"mspass-gridfs-contract":
            raise RuntimeError("GridFS verification failed")

        waveform = TimeSeries(3)
        waveform.dt = 0.1
        waveform.t0 = 0.0
        waveform.set_live()
        for sample, value in enumerate((1.0, 2.0, 3.0)):
            waveform.data[sample] = value
        saved = database.save_data(
            waveform,
            storage_mode="gridfs",
            mode="promiscuous",
            return_data=True,
        )
        restored = database.read_data(saved["_id"], collection="wf_TimeSeries")
        if list(restored.data) != [1.0, 2.0, 3.0]:
            raise RuntimeError("MsPASS waveform round trip failed")

        history = HistoryLogger(database, job=19029)
        history.register("mongodb_8029_contract", "dict", {"value": 2})
        history.save()
        if database.history.find_one({"jobid": history.jobid}) is None:
            raise RuntimeError("MsPASS history verification failed")
    finally:
        client.drop_database(database.name)
        client.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: verify_mongodb_runtime.py MONGODB_URI")
    main(sys.argv[1])
