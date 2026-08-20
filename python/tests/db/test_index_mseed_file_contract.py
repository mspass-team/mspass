import os
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection
from mspasspy.db.database import Database


class FakeErrorLogger:
    def __init__(self, errors=()):
        self._errors = list(errors)
        self._job_id = ObjectId()

    def get_error_log(self):
        return self._errors

    def get_job_id(self):
        return self._job_id

    def size(self):
        return len(self._errors)


def make_index(sta, starttime, loc="00"):
    return SimpleNamespace(
        sta=sta,
        net="XX",
        chan="BHZ",
        loc=loc,
        samprate=20.0,
        starttime=starttime,
        last_packet_time=starttime + 4.95,
        foff=int(starttime),
        nbytes=512,
        npts=100,
        endtime=starttime + 4.95,
    )


@pytest.fixture
def database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    probe = MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        probe.admin.command("ping")
    except ServerSelectionTimeoutError as error:
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    finally:
        probe.close()
    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    name = "test_index_mseed_contract_" + uuid.uuid4().hex
    database = Database(client, name)
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


@pytest.mark.parametrize("return_ids,expected", [(False, None), (True, [[], []])])
def test_empty_index_has_exact_return_and_performs_no_write(
    database, tmp_path, return_ids, expected
):
    error = SimpleNamespace(
        algorithm="mseed_file_indexer",
        badness="Complaint",
        message="empty input",
        p_id=12,
    )
    with (
        patch(
            "mspasspy.db.database._mseed_file_indexer",
            return_value=([], FakeErrorLogger([error])),
        ),
        patch.object(Collection, "insert_one") as insert_one,
    ):
        result = database.index_mseed_file(
            tmp_path / "empty.mseed",
            return_ids=return_ids,
            normalize_channel=True,
        )

    assert result == expected
    insert_one.assert_not_called()
    assert database["wf_miniseed"].count_documents({}) == 0
    assert database["elog"].count_documents({}) == 0


def test_each_segment_persists_only_its_own_unique_channel_match(database, tmp_path):
    unique_channel_id = (
        database["channel"]
        .insert_one(
            {
                "net": "XX",
                "sta": "ONE",
                "chan": "BHZ",
                "loc": "00",
                "starttime": 0.0,
                "endtime": 1000.0,
            }
        )
        .inserted_id
    )
    database["channel"].insert_one(
        {
            "net": "XX",
            "sta": "ONE",
            "chan": "BHZ",
            "loc": "99",
            "starttime": 0.0,
            "endtime": 1000.0,
        }
    )
    database["channel"].insert_one(
        {
            "net": "XX",
            "sta": "ONE",
            "chan": "BHZ",
            "loc": "00",
            "starttime": 100.0,
            "endtime": 1000.0,
        }
    )
    for marker in (1, 2):
        database["channel"].insert_one(
            {
                "net": "XX",
                "sta": "MANY",
                "chan": "BHZ",
                "loc": "00",
                "starttime": 0.0,
                "endtime": 1000.0,
                "marker": marker,
            }
        )
    no_loc_channel_id = (
        database["channel"]
        .insert_one(
            {
                "net": "XX",
                "sta": "NOLOC",
                "chan": "BHZ",
                "loc": "99",
                "starttime": 0.0,
                "endtime": 1000.0,
            }
        )
        .inserted_id
    )
    database["channel"].insert_one(
        {
            "net": "XX",
            "sta": "BOUNDARY",
            "chan": "BHZ",
            "loc": "00",
            "starttime": 400.0,
            "endtime": 1000.0,
        }
    )

    indexes = [
        make_index("ONE", 100.0),
        make_index("ZERO", 200.0),
        make_index("MANY", 300.0),
        make_index("NOLOC", 350.0, loc=""),
        make_index("BOUNDARY", 400.0),
    ]
    with (
        patch(
            "mspasspy.db.database._mseed_file_indexer",
            return_value=(indexes, FakeErrorLogger()),
        ),
        patch("builtins.print") as print_mock,
    ):
        waveform_ids, elog_ids = database.index_mseed_file(
            tmp_path / "segments.mseed",
            return_ids=True,
            normalize_channel=True,
            verbose=True,
        )

    assert len(waveform_ids) == 5
    assert elog_ids == []
    documents = [
        database["wf_miniseed"].find_one({"_id": waveform_id})
        for waveform_id in waveform_ids
    ]
    assert documents[0]["channel_id"] == unique_channel_id
    assert "channel_id" not in documents[1]
    assert "channel_id" not in documents[2]
    assert documents[3]["channel_id"] == no_loc_channel_id
    assert "loc" not in documents[3]
    assert "channel_id" not in documents[4]
    assert [document["sta"] for document in documents] == [
        "ONE",
        "ZERO",
        "MANY",
        "NOLOC",
        "BOUNDARY",
    ]
    print_mock.assert_called_once()
    print_args = print_mock.call_args.args
    assert print_args[1] == 2
    assert print_args[3] == {
        "net": "XX",
        "sta": "MANY",
        "chan": "BHZ",
        "loc": "00",
        "starttime": {"$lt": 300.0},
        "endtime": {"$gt": 300.0},
    }


def test_each_segment_is_normalized_before_its_insert(database, tmp_path):
    indexes = [make_index("FIRST", 100.0), make_index("SECOND", 200.0)]
    failure = RuntimeError("injected second-segment normalization failure")
    original_count_documents = Collection.count_documents

    def fail_second_segment(self, query, *args, **kwargs):
        if self.name == "channel":
            if query["sta"] == "SECOND":
                raise failure
            return 0
        return original_count_documents(self, query, *args, **kwargs)

    with (
        patch(
            "mspasspy.db.database._mseed_file_indexer",
            return_value=(indexes, FakeErrorLogger()),
        ),
        patch.object(Collection, "count_documents", fail_second_segment),
    ):
        with pytest.raises(RuntimeError) as error:
            database.index_mseed_file(
                tmp_path / "normalization-failure.mseed",
                normalize_channel=True,
            )

    assert error.value is failure
    documents = list(database["wf_miniseed"].find({}))
    assert len(documents) == 1
    assert documents[0]["sta"] == "FIRST"
    assert database["elog"].count_documents({}) == 0


def test_each_affected_waveform_gets_a_fresh_elog_document(database, tmp_path):
    error = SimpleNamespace(
        algorithm="mseed_file_indexer",
        badness="Complaint",
        message="recoverable packet error",
        p_id=34,
    )
    indexes = [make_index("FIRST", 100.0), make_index("SECOND", 200.0)]
    logger = FakeErrorLogger([error])
    with patch(
        "mspasspy.db.database._mseed_file_indexer",
        return_value=(indexes, logger),
    ):
        waveform_ids, elog_ids = database.index_mseed_file(
            tmp_path / "errors.mseed",
            return_ids=True,
        )

    assert len(waveform_ids) == 2
    assert len(elog_ids) == 2
    assert len(set(elog_ids)) == 2
    assert database["elog"].count_documents({}) == 2
    for waveform_id, elog_id in zip(waveform_ids, elog_ids):
        document = database["elog"].find_one({"_id": elog_id})
        assert document["wf_miniseed_id"] == waveform_id
        assert document["logdata"] == [
            {
                "job_id": logger.get_job_id(),
                "algorithm": "mseed_file_indexer",
                "badness": "Complaint",
                "error_message": "recoverable packet error",
                "process_id": 34,
            }
        ]
