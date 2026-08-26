import copy
import os
import uuid
from unittest.mock import patch

import pytest
from bson import ObjectId

from mspasspy.ccore.seismic import Seismogram, TimeSeries
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection
from mspasspy.db.database import Database


@pytest.fixture
def database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception as error:
        client.close()
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    name = "test_elog_replacement_" + uuid.uuid4().hex
    database = Database(client, name)
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


def make_timeseries():
    datum = TimeSeries(1)
    datum.set_live()
    return datum


def make_seismogram():
    datum = Seismogram(1)
    datum.set_live()
    return datum


CASES = [
    (make_timeseries, "wf_TimeSeries_id"),
    (make_seismogram, "wf_Seismogram_id"),
]


@pytest.mark.parametrize("factory,waveform_id_key", CASES)
def test_existing_elog_is_replaced_in_place_with_stable_reference(
    database, factory, waveform_id_key
):
    datum = factory()
    waveform_id = ObjectId()
    datum["_id"] = waveform_id
    datum.elog.log_error("first", "first message", ErrorSeverity.Informational)
    elog_id = database._save_elog(datum)
    datum.erase("_id")
    datum.elog.log_error("second", "second message", ErrorSeverity.Complaint)

    with patch.object(
        Collection,
        "delete_one",
        side_effect=AssertionError("delete_one must not be used for replacement"),
    ):
        returned_id = database._save_elog(datum, elog_id=elog_id)

    assert returned_id == elog_id
    assert database["elog"].count_documents({}) == 1
    document = database["elog"].find_one({"_id": elog_id})
    assert document["_id"] == elog_id
    assert document[waveform_id_key] == waveform_id
    assert [entry["algorithm"] for entry in document["logdata"]] == [
        "first",
        "second",
    ]
    assert [entry["error_message"] for entry in document["logdata"]] == [
        "first message",
        "second message",
    ]


@pytest.mark.parametrize("factory,waveform_id_key", CASES)
def test_replace_failure_preserves_previous_elog_document(
    database, factory, waveform_id_key
):
    datum = factory()
    waveform_id = ObjectId()
    datum["_id"] = waveform_id
    datum.elog.log_error("first", "first message", ErrorSeverity.Informational)
    elog_id = database._save_elog(datum)
    previous_document = copy.deepcopy(database["elog"].find_one({"_id": elog_id}))
    assert previous_document[waveform_id_key] == waveform_id
    datum.elog.log_error("second", "second message", ErrorSeverity.Complaint)
    failure = RuntimeError("injected elog replacement failure")

    with patch.object(Collection, "replace_one", side_effect=failure):
        with pytest.raises(RuntimeError) as error:
            database._save_elog(datum, elog_id=elog_id)

    assert error.value is failure
    assert database["elog"].count_documents({}) == 1
    assert database["elog"].find_one({"_id": elog_id}) == previous_document


@pytest.mark.parametrize("factory,_waveform_id_key", CASES)
@pytest.mark.parametrize("pass_missing_id", [False, True])
def test_no_existing_elog_inserts_exactly_one_new_record(
    database, factory, _waveform_id_key, pass_missing_id
):
    datum = factory()
    datum.elog.log_error("new", "new message", ErrorSeverity.Informational)
    missing_id = ObjectId() if pass_missing_id else None

    elog_id = database._save_elog(datum, elog_id=missing_id)

    assert isinstance(elog_id, ObjectId)
    assert elog_id != missing_id
    assert database["elog"].count_documents({}) == 1
    document = database["elog"].find_one({"_id": elog_id})
    assert document["logdata"][0]["algorithm"] == "new"
    assert document["logdata"][0]["error_message"] == "new message"
