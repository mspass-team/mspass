import copy
import os
import pickle
import uuid
from pathlib import Path

import numpy as np
import obspy
import pytest
from bson import BSON, ObjectId
from pymongo.errors import PyMongoError

from mspasspy.algorithms.calib import ApplyCalibEngine
from mspasspy.ccore.seismic import (
    DoubleVector,
    PowerSpectrum,
    TimeReferenceType,
    TimeSeries,
)
from mspasspy.ccore.utility import (
    AtomicType,
    ErrorSeverity,
    Metadata,
    MsPASSError,
    ProcessingHistory,
)
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database, history2doc, trusted_legacy_pickle
from mspasspy.db.serialization import (
    TYPE_KEY,
    VERSION,
    VERSION_KEY,
    decode_inventory,
    decode_power_spectrum,
    decode_processing_history,
    decode_response,
    encode_inventory,
    encode_power_spectrum,
    encode_processing_history,
)
from mspasspy.db.spectrumdb import SpectrumDatabase


def _write_marker_and_return(path, value):
    Path(path).write_text("executed", encoding="utf-8")
    return value


class ExecutingPickle:
    def __init__(self, marker, value):
        self.marker = str(marker)
        self.value = value

    def __reduce__(self):
        return _write_marker_and_return, (self.marker, self.value)


def _assert_no_binary_payload(value):
    if isinstance(value, dict):
        for item in value.values():
            _assert_no_binary_payload(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _assert_no_binary_payload(item)
    else:
        assert not isinstance(value, (bytes, bytearray))


def _history_signature(history):
    current = history.current_nodedata()
    current_signature = (
        current.status.name,
        current.uuid,
        current.type.name,
        current.stage,
        current.algorithm,
        current.algid,
    )
    edges = []
    for child, parents in history.get_nodes().items():
        for parent in parents:
            edges.append(
                (
                    child,
                    parent.status.name,
                    parent.uuid,
                    parent.type.name,
                    parent.stage,
                    parent.algorithm,
                    parent.algid,
                )
            )
    logs = [
        (x.job_id, x.p_id, x.algorithm, x.message, x.badness.name)
        for x in history.elog.get_error_log()
    ]
    return (
        history.jobname(),
        history.jobid(),
        current_signature,
        sorted(edges),
        history.elog.get_job_id(),
        logs,
    )


def _make_history():
    left = TimeSeries(4)
    left.set_as_origin("reader", "left", "left-uuid", AtomicType.TIMESERIES, True)
    left.new_map("filter", "left-filter", AtomicType.TIMESERIES)
    right = TimeSeries(4)
    right.set_as_origin("reader", "right", "right-uuid", AtomicType.TIMESERIES, True)
    history = ProcessingHistory("job-name", "job-id")
    history.new_ensemble_process(
        "stack", "stack-id", AtomicType.TIMESERIES, [left, right]
    )
    history.elog.set_job_id(23)
    history.elog.log_error("stack", "diagnostic", ErrorSeverity.Complaint)
    return history


def _make_spectrum():
    spectrum = PowerSpectrum(
        Metadata({"station": "AAA", "quality": 7, "weights": (1.0, 2.0)}),
        DoubleVector([1.0, 4.0, 9.0]),
        0.25,
        "contract-spectrum",
        0.0,
        0.1,
        20,
    )
    spectrum.elog.set_job_id(31)
    spectrum.elog.log_error("spectrum", "diagnostic", ErrorSeverity.Debug)
    return spectrum


def _spectrum_signature(spectrum):
    logs = [
        (x.job_id, x.p_id, x.algorithm, x.message, x.badness.name)
        for x in spectrum.elog.get_error_log()
    ]
    return (
        dict(spectrum),
        list(spectrum.spectrum),
        spectrum.df(),
        spectrum.f0(),
        spectrum.spectrum_type,
        spectrum.dt(),
        spectrum.timeseries_npts(),
        spectrum.live(),
        spectrum.elog.get_job_id(),
        logs,
    )


@pytest.fixture
def mongo_database():
    client = DBClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    name = f"mspass_issue_841_{uuid.uuid4().hex}"
    database = Database(client, name)
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


def test_contract_suite_loads_selected_source():
    selected_source = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if selected_source:
        assert (
            Path(encode_processing_history.__code__.co_filename).resolve()
            == (Path(selected_source) / "mspasspy/db/serialization.py").resolve()
        )


def test_history_field_document_round_trip_and_database_read(mongo_database):
    history = _make_history()
    document = encode_processing_history(history)
    BSON.encode({"payload": document})
    assert document[TYPE_KEY] == "ProcessingHistory"
    assert document[VERSION_KEY] == VERSION
    _assert_no_binary_payload(document)
    assert _history_signature(
        decode_processing_history(document)
    ) == _history_signature(history)

    saved = history2doc(history)
    identifier = mongo_database.history_object.insert_one(saved).inserted_id
    target = TimeSeries(2)
    target.set_live()
    target["_id"] = ObjectId()
    mongo_database._load_history(target, identifier, alg_name="read", alg_id="read-id")
    assert target.current_nodedata().algorithm == "read"
    assert target.number_of_stages() == history.number_of_stages() + 1


def test_public_save_and_read_history_round_trip(mongo_database):
    source = TimeSeries(4)
    source.set_live()
    source.dt = 0.05
    source.t0 = 1_700_000_000.0
    source.tref = TimeReferenceType.UTC
    source["npts"] = source.npts
    source["sampling_rate"] = 20.0
    source["starttime"] = source.t0
    source["delta"] = source.dt
    source["calib"] = 1.0
    source.data = DoubleVector([1.0, 2.0, 3.0, 4.0])
    source.set_as_origin(
        "reader", "reader-id", "source-uuid", AtomicType.TIMESERIES, True
    )
    source.new_map("filter", "filter-id", AtomicType.TIMESERIES)

    saved = mongo_database.save_data(
        source,
        storage_mode="gridfs",
        save_history=True,
        alg_id="save-id",
        return_data=True,
    )
    history_document = mongo_database.history_object.find_one(
        {"_id": saved["history_object_id"]}
    )
    assert history_document["processing_history"][TYPE_KEY] == "ProcessingHistory"
    BSON.encode(history_document)

    restored = mongo_database.read_data(
        saved["_id"], load_history=True, alg_id="read-id"
    )
    assert restored.live
    assert restored.current_nodedata().algorithm == "read_data"
    assert restored.current_nodedata().algid == "read-id"
    assert restored.number_of_stages() == 3


def test_inventory_response_public_round_trip(mongo_database):
    source = obspy.read_inventory("python/tests/data/TA.035A.xml")
    counts = mongo_database.save_inventory(source, networks_to_exclude=None)
    assert counts[0] > 0
    assert counts[1] == 3

    for document in mongo_database.site.find({}):
        payload = document["serialized_inventory"]
        assert payload[TYPE_KEY] == "Inventory"
        _assert_no_binary_payload(payload)
        BSON.encode({"payload": payload})
    for document in mongo_database.channel.find({}):
        payload = document["serialized_channel_data"]
        assert payload[TYPE_KEY] == "Response"
        _assert_no_binary_payload(payload)
        BSON.encode({"payload": payload})

    restored = mongo_database.read_inventory(net="TA", sta="035A")
    assert restored == source
    time = 1263254500.0
    for channel in ("BHE", "BHN", "BHZ"):
        assert mongo_database.get_response("TA", "035A", channel, "", time) == (
            source.get_response(f"TA.035A..{channel}", time)
        )

    engine = ApplyCalibEngine(mongo_database)
    assert len(engine.calib) == 3


def test_inventory_codec_preserves_complete_obspy_graph():
    source = obspy.read_inventory("python/tests/data/calib_teststa.xml")
    payload = encode_inventory(source)
    BSON.encode({"payload": payload})
    _assert_no_binary_payload(payload)
    assert decode_inventory(payload) == source


def test_power_spectrum_field_document_and_public_round_trip(mongo_database):
    spectrum = _make_spectrum()
    payload = BSON.encode({"payload": encode_power_spectrum(spectrum)}).decode()[
        "payload"
    ]
    assert payload[TYPE_KEY] == "PowerSpectrum"
    _assert_no_binary_payload(payload)
    assert _spectrum_signature(decode_power_spectrum(payload)) == _spectrum_signature(
        spectrum
    )

    handle = SpectrumDatabase.__new__(SpectrumDatabase)
    handle.type_list = [PowerSpectrum]
    handle.collection = mongo_database["PowerSpectrum"]
    identifier = handle.save_data(spectrum)
    stored = handle.collection.find_one({"_id": identifier})
    assert stored["serialized_data"][TYPE_KEY] == "PowerSpectrum"
    assert _spectrum_signature(handle.read_data(identifier)) == _spectrum_signature(
        spectrum
    )

    spectrum.kill()
    dead = decode_power_spectrum(encode_power_spectrum(spectrum))
    assert dead.dead()
    assert _spectrum_signature(dead) == _spectrum_signature(spectrum)


def test_power_spectrum_preserves_supported_structured_metadata():
    spectrum = _make_spectrum()
    spectrum["double_vector"] = DoubleVector([2.0, 3.0])
    spectrum["nested"] = {
        7: "integer key",
        TYPE_KEY: "literal metadata value",
        ("tuple", 1): {VERSION_KEY: "also literal"},
    }

    payload = BSON.encode({"payload": encode_power_spectrum(spectrum)}).decode()[
        "payload"
    ]
    restored = decode_power_spectrum(payload)

    assert isinstance(restored["double_vector"], DoubleVector)
    assert list(restored["double_vector"]) == [2.0, 3.0]
    assert restored["nested"] == spectrum["nested"]


@pytest.mark.parametrize(
    "collection,field,reader",
    [
        (
            "history_object",
            "processing_history",
            lambda database, identifier: _load_legacy_history(database, identifier),
        ),
        (
            "site",
            "serialized_inventory",
            lambda database, identifier: database.read_inventory(net="MAL"),
        ),
        (
            "channel",
            "serialized_channel_data",
            lambda database, identifier: database.get_response(
                "MAL", "BAD", "BHZ", "", 10.0
            ),
        ),
        (
            "PowerSpectrum",
            "serialized_data",
            lambda database, identifier: _read_legacy_spectrum(database, identifier),
        ),
    ],
)
def test_normal_reads_reject_legacy_pickle_without_execution(
    mongo_database, tmp_path, collection, field, reader
):
    marker = tmp_path / f"{collection}.marker"
    payload = pickle.dumps(ExecutingPickle(marker, "unexpected"))
    document = {"_id": ObjectId(), field: payload}
    if collection == "site":
        document.update({"net": "MAL", "sta": "BAD"})
    elif collection == "channel":
        document.update(
            {
                "net": "MAL",
                "sta": "BAD",
                "chan": "BHZ",
                "loc": "",
                "starttime": 0.0,
                "endtime": 20.0,
            }
        )
    mongo_database[collection].insert_one(copy.deepcopy(document))

    with pytest.raises(MsPASSError) as error:
        reader(mongo_database, document["_id"])
    assert error.value.severity == ErrorSeverity.Invalid
    assert not marker.exists()


def _read_legacy_spectrum(database, identifier):
    handle = SpectrumDatabase.__new__(SpectrumDatabase)
    handle.collection = database["PowerSpectrum"]
    return handle.read_data(identifier)


def _load_legacy_history(database, identifier):
    target = TimeSeries(1)
    target["_id"] = ObjectId()
    target.set_live()
    return database._load_history(target, identifier)


def test_apply_calib_rejects_legacy_response_without_execution(
    mongo_database, tmp_path
):
    marker = tmp_path / "calib.marker"
    mongo_database.channel.insert_one(
        {"serialized_channel_data": pickle.dumps(ExecutingPickle(marker, "unexpected"))}
    )
    with pytest.raises(MsPASSError) as error:
        ApplyCalibEngine(mongo_database)
    assert error.value.severity == ErrorSeverity.Invalid
    assert not marker.exists()


@pytest.mark.parametrize(
    "collection,field,value",
    [
        pytest.param(
            "history_object",
            "processing_history",
            _make_history(),
            id="history",
        ),
        pytest.param(
            "PowerSpectrum",
            "serialized_data",
            _make_spectrum(),
            id="spectrum",
        ),
    ],
)
def test_explicit_trusted_migration_executes_and_is_idempotent(
    mongo_database, tmp_path, collection, field, value
):
    marker = tmp_path / f"{collection}.marker"
    identifier = (
        mongo_database[collection]
        .insert_one({field: pickle.dumps(ExecutingPickle(marker, value))})
        .inserted_id
    )
    assert trusted_legacy_pickle(mongo_database, collection) == 1
    assert marker.read_text(encoding="utf-8") == "executed"
    stored = mongo_database[collection].find_one({"_id": identifier})
    assert stored[field][VERSION_KEY] == VERSION
    if field == "processing_history":
        assert _history_signature(
            decode_processing_history(stored[field])
        ) == _history_signature(value)
    else:
        assert _spectrum_signature(
            decode_power_spectrum(stored[field])
        ) == _spectrum_signature(value)
    assert trusted_legacy_pickle(mongo_database, collection) == 0


def test_migration_skips_new_record_with_payload_named_metadata(
    mongo_database, tmp_path
):
    marker = tmp_path / "already-new.marker"
    mongo_database.PowerSpectrum.insert_one(
        {
            "processing_history": pickle.dumps(
                ExecutingPickle(marker, "metadata, not the persisted spectrum")
            ),
            "serialized_data": encode_power_spectrum(_make_spectrum()),
        }
    )

    assert trusted_legacy_pickle(mongo_database, "PowerSpectrum") == 0
    assert not marker.exists()


def test_inventory_and_response_trusted_migration(mongo_database):
    inventory = obspy.read_inventory("python/tests/data/TA.035A.xml")
    network = inventory.networks[0]
    channel = network.stations[0].channels[0]
    site_id = mongo_database.site.insert_one(
        {"serialized_inventory": pickle.dumps(network)}
    ).inserted_id
    channel_id = mongo_database.channel.insert_one(
        {
            "net": network.code,
            "sta": network.stations[0].code,
            "serialized_channel_data": pickle.dumps(channel),
        }
    ).inserted_id
    assert trusted_legacy_pickle(mongo_database, "site") == 1
    assert trusted_legacy_pickle(mongo_database, "channel") == 1
    inventory_payload = mongo_database.site.find_one({"_id": site_id})[
        "serialized_inventory"
    ]
    response_payload = mongo_database.channel.find_one({"_id": channel_id})[
        "serialized_channel_data"
    ]
    assert inventory_payload[TYPE_KEY] == "Inventory"
    assert response_payload[TYPE_KEY] == "Response"
    assert decode_inventory(inventory_payload).networks == [network]
    assert decode_response(response_payload) == channel.response


def test_migration_query_order_failure_prefix_and_retry(mongo_database):
    collection = mongo_database["migration_order"]
    identifiers = [
        ObjectId.from_datetime(obspy.UTCDateTime(x).datetime) for x in (1, 2, 3)
    ]
    values = [_make_spectrum(), "wrong type", _make_spectrum()]
    originals = []
    for identifier, value in zip(identifiers, values):
        document = {
            "_id": identifier,
            "selected": True,
            "serialized_data": pickle.dumps(value),
        }
        originals.append(copy.deepcopy(document))
        collection.insert_one(document)
    collection.insert_one(
        {"selected": False, "serialized_data": pickle.dumps(_make_spectrum())}
    )

    with pytest.raises(MsPASSError) as error:
        trusted_legacy_pickle(mongo_database, "migration_order", {"selected": True})
    assert error.value.severity == ErrorSeverity.Invalid
    assert isinstance(
        collection.find_one({"_id": identifiers[0]})["serialized_data"], dict
    )
    assert collection.find_one({"_id": identifiers[1]}) == originals[1]
    assert collection.find_one({"_id": identifiers[2]}) == originals[2]

    collection.update_one(
        {"_id": identifiers[1]},
        {"$set": {"serialized_data": pickle.dumps(_make_spectrum())}},
    )
    assert (
        trusted_legacy_pickle(mongo_database, "migration_order", {"selected": True})
        == 2
    )
    assert isinstance(
        collection.find_one({"selected": False})["serialized_data"], bytes
    )


@pytest.mark.parametrize(
    "payload",
    [b"not a pickle", "not bytes"],
    ids=["unpickle-error", "non-bytes"],
)
def test_migration_deserialization_failure_leaves_record_unchanged(
    mongo_database, payload
):
    collection_name = "migration_deserialization_failure"
    collection = mongo_database[collection_name]
    original = {"_id": ObjectId(), "serialized_data": payload}
    collection.insert_one(copy.deepcopy(original))

    with pytest.raises(MsPASSError) as error:
        trusted_legacy_pickle(mongo_database, collection_name)
    assert error.value.severity == ErrorSeverity.Invalid
    assert collection.find_one({"_id": original["_id"]}) == original


class FailingCollection:
    def __init__(self, collection, fail_id):
        self.collection = collection
        self.fail_id = fail_id

    def find(self, query):
        return self.collection.find(query)

    def replace_one(self, query, replacement):
        if query["_id"] == self.fail_id:
            raise PyMongoError("injected write failure")
        return self.collection.replace_one(query, replacement)


class FailingDatabase:
    def __init__(self, database, collection_name, fail_id):
        self.database = database
        self.collection_name = collection_name
        self.fail_id = fail_id

    def __getitem__(self, collection_name):
        assert collection_name == self.collection_name
        return FailingCollection(self.database[collection_name], self.fail_id)


def test_migration_write_failure_keeps_failing_record_and_successful_prefix(
    mongo_database,
):
    collection_name = "migration_write_failure"
    collection = mongo_database[collection_name]
    identifiers = [
        ObjectId.from_datetime(obspy.UTCDateTime(x).datetime) for x in (4, 5, 6)
    ]
    originals = {}
    for identifier in identifiers:
        original = {
            "_id": identifier,
            "serialized_data": pickle.dumps(_make_spectrum()),
        }
        originals[identifier] = copy.deepcopy(original)
        collection.insert_one(original)

    failing_database = FailingDatabase(mongo_database, collection_name, identifiers[1])
    with pytest.raises(MsPASSError) as error:
        trusted_legacy_pickle(failing_database, collection_name)
    assert error.value.severity == ErrorSeverity.Invalid
    assert isinstance(
        collection.find_one({"_id": identifiers[0]})["serialized_data"], dict
    )
    assert collection.find_one({"_id": identifiers[1]}) == originals[identifiers[1]]
    assert collection.find_one({"_id": identifiers[2]}) == originals[identifiers[2]]
    assert trusted_legacy_pickle(mongo_database, collection_name) == 2
