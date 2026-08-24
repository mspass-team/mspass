import os
import pickle
import uuid
from pathlib import Path

import numpy as np
import obspy
import pytest
from bson import BSON, ObjectId

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
from mspasspy.db.database import Database, history2doc
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


def test_legacy_pickle_codecs_round_trip():
    history = _make_history()
    assert _history_signature(
        decode_processing_history(pickle.dumps(history))
    ) == _history_signature(history)

    inventory = obspy.read_inventory("python/tests/data/TA.035A.xml")
    network = inventory.networks[0]
    channel = network.stations[0].channels[0]
    assert decode_inventory(pickle.dumps(network)).networks == [network]
    assert decode_inventory(pickle.dumps(inventory)) == inventory
    assert decode_response(pickle.dumps(channel)) == channel.response

    spectrum = _make_spectrum()
    assert _spectrum_signature(
        decode_power_spectrum(pickle.dumps(spectrum))
    ) == _spectrum_signature(spectrum)


def test_public_readers_accept_legacy_pickle(mongo_database):
    history = _make_history()
    history_document = history2doc(history)
    history_document["processing_history"] = pickle.dumps(history)
    history_id = mongo_database.history_object.insert_one(history_document).inserted_id
    target = TimeSeries(1)
    target["_id"] = ObjectId()
    target.set_live()
    mongo_database._load_history(target, history_id, alg_name="read", alg_id="read-id")
    assert target.current_nodedata().algorithm == "read"

    inventory = obspy.read_inventory("python/tests/data/TA.035A.xml")
    network = inventory.networks[0]
    station = network.stations[0]
    channel = station.channels[0]
    mongo_database.site.insert_one(
        {
            "net": network.code,
            "sta": station.code,
            "loc": channel.location_code,
            "serialized_inventory": pickle.dumps(network),
        }
    )
    restored = mongo_database.read_inventory(net=network.code, sta=station.code)
    assert restored.networks == [network]

    starttime = channel.start_date.timestamp
    endtime = channel.end_date.timestamp
    mongo_database.channel.insert_one(
        {
            "net": network.code,
            "sta": station.code,
            "chan": channel.code,
            "loc": channel.location_code,
            "starttime": starttime,
            "endtime": endtime,
            "serialized_channel_data": pickle.dumps(channel),
        }
    )
    query_time = (starttime + endtime) / 2.0
    assert (
        mongo_database.get_response(
            network.code,
            station.code,
            channel.code,
            channel.location_code,
            query_time,
        )
        == channel.response
    )
    assert len(ApplyCalibEngine(mongo_database).calib) == 1

    spectrum = _make_spectrum()
    handle = SpectrumDatabase.__new__(SpectrumDatabase)
    handle.type_list = [PowerSpectrum]
    handle.collection = mongo_database["PowerSpectrum"]
    spectrum_id = handle.collection.insert_one(
        {"serialized_data": pickle.dumps(spectrum)}
    ).inserted_id
    assert _spectrum_signature(handle.read_data(spectrum_id)) == _spectrum_signature(
        spectrum
    )


def test_mixed_power_spectrum_formats_are_read_per_document(mongo_database):
    handle = SpectrumDatabase.__new__(SpectrumDatabase)
    handle.type_list = [PowerSpectrum]
    handle.collection = mongo_database["PowerSpectrum"]
    spectrum = _make_spectrum()
    bson_id = handle.save_data(spectrum)
    pickle_id = handle.save_data(spectrum, format="pickle")

    assert isinstance(
        handle.collection.find_one({"_id": bson_id})["serialized_data"], dict
    )
    assert isinstance(
        handle.collection.find_one({"_id": pickle_id})["serialized_data"], bytes
    )
    for identifier in (bson_id, pickle_id):
        assert _spectrum_signature(handle.read_data(identifier)) == _spectrum_signature(
            spectrum
        )
