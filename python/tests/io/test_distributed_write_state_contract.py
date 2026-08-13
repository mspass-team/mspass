import os
from pathlib import Path
import uuid

from bson import BSON
from bson.objectid import ObjectId
import dask
import dask.bag
import pymongo
import pytest

from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import AtomicType, ErrorLogger, ErrorSeverity
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
import mspasspy.io.distributed as distributed_module
from mspasspy.util.Undertaker import Undertaker

SOURCE_PYTHON_ROOT = Path(
    os.environ.get("MSPASS_TEST_SOURCE_ROOT", Path(__file__).resolve().parents[2])
)


class CollectionSchema:
    def __init__(self, collection_name):
        self.collection_name = collection_name

    def collection(self, key):
        assert key == "_id"
        return self.collection_name


@pytest.fixture(scope="session", autouse=True)
def assert_distributed_module_loaded_from_selected_worktree():
    expected = SOURCE_PYTHON_ROOT / "mspasspy/io/distributed.py"
    assert Path(distributed_module.__file__).resolve() == expected.resolve()


@pytest.fixture
def database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    probe = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
    probe.admin.command("ping")
    probe.close()

    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    database_name = "test_issue_833_" + uuid.uuid4().hex
    database = Database(client, database_name)
    try:
        yield database
    finally:
        client.drop_database(database_name)
        client.close()


def make_datum(marker):
    datum = TimeSeries(2)
    datum.set_live()
    datum["waveform_marker"] = marker
    datum["preserved_number"] = 17
    datum.set_as_origin("contract_origin", "0", marker, AtomicType.TIMESERIES)
    return datum


def conversion_logger(enabled):
    elog = ErrorLogger()
    if enabled:
        elog.log_error(
            "conversion_log",
            "metadata conversion diagnostic",
            ErrorSeverity.Complaint,
        )
    return elog


def install_md2doc(monkeypatch, with_conversion_log):
    def fake_md2doc(datum, **kwargs):
        doc = {
            "waveform_marker": datum["waveform_marker"],
            "preserved_number": datum["preserved_number"],
        }
        return doc, True, conversion_logger(with_conversion_log)

    monkeypatch.setattr(distributed_module, "md2doc", fake_md2doc)


def install_rejected_md2doc(monkeypatch):
    def fake_md2doc(datum, **kwargs):
        return (
            {
                "waveform_marker": datum["waveform_marker"],
                "preserved_number": datum["preserved_number"],
            },
            False,
            conversion_logger(True),
        )

    monkeypatch.setattr(distributed_module, "md2doc", fake_md2doc)


def persist_distributed_document(
    path,
    datum,
    database,
    collection_name,
    post_elog,
):
    save_schema = CollectionSchema(collection_name)
    undertaker = Undertaker(database)
    common_arguments = {
        "save_schema": save_schema,
        "exclude_keys": [],
        "mode": "promiscuous",
        "normalizing_collections": [],
        "post_elog": post_elog,
        "save_history": False,
        "post_history": False,
    }

    if path == "atomic":
        doc = distributed_module._atomic_extract_wf_document(
            datum,
            database,
            undertaker=undertaker,
            **common_arguments,
        )
        BSON.encode(doc)
        waveform_id = database[collection_name].insert_one(doc).inserted_id
    else:
        ensemble = TimeSeriesEnsemble()
        ensemble.member.append(datum)
        ensemble.set_live()
        waveform_ids = distributed_module._save_ensemble_wfdocs(
            ensemble,
            database,
            undertaker=undertaker,
            cremate=False,
            **common_arguments,
        )
        assert len(waveform_ids) == 1
        waveform_id = waveform_ids[0]

    persisted = database[collection_name].find_one({"_id": waveform_id})
    assert persisted is not None
    BSON.encode(persisted)
    return persisted


@pytest.mark.parametrize("path", ["atomic", "ensemble"])
def test_public_distributed_history_preserves_waveform_and_is_bson_writable(
    database, path
):
    marker = path + "_history_marker"
    datum = make_datum(marker)
    if path == "atomic":
        data = datum
    else:
        data = TimeSeriesEnsemble()
        data.member.append(datum)
        data.set_live()
    bag = dask.bag.from_sequence([data], npartitions=1)
    data_tag = "issue_833_" + uuid.uuid4().hex

    with dask.config.set(scheduler="synchronous"):
        distributed_module.write_distributed_data(
            bag,
            database,
            data_are_atomic=path == "atomic",
            collection="wf_TimeSeries",
            data_tag=data_tag,
            post_elog=True,
            save_history=True,
            post_history=True,
        )

    assert database["wf_TimeSeries"].count_documents({"data_tag": data_tag}) == 1
    persisted = database["wf_TimeSeries"].find_one({"data_tag": data_tag})
    assert persisted is not None
    BSON.encode(persisted)
    assert persisted["waveform_marker"] == marker
    assert persisted["preserved_number"] == 17
    assert persisted["storage_mode"] == "gridfs"
    assert isinstance(persisted["gridfs_id"], ObjectId)
    history = persisted["history_data"]
    assert set(history) == {
        "save_uuid",
        "save_stage",
        "processing_history",
        "alg_id",
        "alg_name",
    }
    assert history["save_uuid"] == marker
    assert history["save_stage"] == 0
    assert history["alg_name"] == "contract_origin"
    assert history["alg_id"] == "0"
    assert isinstance(history["processing_history"], bytes)
    assert "history_data" not in history


@pytest.mark.parametrize("path", ["atomic", "ensemble"])
def test_public_distributed_empty_history_preserves_waveform_without_subdocument(
    database, path
):
    marker = path + "_empty_history"
    datum = TimeSeries(2)
    datum.set_live()
    datum["waveform_marker"] = marker
    if path == "atomic":
        data = datum
    else:
        data = TimeSeriesEnsemble()
        data.member.append(datum)
        data.set_live()
    bag = dask.bag.from_sequence([data], npartitions=1)
    data_tag = "issue_833_empty_history_" + uuid.uuid4().hex

    with dask.config.set(scheduler="synchronous"):
        distributed_module.write_distributed_data(
            bag,
            database,
            data_are_atomic=path == "atomic",
            collection="wf_TimeSeries",
            data_tag=data_tag,
            save_history=True,
            post_history=True,
        )

    persisted = database["wf_TimeSeries"].find_one({"data_tag": data_tag})
    assert persisted is not None
    assert persisted["waveform_marker"] == marker
    assert "history_data" not in persisted


@pytest.mark.parametrize("path", ["atomic", "ensemble"])
@pytest.mark.parametrize("post_elog", [False, True])
def test_public_dask_write_merges_both_elogs_and_honors_post_flag(
    monkeypatch, database, path, post_elog
):
    install_md2doc(monkeypatch, with_conversion_log=True)
    datum = make_datum(path + "_public_elog")
    datum.elog.set_job_id(833)
    datum.elog.log_error("datum_log", "datum diagnostic", ErrorSeverity.Complaint)
    if path == "atomic":
        data = datum
    else:
        data = TimeSeriesEnsemble()
        data.member.append(datum)
        data.set_live()
    bag = dask.bag.from_sequence([data], npartitions=1)
    data_tag = "issue_833_elog_" + uuid.uuid4().hex

    with dask.config.set(scheduler="synchronous"):
        distributed_module.write_distributed_data(
            bag,
            database,
            data_are_atomic=path == "atomic",
            collection="wf_TimeSeries",
            data_tag=data_tag,
            post_elog=post_elog,
        )

    persisted = database["wf_TimeSeries"].find_one({"data_tag": data_tag})
    assert persisted is not None
    if post_elog:
        assert "elog_id" not in persisted
        elog_document = persisted["error_log"]
        assert database["elog"].count_documents({}) == 0
    else:
        assert "error_log" not in persisted
        assert isinstance(persisted["elog_id"], ObjectId)
        elog_document = database["elog"].find_one({"_id": persisted["elog_id"]})
        assert elog_document is not None
        assert database["elog"].count_documents({}) == 1
    assert [entry["algorithm"] for entry in elog_document["logdata"]] == [
        "datum_log",
        "conversion_log",
    ]
    assert [entry["job_id"] for entry in elog_document["logdata"]] == [833, 833]


@pytest.mark.parametrize("path", ["atomic", "ensemble"])
@pytest.mark.parametrize("post_elog", [False, True])
def test_rejected_metadata_conversion_buries_both_elogs(
    monkeypatch, database, path, post_elog
):
    install_rejected_md2doc(monkeypatch)
    datum = make_datum(path + "_rejected_metadata")
    datum.elog.log_error("datum_log", "datum diagnostic", ErrorSeverity.Complaint)
    if path == "atomic":
        data = datum
    else:
        data = TimeSeriesEnsemble()
        data.member.append(datum)
        data.set_live()
    bag = dask.bag.from_sequence([data], npartitions=1)

    with dask.config.set(scheduler="synchronous"):
        result = distributed_module.write_distributed_data(
            bag,
            database,
            data_are_atomic=path == "atomic",
            collection="wf_TimeSeries",
            post_elog=post_elog,
        )

    expected_result = [None] if path == "atomic" else [[]]
    assert result == expected_result
    assert database["wf_TimeSeries"].count_documents({}) == 0
    cemetery = database["cemetery"].find_one({})
    assert cemetery is not None
    algorithms = [entry["algorithm"] for entry in cemetery["logdata"]]
    assert algorithms.count("datum_log") == 1
    assert algorithms.count("conversion_log") == 1
    assert algorithms.index("datum_log") < algorithms.index("conversion_log")


def test_post_error_log_preserves_duplicate_entries_without_mutating_inputs():
    datum = make_datum("duplicate_elog")
    datum.elog.set_job_id(833)
    datum.elog.log_error("same", "same diagnostic", ErrorSeverity.Complaint)
    conversion_elog = ErrorLogger()
    conversion_elog.log_error("same", "same diagnostic", ErrorSeverity.Complaint)

    result = distributed_module.post_error_log(datum, {}, other_elog=conversion_elog)

    entries = result["error_log"]["logdata"]
    assert [entry["algorithm"] for entry in entries] == ["same", "same"]
    assert [entry["error_message"] for entry in entries] == [
        "same diagnostic",
        "same diagnostic",
    ]
    assert [entry["job_id"] for entry in entries] == [833, 833]
    assert datum.elog.size() == 1
    assert conversion_elog.size() == 1


def test_post_error_log_uses_datum_job_id_for_conversion_only_log():
    datum = make_datum("conversion_only_elog")
    datum.elog.set_job_id(833)
    conversion_elog = ErrorLogger(912)
    conversion_elog.log_error(
        "conversion_log", "conversion diagnostic", ErrorSeverity.Complaint
    )

    result = distributed_module.post_error_log(datum, {}, other_elog=conversion_elog)

    assert [entry["job_id"] for entry in result["error_log"]["logdata"]] == [833]
    assert datum.elog.size() == 0
    assert conversion_elog.size() == 1


@pytest.mark.parametrize("path", ["atomic", "ensemble"])
@pytest.mark.parametrize("post_elog", [False, True])
@pytest.mark.parametrize("log_sources", ["none", "datum", "conversion", "both"])
def test_elog_posting_matrix_preserves_every_entry_and_honors_flag(
    monkeypatch, database, path, post_elog, log_sources
):
    with_datum_log = log_sources in {"datum", "both"}
    with_conversion_log = log_sources in {"conversion", "both"}
    install_md2doc(monkeypatch, with_conversion_log=with_conversion_log)
    collection_name = "wf_issue_833_" + uuid.uuid4().hex
    datum = make_datum(path + "_" + log_sources)
    if with_datum_log:
        datum.elog.log_error("datum_log", "datum diagnostic", ErrorSeverity.Complaint)

    persisted = persist_distributed_document(
        path,
        datum,
        database,
        collection_name,
        post_elog=post_elog,
    )

    expected_algorithms = set()
    if with_datum_log:
        expected_algorithms.add("datum_log")
    if with_conversion_log:
        expected_algorithms.add("conversion_log")

    if post_elog:
        assert "elog_id" not in persisted
        assert database["elog"].count_documents({}) == 0
        if expected_algorithms:
            embedded = persisted["error_log"]["logdata"]
            assert {entry["algorithm"] for entry in embedded} == expected_algorithms
            assert len(embedded) == len(expected_algorithms)
        else:
            assert "error_log" not in persisted
    else:
        assert "error_log" not in persisted
        if expected_algorithms:
            assert isinstance(persisted["elog_id"], ObjectId)
            elog_document = database["elog"].find_one({"_id": persisted["elog_id"]})
            assert elog_document is not None
            assert {
                entry["algorithm"] for entry in elog_document["logdata"]
            } == expected_algorithms
            assert len(elog_document["logdata"]) == len(expected_algorithms)
            assert database["elog"].count_documents({}) == 1
        else:
            assert "elog_id" not in persisted
            assert database["elog"].count_documents({}) == 0
