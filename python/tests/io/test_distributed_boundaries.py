import os
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import dask.bag
import pandas as pd
import pytest
from pymongo.errors import ServerSelectionTimeoutError
from pyspark import SparkConf, SparkContext
from pyspark.errors import PySparkRuntimeError

import mspasspy.io.distributed as distributed_module
from mspasspy.ccore.utility import ErrorLogger, ErrorSeverity, Metadata
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.io.distributed import (
    _partitioned_save_wfdoc,
    pyspark_mappartition_interface,
    read_to_dataframe,
)

SOURCE_PYTHON_ROOT = Path(
    os.environ.get("MSPASS_TEST_SOURCE_ROOT", Path(__file__).resolve().parents[2])
)
EXPECTED_MODULE = SOURCE_PYTHON_ROOT / "mspasspy/io/distributed.py"


class RecordingCollection:
    def __init__(self, spark_counter=None):
        self.insert_many_calls = 0
        self.spark_counter = spark_counter

    def insert_many(self, documents):
        self.insert_many_calls += 1
        if self.spark_counter is not None:
            self.spark_counter.add(1)
        return SimpleNamespace(inserted_ids=[])


class RecordingDatabase:
    def __init__(self, spark_counter=None):
        self.collection = RecordingCollection(spark_counter)

    def __getitem__(self, _collection):
        return self.collection


class ClosingCursor:
    def __init__(self, cursor):
        self.cursor = cursor
        self.close_calls = 0

    @property
    def collection(self):
        return self.cursor.collection

    def __iter__(self):
        return iter(self.cursor)

    def close(self):
        self.close_calls += 1
        self.cursor.close()


@pytest.fixture
def mongo_database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as error:
        client.close()
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    database_name = "issue_835_" + uuid.uuid4().hex
    database = Database(client, database_name)
    try:
        yield database
    finally:
        client.drop_database(database_name)
        client.close()


def test_contract_suite_loads_expected_worktree_module():
    assert Path(distributed_module.__file__).resolve() == EXPECTED_MODULE


def test_empty_dask_partition_returns_empty_without_insert():
    database = RecordingDatabase()
    bag = dask.bag.from_sequence([])

    result = bag.map_partitions(
        _partitioned_save_wfdoc, database, collection="wf_TimeSeries"
    ).compute(scheduler="synchronous")

    assert result == []
    assert database.collection.insert_many_calls == 0


def test_empty_spark_partition_adapter_returns_empty_without_insert():
    database = RecordingDatabase()
    interface = pyspark_mappartition_interface(database, "wf_TimeSeries")

    result = interface.partitioned_save_wfdoc(iter(()))

    assert result == []
    assert database.collection.insert_many_calls == 0


@pytest.fixture(scope="module")
def spark_context():
    previous_worker_python = os.environ.get("PYSPARK_PYTHON")
    previous_driver_python = os.environ.get("PYSPARK_DRIVER_PYTHON")
    previous_pythonpath = os.environ.get("PYTHONPATH")
    previous_spark_local_ip = os.environ.get("SPARK_LOCAL_IP")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    test_module_directory = str(Path(__file__).parent)
    os.environ["PYTHONPATH"] = (
        test_module_directory
        if previous_pythonpath is None
        else test_module_directory + os.pathsep + previous_pythonpath
    )
    active_context = SparkContext._active_spark_context
    conf = (
        SparkConf()
        .setMaster("local[2]")
        .setAppName("mspass-issue-835")
        .set("spark.ui.enabled", "false")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.driver.bindAddress", "127.0.0.1")
    )
    context = None
    owns_context = False
    try:
        try:
            context = SparkContext.getOrCreate(conf)
        except PySparkRuntimeError as error:
            pytest.skip(f"Local Spark runtime is unavailable: {error}")
        owns_context = active_context is None
        yield context
    finally:
        if owns_context and context is not None:
            context.stop()
        for key, previous_value in (
            ("PYSPARK_PYTHON", previous_worker_python),
            ("PYSPARK_DRIVER_PYTHON", previous_driver_python),
            ("PYTHONPATH", previous_pythonpath),
            ("SPARK_LOCAL_IP", previous_spark_local_ip),
        ):
            if previous_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous_value


def test_real_empty_spark_partitions_return_empty_without_insert(spark_context):
    context = spark_context
    insert_counter = context.accumulator(0)
    database = RecordingDatabase(insert_counter)
    result = (
        context.parallelize([], 2)
        .mapPartitions(
            lambda partition: _partitioned_save_wfdoc(
                partition, database, collection="wf_TimeSeries"
            )
        )
        .collect()
    )

    assert result == []
    assert insert_counter.value == 0


def _controlled_doc2md(document, *_args, **_kwargs):
    elog = ErrorLogger()
    for message in document.get("errors", []):
        elog.log_error(
            "document-{}".format(document["seq"]),
            message,
            ErrorSeverity.Invalid,
        )
    metadata = Metadata({"seq": document["seq"]})
    return metadata, elog.size() == 0, elog


def test_empty_cursor_returns_only_empty_dataframe_and_closes(mongo_database, capsys):
    cursor = ClosingCursor(mongo_database["wf_TimeSeries"].find({}))

    result = read_to_dataframe(mongo_database, cursor)

    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, pd.DataFrame())
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert cursor.close_calls == 1


def test_one_earlier_conversion_error_is_printed_once_and_cursor_closes(
    mongo_database, monkeypatch, capsys
):
    mongo_database["wf_TimeSeries"].insert_many(
        [
            {"seq": 0, "errors": ["only-error"]},
            {"seq": 1, "errors": []},
        ]
    )
    cursor = ClosingCursor(mongo_database["wf_TimeSeries"].find({}).sort("seq", 1))
    monkeypatch.setattr(distributed_module, "doc2md", _controlled_doc2md)

    result = read_to_dataframe(mongo_database, cursor)

    assert result["seq"].tolist() == [1]
    output = capsys.readouterr().out
    assert output.count("only-error") == 1
    assert "1  errors were handled" in output
    assert cursor.close_calls == 1


def test_multiple_conversion_errors_are_printed_once_in_input_order(
    mongo_database, monkeypatch, capsys
):
    mongo_database["wf_TimeSeries"].insert_many(
        [
            {"seq": 0, "errors": ["first-error-a", "first-error-b"]},
            {"seq": 1, "errors": []},
            {"seq": 2, "errors": ["last-error"]},
        ]
    )
    cursor = ClosingCursor(mongo_database["wf_TimeSeries"].find({}).sort("seq", 1))
    monkeypatch.setattr(distributed_module, "doc2md", _controlled_doc2md)

    result = read_to_dataframe(mongo_database, cursor)

    assert result["seq"].tolist() == [1]
    output = capsys.readouterr().out
    for message in ("first-error-a", "first-error-b", "last-error"):
        assert output.count(message) == 1
    assert output.index("first-error-a") < output.index("first-error-b")
    assert output.index("first-error-b") < output.index("last-error")
    assert "3  errors were handled" in output
    assert cursor.close_calls == 1


def test_conversion_exception_closes_cursor_and_propagates_original(
    mongo_database, monkeypatch
):
    mongo_database["wf_TimeSeries"].insert_one({"seq": 0})
    cursor = ClosingCursor(mongo_database["wf_TimeSeries"].find({}))
    original_error = RuntimeError("injected conversion failure")

    def fail_conversion(*_args, **_kwargs):
        raise original_error

    monkeypatch.setattr(distributed_module, "doc2md", fail_conversion)

    with pytest.raises(RuntimeError) as caught:
        read_to_dataframe(mongo_database, cursor)

    assert caught.value is original_error
    assert cursor.close_calls == 1
