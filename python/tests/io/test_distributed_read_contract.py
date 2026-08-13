import datetime
import os
import subprocess
import sys
from importlib.metadata import distribution, version
from pathlib import Path

import dask.bag
import pytest
from bson import ObjectId, json_util
from pymongo import ASCENDING, DESCENDING

import mspasspy.io.distributed as distributed


class FakeCursor:
    def __init__(self, collection, documents):
        self.collection = collection
        self.documents = documents

    def __iter__(self):
        return iter(self.documents)

    def sort(self, sort_clause):
        self.collection.sort_clauses.append(sort_clause)
        return self


class FakeCollection:
    def __init__(self, documents):
        self.documents = list(documents)
        self.find_queries = []
        self.sort_clauses = []

    def find(self, query):
        self.find_queries.append(query.copy())
        return FakeCursor(self, self.documents)


class FakeDatum:
    live = True


class FakeEnsemble:
    def __init__(self):
        self.member = [FakeDatum()]

    def kill(self):
        raise AssertionError("an ensemble with a live member must not be killed")


class FakeDatabase:
    def __init__(self, documents, fail_on_read=False):
        self.collection = FakeCollection(documents)
        self.fail_on_read = fail_on_read
        self.name = "fake_database"

    def __getitem__(self, collection):
        assert collection == "wf_TimeSeries"
        return self.collection

    def read_data(self, document, **kwargs):
        if self.fail_on_read:
            raise RuntimeError("controlled read failure")
        if isinstance(document, FakeCursor):
            return FakeEnsemble()
        return document


def _assert_module_from_selected_build(module, relative_path):
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if source_root:
        expected_module = Path(source_root) / relative_path
    else:
        expected_module = distribution("mspasspy").locate_file(relative_path)
        installed_version = version("mspasspy")
        installed_commit = installed_version.partition("+g")[2].partition(".")[0]
        assert installed_commit, "installed mspasspy version lacks a source commit"
        repository_root = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / ".git").exists()
        )
        checkout_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, text=True
        ).strip()
        assert checkout_commit.startswith(installed_commit)
    assert Path(module.__file__).resolve() == Path(expected_module).resolve()


def test_contract_suite_loads_distributed_from_selected_build():
    _assert_module_from_selected_build(distributed, Path("mspasspy/io/distributed.py"))


@pytest.mark.parametrize(
    "sort_clause",
    [
        ("starttime", ASCENDING),
        "starttime",
        {"starttime": ASCENDING},
        [["starttime", ASCENDING]],
        [("starttime",)],
        [("starttime", ASCENDING, "extra")],
        [(1, ASCENDING)],
        [("", ASCENDING)],
        [("starttime", True)],
        [("starttime", 0)],
        [("starttime", "ascending")],
    ],
)
def test_invalid_truthy_sort_clauses_fail_before_query(monkeypatch, sort_clause):
    database = FakeDatabase([])
    monkeypatch.setattr(distributed, "Database", FakeDatabase)

    with pytest.raises(TypeError):
        distributed.read_distributed_data(
            [{"sta": "AAA"}],
            db=database,
            scheduler="dask",
            npartitions=1,
            sort_clause=sort_clause,
        )

    assert database.collection.find_queries == []


def test_valid_sort_pairs_reach_cursor_sort_after_lazy_compute(monkeypatch):
    database = FakeDatabase([])
    monkeypatch.setattr(distributed, "Database", FakeDatabase)
    monkeypatch.setattr(distributed, "fetch_dbhandle", lambda _: database)
    sort_clause = [("sta", ASCENDING), ("starttime", DESCENDING)]

    result = distributed.read_distributed_data(
        [{"sta": "AAA"}],
        db=database,
        scheduler="dask",
        npartitions=1,
        sort_clause=sort_clause,
    )

    assert isinstance(result, dask.bag.Bag)
    assert database.collection.find_queries == []
    computed = result.compute(scheduler="synchronous")
    assert len(computed) == 1
    assert isinstance(computed[0], FakeEnsemble)
    assert database.collection.find_queries == [{"sta": "AAA"}]
    assert database.collection.sort_clauses == [sort_clause]


def make_documents():
    return [
        {
            "_id": ObjectId("64b917ce9aa746564e8ecbfd"),
            "starttime": datetime.datetime(2020, 1, 2, 3, 4, 5),
            "value": 1,
        },
        {
            "_id": ObjectId("64b917d69aa746564e8ecbfe"),
            "starttime": datetime.datetime(2021, 2, 3, 4, 5, 6),
            "value": 2,
        },
    ]


def test_dask_scratch_is_extended_json_lines_lazy_and_caller_owned(
    monkeypatch, tmp_path
):
    documents = make_documents()
    database = FakeDatabase(documents)
    monkeypatch.setattr(distributed, "Database", FakeDatabase)
    scratch = tmp_path / "waveforms.jsonl"

    result = distributed.read_distributed_data(
        database,
        scratchfile=str(scratch),
        scheduler="dask",
        npartitions=1,
    )

    assert isinstance(result, dask.bag.Bag)
    assert scratch.exists()
    lines = scratch.read_text().splitlines()
    assert len(lines) == 2
    assert [json_util.loads(line) for line in lines] == documents
    assert result.compute(scheduler="synchronous") == documents
    assert result.compute(scheduler="synchronous") == documents
    assert scratch.exists()
    scratch.unlink()
    assert not scratch.exists()


def test_dask_compute_failure_never_deletes_caller_scratch(monkeypatch, tmp_path):
    database = FakeDatabase(make_documents(), fail_on_read=True)
    monkeypatch.setattr(distributed, "Database", FakeDatabase)
    scratch = tmp_path / "failing.jsonl"
    result = distributed.read_distributed_data(
        database,
        scratchfile=str(scratch),
        scheduler="dask",
        npartitions=1,
    )

    with pytest.raises(RuntimeError, match="controlled read failure"):
        result.compute(scheduler="synchronous")

    assert scratch.exists()
    scratch.unlink()


@pytest.fixture(scope="module")
def spark_context():
    pyspark = pytest.importorskip("pyspark")
    active_context = pyspark.SparkContext._active_spark_context
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    previous_worker_python = os.environ.get("PYSPARK_PYTHON")
    previous_driver_python = os.environ.get("PYSPARK_DRIVER_PYTHON")
    previous_pythonpath = os.environ.get("PYTHONPATH")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    test_module_directory = str(Path(__file__).parent)
    os.environ["PYTHONPATH"] = (
        test_module_directory
        if previous_pythonpath is None
        else test_module_directory + os.pathsep + previous_pythonpath
    )
    configuration = (
        pyspark.SparkConf()
        .setMaster("local[1]")
        .setAppName("mspass-distributed-read-contract")
        .set("spark.ui.enabled", "false")
        .set("spark.driver.bindAddress", "127.0.0.1")
    )
    context = pyspark.SparkContext.getOrCreate(configuration)
    context.addPyFile(str(Path(__file__).resolve()))
    context.setLogLevel("ERROR")
    try:
        yield context
    finally:
        if active_context is None:
            context.stop()
        if previous_worker_python is None:
            os.environ.pop("PYSPARK_PYTHON", None)
        else:
            os.environ["PYSPARK_PYTHON"] = previous_worker_python
        if previous_driver_python is None:
            os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
        else:
            os.environ["PYSPARK_DRIVER_PYTHON"] = previous_driver_python
        if previous_pythonpath is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = previous_pythonpath


def test_spark_scratch_returns_repeatable_rdd_and_remains_caller_owned(
    monkeypatch, tmp_path, spark_context
):
    documents = make_documents()
    database = FakeDatabase(documents)
    monkeypatch.setattr(distributed, "Database", FakeDatabase)
    scratch = tmp_path / "spark-waveforms.jsonl"

    result = distributed.read_distributed_data(
        database,
        scratchfile=str(scratch),
        scheduler="spark",
        spark_context=spark_context,
        npartitions=1,
    )

    from pyspark import RDD

    assert isinstance(result, RDD)
    assert result.collect() == documents
    assert result.collect() == documents
    assert scratch.exists()
    scratch.unlink()
    assert not scratch.exists()
