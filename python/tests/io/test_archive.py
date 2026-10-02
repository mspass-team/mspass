"""Archive contracts using native waveform I/O and mocked MongoDB boundaries."""

from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import MagicMock

import numpy as np
import pytest
from bson import ObjectId, json_util
from pymongo.collection import Collection

from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import MsPASSError
from mspasspy.io import archive


def ensemble(kind=TimeSeries, count=3):
    result = TimeSeriesEnsemble() if kind is TimeSeries else SeismogramEnsemble()
    for index in range(count):
        datum = kind(index + 3)
        datum.dt = 0.25
        datum.t0 = 1234.0 + index
        datum.set_live()
        values = np.arange(datum.npts, dtype=float) + index * 10
        if kind is TimeSeries:
            np.asarray(datum.data)[:] = values
        else:
            np.asarray(datum.data)[:] = np.vstack([values, values + 100, values + 200])
        datum["marker"] = index
        datum["source_id"] = ObjectId()
        datum["nested"] = {"items": [1, "two"], "id": ObjectId()}
        result.member.append(datum)
    result.set_live()
    return result


def documents(path):
    return json_util.loads(path.read_text())


@pytest.mark.parametrize("kind", [TimeSeries, Seismogram])
@pytest.mark.parametrize("mixed", [False, True])
def test_native_archive_round_trip(tmp_path, kind, mixed):
    original = ensemble(kind)
    if mixed:
        original.member[1].kill()
    expected = [datum for datum in original.member if datum.live]
    base = tmp_path / "waveforms"
    assert archive.save_to_archive_files(original, base) == len(expected)
    metadata = documents(base.with_suffix(".json"))
    stride = 8 if kind is TimeSeries else 24
    offsets = np.cumsum([0] + [stride * datum.npts for datum in expected[:-1]]).tolist()
    assert [doc["foff"] for doc in metadata] == offsets
    assert all(doc["atomic_data_type"] == kind.__name__ for doc in metadata)
    assert base.with_suffix(".dat").stat().st_size == stride * sum(
        d.npts for d in expected
    )
    restored = archive.read_from_archive_file(base)
    assert restored.live
    assert len(restored.member) == len(expected)
    for before, after in zip(expected, restored.member):
        assert after.live
        assert isinstance(after, kind)
        np.testing.assert_array_equal(np.asarray(after.data), np.asarray(before.data))
        assert after.dt == before.dt
        assert after.t0 == before.t0
        assert after["source_id"] == before["source_id"]
        assert after["nested"] == before["nested"]


def test_overwrite_resets_offsets_and_sample_file(tmp_path):
    base = tmp_path / "waveforms"
    archive.save_to_archive_files(ensemble(count=3), base)
    previous = base.with_suffix(".dat").read_bytes()
    with pytest.raises(FileExistsError):
        archive.save_to_archive_files(ensemble(count=1), base)
    assert base.with_suffix(".dat").read_bytes() == previous
    assert archive.save_to_archive_files(ensemble(count=1), base, overwrite=True) == 1
    assert base.with_suffix(".dat").stat().st_size == 3 * 8
    assert documents(base.with_suffix(".json"))[0]["foff"] == 0
    assert len(archive.read_from_archive_file(base).member) == 1


def test_dead_archives_do_not_create_files(tmp_path):
    data = ensemble()
    data.kill()
    assert archive.save_to_archive_files(data, tmp_path / "dead") == 0
    data.set_live()
    for member in data.member:
        member.kill()
    assert archive.save_to_archive_files(data, tmp_path / "empty") == 0
    assert list(tmp_path.iterdir()) == []


def matches(doc, query):
    if "$or" in query:
        return any(matches(doc, clause) for clause in query["$or"])
    if "$and" in query:
        return all(matches(doc, clause) for clause in query["$and"])
    return all(doc.get(key) == value for key, value in query.items())


def collection(docs):
    result = MagicMock(spec=Collection)
    result.find.side_effect = lambda query: iter(
        deepcopy([d for d in docs if matches(d, query)])
    )
    result.count_documents.side_effect = lambda query: sum(
        matches(d, query) for d in docs
    )
    result.distinct.side_effect = lambda key: list({d[key] for d in docs if key in d})
    result.name = "wf_TimeSeries"
    return result


def test_index_preserves_selection_and_groups_relative_absolute_paths(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    waveform = tmp_path / "data" / "samples.dat"
    waveform.write_bytes(b"")
    oid = ObjectId()
    date = datetime(2026, 1, 2, tzinfo=timezone.utc)
    common = {
        "dfile": waveform.name,
        "storage_mode": "file",
        "data_tag": "selected",
        "source_id": oid,
        "date": date,
    }
    docs = [
        dict(common, dir="data", foff=0),
        dict(common, dir=str(waveform.parent), foff=24, tmatrix=list(np.eye(3).flat)),
        dict(common, dir="data", foff=48, data_tag="excluded"),
        dict(common, dir="data", foff=72, storage_mode="gridfs"),
        dict(common, dir="missing", foff=96),
    ]
    db = collection(docs)
    query = {"data_tag": "selected"}
    before = deepcopy(query)
    assert archive.create_archive_index(db, query=query) == 1
    assert query == before
    result = documents(waveform.with_suffix(".json"))
    assert [doc["foff"] for doc in result] == [0, 24]
    assert [doc["atomic_data_type"] for doc in result] == ["TimeSeries", "Seismogram"]
    assert all(doc["source_id"] == oid for doc in result)
    assert all(doc["date"] == date.replace(tzinfo=None) for doc in result)
    assert all(doc["dir"] == "." and doc["datatype"] == "f8" for doc in result)


@pytest.mark.parametrize(
    "count, block_size, names",
    [
        (0, 2, []),
        (2, 2, ["gridfs_archive"]),
        (5, 2, ["gridfs_archive_1", "gridfs_archive_2", "gridfs_archive_3"]),
    ],
)
def test_gridfs_chunking_uses_database_reader(tmp_path, count, block_size, names):
    docs = [{"storage_mode": "gridfs", "marker": n} for n in range(count)]
    db = collection(docs)

    def read(doc, collection):
        assert collection == "wf_TimeSeries"
        assert isinstance(doc, dict)
        datum = TimeSeries(ensemble(count=1).member[0])
        datum["marker"] = doc["marker"]
        return datum

    db.database.read_data.side_effect = read
    assert archive.archive_gridfs_data(
        db, output_directory=tmp_path, objects_per_file=block_size
    ) == len(names)
    assert db.database.read_data.call_count == count
    recovered = []
    for name in names:
        recovered.extend(
            d["marker"] for d in archive.read_from_archive_file(tmp_path / name).member
        )
    assert recovered == list(range(count))
    assert len(list(tmp_path.glob("*.json"))) == len(names)


@pytest.mark.parametrize("size", [0, -1, 1.5])
def test_gridfs_rejects_invalid_chunk_size(size, tmp_path):
    with pytest.raises(ValueError, match="positive integer"):
        archive.archive_gridfs_data(
            collection([]), output_directory=tmp_path, objects_per_file=size
        )


@pytest.mark.parametrize("content", ["", "   \n", "[]", "{}"])
def test_reader_rejects_empty_metadata(tmp_path, content):
    base = tmp_path / "bad"
    base.with_suffix(".dat").write_bytes(b"")
    base.with_suffix(".json").write_text(content)
    with pytest.raises(ValueError):
        archive.read_from_archive_file(base)


def test_reader_handles_multiline_json_and_rejects_unsupported_datatype(tmp_path):
    base = tmp_path / "pretty"
    archive.save_to_archive_files(ensemble(count=2), base)
    metadata = documents(base.with_suffix(".json"))
    base.with_suffix(".json").write_text(
        "\n" + json_util.dumps(metadata, indent=2) + "\n\n"
    )
    assert len(archive.read_from_archive_file(base).member) == 2
    metadata[1]["datatype"] = "f4"
    base.with_suffix(".json").write_text(json_util.dumps(metadata))
    with pytest.raises(ValueError, match="datatype f8"):
        archive.read_from_archive_file(base)


def test_gridfs_query_sort_and_dead_blocks(tmp_path):
    docs = [
        {"storage_mode": "gridfs", "data_tag": "chosen", "marker": n} for n in range(3)
    ]
    docs.append({"storage_mode": "file", "data_tag": "chosen", "marker": 9})
    db = collection(docs)
    cursor = MagicMock()
    cursor.sort.return_value = iter(docs[:3])
    db.find.return_value = cursor
    db.find.side_effect = None

    def read(doc, collection):
        datum = TimeSeries(ensemble(count=1).member[0])
        if doc["marker"] < 2:
            datum.kill()
        return datum

    db.database.read_data.side_effect = read
    query = {"data_tag": "chosen"}
    assert (
        archive.archive_gridfs_data(
            db,
            query=query,
            sort=[("marker", 1)],
            output_directory=tmp_path,
            objects_per_file=2,
        )
        == 1
    )
    db.find.assert_called_once_with({"$and": [query, {"storage_mode": "gridfs"}]})
    cursor.sort.assert_called_once_with([("marker", 1)])
    assert not (tmp_path / "gridfs_archive_1.dat").exists()
    assert (tmp_path / "gridfs_archive_2.dat").exists()
    assert query == {"data_tag": "chosen"}


@pytest.fixture
def mongo_database():
    import os
    import uuid
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError
    from mspasspy.db.client import DBClient
    from mspasspy.db.database import Database

    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    probe = MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        probe.admin.command("ping")
    except ServerSelectionTimeoutError as error:
        pytest.skip(f"MongoDB unavailable: {error}")
    finally:
        probe.close()
    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    name = "test_archive_" + uuid.uuid4().hex
    try:
        yield Database(client, name)
    finally:
        client.drop_database(name)
        client.close()


@pytest.mark.parametrize("kind", [TimeSeries, Seismogram])
def test_real_gridfs_native_archive_round_trip(mongo_database, tmp_path, kind):
    original = ensemble(kind, count=3)
    collection_name = "wf_" + kind.__name__
    for datum in original.member:
        mongo_database.save_data(
            datum, storage_mode="gridfs", mode="promiscuous", collection=collection_name
        )
    assert (
        archive.archive_gridfs_data(
            mongo_database[collection_name],
            output_directory=tmp_path,
            objects_per_file=2,
        )
        == 2
    )
    restored = []
    for n in [1, 2]:
        result = archive.read_from_archive_file(tmp_path / f"gridfs_archive_{n}")
        restored.extend(kind(member) for member in result.member)
    assert len(restored) == 3
    for before, after in zip(original.member, restored):
        assert after.live
        np.testing.assert_array_equal(np.asarray(before.data), np.asarray(after.data))
        assert after["marker"] == before["marker"]
        assert isinstance(after, kind)
        assert after.dt == before.dt
        assert after.t0 == before.t0
        assert after["source_id"] == before["source_id"]
        assert after["nested"] == before["nested"]


@pytest.mark.parametrize("docs", [[], [{"storage_mode": "gridfs"}]])
def test_index_reports_missing_directory_metadata(docs):
    with pytest.raises(MsPASSError, match="no documents with the dir"):
        archive.create_archive_index(collection(docs))


def test_archive_invalid_query_and_format_raise_domain_error():
    with pytest.raises(MsPASSError, match="query argument"):
        archive.archive_gridfs_data(collection([]), query="invalid")
    with pytest.raises(MsPASSError, match="binary raw format"):
        archive.update_document_for_json_output({"format": "mseed"})
