import os
from pathlib import Path
from unittest.mock import Mock

import pytest
from bson import BSON, ObjectId
from pymongo.results import DeleteResult, InsertOneResult

import mspasspy.db.spectrumdb as spectrumdb_module
import mspasspy.ccore.seismic as seismic_binding
from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum
from mspasspy.ccore.utility import ErrorSeverity, Metadata, MsPASSError
from mspasspy.db.spectrumdb import SpectrumDatabase
from mspasspy.db.serialization import TYPE_KEY, VERSION, VERSION_KEY


class MemoryCollection:
    """Small controllable collection implementing the CRUD calls under test."""

    def __init__(self):
        self.documents = []
        self.insert_calls = 0

    def insert_one(self, document):
        self.insert_calls += 1
        stored = dict(document)
        stored.setdefault("_id", ObjectId())
        self.documents.append(stored)
        return InsertOneResult(stored["_id"], acknowledged=True)

    def find_one(self, query):
        return next(
            (
                dict(document)
                for document in self.documents
                if self._matches(document, query)
            ),
            None,
        )

    def find(self, query):
        return [
            dict(document)
            for document in self.documents
            if self._matches(document, query)
        ]

    def delete_one(self, query):
        for index, document in enumerate(self.documents):
            if self._matches(document, query):
                del self.documents[index]
                return DeleteResult({"n": 1, "ok": 1.0}, acknowledged=True)
        return DeleteResult({"n": 0, "ok": 1.0}, acknowledged=True)

    @staticmethod
    def _matches(document, query):
        return all(document.get(key) == value for key, value in query.items())


class MemoryDatabase:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, MemoryCollection())


def _build_database(monkeypatch, *args, **kwargs):
    database = MemoryDatabase()
    client = Mock()
    client.get_database.return_value = database
    monkeypatch.setattr(spectrumdb_module, "DBClient", Mock(return_value=client))
    handle = SpectrumDatabase("spectra", *args, **kwargs)
    return handle, client, database


def _configured_handle():
    handle = SpectrumDatabase.__new__(SpectrumDatabase)
    handle.type_list = [PowerSpectrum]
    handle.collection = MemoryCollection()
    return handle


def _live_spectrum():
    spectrum = PowerSpectrum(
        Metadata(),
        DoubleVector([1.0, 4.0, 9.0]),
        0.25,
        "contract-test",
        0.0,
        1.0,
        8,
    )
    spectrum["station"] = "AAA"
    spectrum["quality"] = 7
    return spectrum


def test_contract_suite_uses_worktree_module_and_real_binding():
    selected_source = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if selected_source:
        expected_module = Path(selected_source) / "mspasspy" / "db" / "spectrumdb.py"
        assert Path(spectrumdb_module.__file__).resolve() == expected_module.resolve()
    assert Path(seismic_binding.__file__).suffix == ".so"


def test_constructor_forwards_get_database_arguments_unchanged(monkeypatch):
    sentinel_schema = object()
    sentinel_codec = object()
    sentinel_read_preference = object()
    handle, client, database = _build_database(
        monkeypatch,
        sentinel_schema,
        sentinel_codec,
        read_preference=sentinel_read_preference,
        collection="custom_spectra",
    )

    client.get_database.assert_called_once_with(
        "spectra",
        sentinel_schema,
        sentinel_codec,
        read_preference=sentinel_read_preference,
    )
    assert handle.collection is database["custom_spectra"]


def test_live_save_read_and_delete_roundtrip(monkeypatch):
    handle, _, _ = _build_database(monkeypatch)
    original = _live_spectrum()

    oid = handle.save_data(original)

    assert isinstance(oid, ObjectId)
    stored = handle.collection.find_one({"_id": oid})
    assert stored["station"] == "AAA"
    assert stored["serialized_data"][TYPE_KEY] == "PowerSpectrum"
    assert stored["serialized_data"][VERSION_KEY] == VERSION
    BSON.encode(stored)
    restored = handle.read_data(oid)
    assert isinstance(restored, PowerSpectrum)
    assert restored.live()
    assert dict(restored) == dict(original)
    assert list(restored.spectrum) == pytest.approx([1.0, 4.0, 9.0])
    assert restored.df() == pytest.approx(0.25)
    assert restored.f0() == pytest.approx(0.0)
    assert restored.dt() == pytest.approx(1.0)

    handle.delete_data({"_id": oid})
    assert handle.collection.find_one({"_id": oid}) is None
    with pytest.raises(MsPASSError) as excinfo:
        handle.read_data(oid)
    assert excinfo.value.severity == ErrorSeverity.Invalid


def test_save_path_independently_validates_the_input_object():
    handle = _configured_handle()
    spectrum = _live_spectrum()

    oid = handle.save_data(spectrum)

    assert isinstance(oid, ObjectId)
    assert handle.collection.insert_calls == 1


def test_delete_path_independently_removes_the_selected_record():
    handle = _configured_handle()
    first = handle.collection.insert_one({"serialized_data": b"first"}).inserted_id
    second = handle.collection.insert_one({"serialized_data": b"second"}).inserted_id

    handle.delete_data(first)

    assert handle.collection.find_one({"_id": first}) is None
    assert handle.collection.find_one({"_id": second}) is not None


def test_dead_save_returns_identity_and_performs_no_write(monkeypatch):
    handle, _, _ = _build_database(monkeypatch)
    dead = PowerSpectrum()
    dead.kill()

    result = handle.save_data(dead)

    assert result is dead
    assert handle.collection.insert_calls == 0
    assert handle.collection.documents == []


def test_save_rejects_executable_pickle_format(monkeypatch):
    handle, _, _ = _build_database(monkeypatch)

    with pytest.raises(MsPASSError) as excinfo:
        handle.save_data(_live_spectrum(), format="pickle")

    assert excinfo.value.severity == ErrorSeverity.Invalid
    assert handle.collection.insert_calls == 0


@pytest.mark.parametrize("value", (None, 1, "spectrum", {}, object()))
def test_save_wrong_type_is_fatal(monkeypatch, value):
    handle, _, _ = _build_database(monkeypatch)

    with pytest.raises(MsPASSError) as excinfo:
        handle.save_data(value)

    assert excinfo.value.severity == ErrorSeverity.Fatal
    assert handle.collection.insert_calls == 0


def test_read_missing_is_invalid(monkeypatch):
    handle, _, _ = _build_database(monkeypatch)

    with pytest.raises(MsPASSError) as excinfo:
        handle.read_data(ObjectId())

    assert excinfo.value.severity == ErrorSeverity.Invalid


def test_verify_empty_all_valid_and_mixed(monkeypatch):
    handle, _, _ = _build_database(monkeypatch)
    assert handle.verify() == [0, 0]

    first = handle.save_data(_live_spectrum())
    second = handle.save_data(_live_spectrum())
    assert handle.verify() == [2, 2]
    assert handle.verify(required=["station", "quality"]) == [2, 2]

    handle.collection.documents.append({"_id": ObjectId(), "station": "BAD"})
    handle.collection.documents.append(
        {"_id": ObjectId(), "serialized_data": b"not-used", "station": "PARTIAL"}
    )
    assert handle.verify() == [4, 3]
    assert handle.verify(required=["station", "quality"]) == [4, 2]
    assert handle.verify(query={"_id": first}) == [1, 1]
    assert handle.verify(query={"_id": second}) == [1, 1]


def test_verify_independently_counts_each_scanned_document():
    handle = _configured_handle()
    handle.collection.documents.extend(
        [
            {"serialized_data": b"first", "required": True},
            {"serialized_data": b"second"},
            {"required": True},
        ]
    )

    assert handle.verify() == [3, 2]
    assert handle.verify(required=["required"]) == [3, 1]
