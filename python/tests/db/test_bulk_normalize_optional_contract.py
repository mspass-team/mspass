import copy

import pytest

from mspasspy.ccore.seismic import TimeSeries
from mspasspy.ccore.utility import Metadata
import mspasspy.db.normalize as normalize_module
from mspasspy.db.normalize import BasicMatcher, bulk_normalize, normalize


class FakeCursor(list):
    def close(self):
        self.closed = True


class FakeCollection:
    def __init__(self, documents):
        self.documents = copy.deepcopy(documents)

    def count_documents(self, query):
        return len(self.documents)

    def find(self, query):
        return FakeCursor(copy.deepcopy(self.documents))

    def bulk_write(self, operations):
        for operation in operations:
            for document in self.documents:
                if document["_id"] == operation.query["_id"]:
                    document.update(operation.update["$set"])


class FakeDatabase:
    def __init__(self, collection):
        self.collection = collection

    def __getitem__(self, name):
        assert name == "wf_test"
        return self.collection


class FakeUpdateOne:
    def __init__(self, query, update):
        self.query = query
        self.update = update


class OutputMatcher(BasicMatcher):
    def __init__(self, records, *, aliases=None, prepend_collection_name=False):
        super().__init__(
            attributes_to_load=["required"],
            load_if_defined=["optional"],
            aliases=aliases,
        )
        self.records = records
        self.collection = "source"
        self.prepend_collection_name = prepend_collection_name

    def _output_key(self, key):
        output_key = self.aliases.get(key, key)
        if self.prepend_collection_name:
            if output_key == "_id":
                return self.collection + output_key
            return self.collection + "_" + output_key
        return output_key

    def find_doc(self, document):
        record = self.records[document["match"]]
        result = {}
        for key in self.attributes_to_load:
            result[self._output_key(key)] = record[key]
        for key in self.load_if_defined:
            if key in record:
                result[self._output_key(key)] = record[key]
        return result

    def find_one(self, mspass_object):
        return [Metadata(self.find_doc(mspass_object)), None]

    def find(self, mspass_object):
        return [[self.find_one(mspass_object)[0]], None]


@pytest.mark.parametrize("prepend_collection_name", [False, True])
@pytest.mark.parametrize("use_aliases", [False, True])
def test_bulk_output_matches_per_object_required_and_optional_mapping(
    monkeypatch, prepend_collection_name, use_aliases
):
    monkeypatch.setattr(normalize_module.pymongo, "UpdateOne", FakeUpdateOne)
    input_documents = [
        {"_id": 1, "match": "present"},
        {"_id": 2, "match": "absent"},
        {"_id": 3, "match": "second-present"},
    ]
    collection = FakeCollection(input_documents)
    database = FakeDatabase(collection)
    records = {
        "present": {"required": 10, "optional": "first"},
        "absent": {"required": 20},
        "second-present": {"required": 30, "optional": "third"},
    }
    aliases = (
        {"required": "required_alias", "optional": "optional_alias"}
        if use_aliases
        else None
    )
    matcher = OutputMatcher(
        records,
        aliases=aliases,
        prepend_collection_name=prepend_collection_name,
    )
    optional_key = matcher._output_key("optional")
    expected = {}
    for document in input_documents:
        datum = TimeSeries(1)
        datum.set_live()
        for key, value in document.items():
            datum[key] = value
        normalized = normalize(datum, matcher)
        expected[document["_id"]] = matcher.find_doc(document)
        for key, value in expected[document["_id"]].items():
            assert normalized[key] == value
        assert normalized.is_defined(optional_key) == (
            "optional" in records[document["match"]]
        )

    result = bulk_normalize(
        database, wf_col="wf_test", matcher_list=[matcher], blocksize=2
    )

    assert result == [3, 3]
    assert len(collection.documents) == 3
    for document in collection.documents:
        output = {
            key: value for key, value in document.items() if key not in {"_id", "match"}
        }
        assert output == expected[document["_id"]]
    assert optional_key in collection.documents[0]
    assert optional_key not in collection.documents[1]
    assert optional_key in collection.documents[2]
