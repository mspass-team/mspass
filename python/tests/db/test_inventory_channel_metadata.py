"""Offline save_inventory tests: real StationXML and mocked Mongo persistence."""

import copy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from bson import ObjectId
from obspy import read_inventory
from obspy.core.inventory import Equipment

from mspasspy.db.database import Database
from mspasspy.db.serialization import decode_channel, decode_inventory


def collection_mock():
    """Implement only persistence operations; retain real duplicate queries."""
    documents = []
    collection = MagicMock()

    def matches(query):
        return [
            doc for doc in documents if all(doc.get(k) == v for k, v in query.items())
        ]

    def insert(document):
        document.setdefault("_id", ObjectId())
        documents.append(copy.deepcopy(document))
        return SimpleNamespace(inserted_id=document["_id"])

    def update(query, change):
        for document in matches(query):
            document.update(copy.deepcopy(change["$set"]))

    def find(query, **kwargs):
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.__iter__.side_effect = lambda: iter(copy.deepcopy(matches(query)))
        return cursor

    collection.insert_one.side_effect = insert
    collection.update_one.side_effect = update
    collection.count_documents.side_effect = lambda query: len(matches(query))
    collection.find.side_effect = find
    collection.documents = documents
    return collection


@pytest.fixture
def database():
    # Bind the production methods without constructing a MongoClient or connecting.
    database = SimpleNamespace(site=collection_mock(), channel=collection_mock())
    for name in (
        "save_inventory",
        "_site_is_not_in_db",
        "_channel_is_not_in_db",
        "_handle_null_starttime",
        "_handle_null_endtime",
    ):
        setattr(database, name, getattr(Database, name).__get__(database))
    database._extract_locdata = Database._extract_locdata
    return database


@pytest.fixture
def inventory():
    path = Path(__file__).resolve().parents[1] / "data" / "TA.035A.xml"
    inventory = read_inventory(str(path))
    inventory.networks = inventory.networks[:1]
    inventory[0].stations = inventory[0].stations[:1]
    inventory[0][0].channels = inventory[0][0].channels[:1]
    return inventory


@pytest.mark.parametrize(
    "sensor,expected",
    [
        (
            Equipment(
                description="broadband sensor",
                model="STS-2",
                manufacturer="Streckeisen",
            ),
            {
                "sensor_description": "broadband sensor",
                "sensor_model": "STS-2",
                "sensor_manufacturer": "Streckeisen",
            },
        ),
        (Equipment(description="sensor"), {"sensor_description": "sensor"}),
        (
            Equipment(model="model", manufacturer="maker"),
            {"sensor_model": "model", "sensor_manufacturer": "maker"},
        ),
        (Equipment(), {}),
        (None, {}),
    ],
    ids=["complete", "description-only", "model-manufacturer", "empty", "absent"],
)
def test_sensor_metadata_handles_optional_equipment(
    database, inventory, sensor, expected
):
    inventory[0][0][0].sensor = sensor
    assert database.save_inventory(inventory, networks_to_exclude=None) == (1, 1, 1, 1)
    document = database.channel.documents[0]
    assert {
        key: value for key, value in document.items() if key.startswith("sensor_")
    } == expected
    restored = decode_channel(document["serialized_channel_data"])
    assert restored.sensor == sensor


@pytest.mark.parametrize("rate", [40.0, 0.0, None], ids=["positive", "zero", "unknown"])
def test_sampling_metadata_handles_unknown_and_zero_rate(database, inventory, rate):
    inventory[0][0][0].sample_rate = rate
    inventory[0][0][0].sensor = None
    assert database.save_inventory(inventory, networks_to_exclude=None) == (1, 1, 1, 1)
    document = database.channel.documents[0]
    assert document["sampling_rate"] == rate
    if rate:
        assert document["delta"] == pytest.approx(1.0 / rate)
    else:
        assert "delta" not in document
    assert decode_channel(document["serialized_channel_data"]).sample_rate == rate


def test_channel_metadata_and_serialization_remain_channel_specific(
    database, inventory
):
    station = inventory[0][0]
    template = station[0]
    station.channels = [copy.deepcopy(template) for _ in range(3)]
    sensors = [
        Equipment(description="first", model="A", manufacturer="maker"),
        Equipment(model="B"),
        None,
    ]
    for index, channel in enumerate(station.channels):
        channel.code = ["BHZ", "BHN", "BHE"][index]
        channel.sample_rate = [20.0, 40.0, 100.0][index]
        channel.sensor = sensors[index]
    original = copy.deepcopy(inventory)

    assert database.save_inventory(inventory, networks_to_exclude=None) == (1, 3, 1, 3)
    assert inventory == original
    site = database.site.documents[0]
    assert decode_inventory(site["serialized_inventory"]) == original
    assert "sampling_rate" not in site and "sensor_description" not in site
    for document, channel in zip(database.channel.documents, station.channels):
        assert document["chan"] == channel.code
        assert document["sampling_rate"] == channel.sample_rate
        assert document["delta"] == pytest.approx(1.0 / channel.sample_rate)
        assert "serialized_inventory" not in document
        assert decode_channel(document["serialized_channel_data"]) == channel
    assert database.channel.documents[0]["sensor_description"] == "first"
    assert database.channel.documents[1]["sensor_model"] == "B"
    assert "sensor_description" not in database.channel.documents[1]
    assert not any(key.startswith("sensor_") for key in database.channel.documents[2])
    before = copy.deepcopy((database.site.documents, database.channel.documents))
    assert database.save_inventory(inventory, networks_to_exclude=None) == (0, 0, 1, 3)
    assert (database.site.documents, database.channel.documents) == before
    assert database.site.insert_one.call_count == 1
    assert database.channel.insert_one.call_count == 3
