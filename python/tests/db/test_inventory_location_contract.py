import copy
import os
import uuid

import pytest
from obspy import UTCDateTime
from obspy.core.inventory import Channel, Inventory, Network, Response, Station
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.db.serialization import decode_channel, decode_inventory


@pytest.fixture
def database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    probe = MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        probe.admin.command("ping")
    except ServerSelectionTimeoutError as error:
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    finally:
        probe.close()
    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    name = "test_inventory_locations_" + uuid.uuid4().hex
    database = Database(client, name)
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


def make_channel(code, location, latitude, longitude, elevation, depth, start, end):
    return Channel(
        code=code,
        location_code=location,
        latitude=latitude,
        longitude=longitude,
        elevation=elevation,
        depth=depth,
        azimuth=10.0,
        dip=-90.0,
        sample_rate=20.0,
        response=Response(),
        start_date=UTCDateTime(start),
        end_date=UTCDateTime(end),
    )


def make_multi_location_inventory():
    loc00 = {
        "latitude": 30.0,
        "longitude": -100.0,
        "elevation": 100.0,
        "depth": 1.0,
        "start": "2020-01-01",
        "end": "2021-01-01",
    }
    loc10 = {
        "latitude": 31.0,
        "longitude": -101.0,
        "elevation": 200.0,
        "depth": 2.0,
        "start": "2022-01-01",
        "end": "2023-01-01",
    }
    channels = [
        make_channel("BHZ", "00", **loc00),
        make_channel("BHN", "00", **loc00),
        make_channel("HHZ", "10", **loc10),
    ]
    station = Station(
        code="TEST",
        latitude=35.0,
        longitude=-105.0,
        elevation=500.0,
        channels=channels,
        start_date=UTCDateTime("2010-01-01"),
        end_date=UTCDateTime("2030-01-01"),
    )
    return Inventory(networks=[Network(code="XX", stations=[station])], source="test")


def test_extract_locdata_preserves_every_channel_in_its_location_group():
    channels = make_multi_location_inventory().networks[0].stations[0].channels
    grouped = Database._extract_locdata(channels)

    assert set(grouped) == {"00", "10"}
    assert grouped["00"] == channels[:2]
    assert grouped["10"] == channels[2:]


def test_save_inventory_groups_channels_only_under_their_own_location(database):
    inventory = make_multi_location_inventory()
    source_channels = inventory.networks[0].stations[0].channels
    source_state = [
        (
            id(channel),
            id(channel.response),
            channel.location_code,
            channel.code,
            channel.latitude,
            channel.longitude,
            channel.start_date,
            channel.end_date,
        )
        for channel in source_channels
    ]

    counts = database.save_inventory(inventory, networks_to_exclude=None)

    assert counts == (2, 3, 2, 3)
    assert [
        (
            id(channel),
            id(channel.response),
            channel.location_code,
            channel.code,
            channel.latitude,
            channel.longitude,
            channel.start_date,
            channel.end_date,
        )
        for channel in inventory.networks[0].stations[0].channels
    ] == source_state
    sites = list(database.site.find({"net": "XX", "sta": "TEST"}))
    assert len(sites) == 2
    sites_by_location = {document["loc"]: document for document in sites}
    assert set(sites_by_location) == {"00", "10"}
    expected_locations = {
        "00": (30.0, -100.0, 0.1, 1.0, "2020-01-01", "2021-01-01"),
        "10": (31.0, -101.0, 0.2, 2.0, "2022-01-01", "2023-01-01"),
    }
    for location, expected in expected_locations.items():
        latitude, longitude, elevation, depth, start, end = expected
        document = sites_by_location[location]
        assert document["lat"] == latitude
        assert document["lon"] == longitude
        assert document["elev"] == elevation
        assert document["edepth"] == depth
        assert document["starttime"] == UTCDateTime(start).timestamp
        assert document["endtime"] == UTCDateTime(end).timestamp
        restored = decode_inventory(document["serialized_inventory"])
        restored_channels = restored.networks[0].stations[0].channels
        assert {channel.location_code for channel in restored_channels} == {location}
        assert {channel.code for channel in restored_channels} == {
            channel.code
            for channel in source_channels
            if channel.location_code == location
        }

    channels = list(database.channel.find({"net": "XX", "sta": "TEST"}))
    assert len(channels) == 3
    assert {(document["loc"], document["chan"]) for document in channels} == {
        ("00", "BHZ"),
        ("00", "BHN"),
        ("10", "HHZ"),
    }
    source_by_key = {
        (channel.location_code, channel.code): channel for channel in source_channels
    }
    for document in channels:
        expected = expected_locations[document["loc"]]
        restored = decode_channel(document["serialized_channel_data"])
        source = source_by_key[(document["loc"], document["chan"])]
        assert restored.code == source.code
        assert restored.location_code == source.location_code
        assert restored.response == source.response
        assert document["lat"] == expected[0]
        assert document["lon"] == expected[1]
        assert document["starttime"] == UTCDateTime(expected[4]).timestamp
        assert document["endtime"] == UTCDateTime(expected[5]).timestamp


def test_conflicting_channel_geometry_uses_station_site_and_exact_channels(
    database, capsys
):
    channels = [
        make_channel("BHZ", "00", 30.0, -100.0, 100.0, 1.0, "2020-01-01", "2021-01-01"),
        make_channel("BHN", "00", 31.0, -101.0, 200.0, 2.0, "2019-01-01", "2022-01-01"),
    ]
    station = Station(
        code="TEST",
        latitude=35.0,
        longitude=-105.0,
        elevation=500.0,
        channels=channels,
    )
    inventory = Inventory(
        networks=[Network(code="XX", stations=[station])], source="test"
    )

    counts = database.save_inventory(inventory, networks_to_exclude=None)

    assert counts == (1, 2, 1, 2)
    output = capsys.readouterr().out
    assert output.count("channels sharing this location code have conflicting") == 1

    site = database.site.find_one({"net": "XX", "sta": "TEST", "loc": "00"})
    assert site["lat"] == 35.0
    assert site["lon"] == -105.0
    assert site["elev"] == 0.5
    assert site["coords"] == [-105.0, 35.0]
    assert site["location"]["coordinates"] == [-105.0, 35.0]
    assert site["starttime"] == UTCDateTime("2019-01-01").timestamp
    assert site["endtime"] == UTCDateTime("2022-01-01").timestamp
    assert "edepth" not in site

    documents = {
        document["chan"]: document
        for document in database.channel.find({"net": "XX", "sta": "TEST"})
    }
    assert set(documents) == {"BHZ", "BHN"}
    for channel in channels:
        document = documents[channel.code]
        assert document["lat"] == channel.latitude
        assert document["lon"] == channel.longitude
        assert document["elev"] == channel.elevation / 1000.0
        assert document["edepth"] == channel.depth
        assert document["coords"] == [channel.longitude, channel.latitude]
        assert document["location"]["coordinates"] == [
            channel.longitude,
            channel.latitude,
        ]
        assert document["starttime"] == channel.start_date.timestamp
        assert document["endtime"] == channel.end_date.timestamp
        restored = decode_channel(document["serialized_channel_data"])
        assert restored.code == channel.code
        assert restored.location_code == channel.location_code
        assert restored.latitude == channel.latitude
        assert restored.longitude == channel.longitude
        assert restored.elevation == channel.elevation
        assert restored.depth == channel.depth
        assert restored.response == channel.response


@pytest.mark.parametrize("location", ["00", ""])
def test_explicit_location_lookup_has_zero_one_and_ambiguous_results(
    database, location
):
    query_document = {
        "net": "XX",
        "sta": "TEST",
        "chan": "BHZ",
        "loc": location,
        "starttime": 0.0,
        "endtime": 100.0,
    }
    distractor = copy.deepcopy(query_document)
    distractor["loc"] = "10"
    database.channel.insert_one(distractor)
    assert database.get_seed_channel("XX", "TEST", "BHZ", location, time=50.0) is None

    document_id = database.channel.insert_one(copy.deepcopy(query_document)).inserted_id
    result = database.get_seed_channel("XX", "TEST", "BHZ", location, time=50.0)
    assert result["_id"] == document_id

    database.channel.insert_one(copy.deepcopy(query_document))
    documents_before_error = list(database.channel.find().sort("_id"))
    with pytest.raises(MsPASSError) as error:
        database.get_seed_channel("XX", "TEST", "BHZ", location, time=50.0)
    assert error.value.severity == ErrorSeverity.Invalid
    assert "explicit location query returned 2 matches" in str(error.value)
    assert list(database.channel.find().sort("_id")) == documents_before_error
