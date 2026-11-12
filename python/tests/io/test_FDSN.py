"""Offline tests for the FDSN S3 provider and channel selection contracts."""

from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
import yaml

from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import Metadata, MsPASSError
from mspasspy.io.FDSN import (
    ChannelPreferences,
    DataAccess,
    EnsembleMatcher,
    S3SeismicProvider,
    clean_ensemble,
    is_s3_client,
    parse_format_string,
    safe_parse,
)


@pytest.fixture
def preferences(tmp_path):
    # The repeated BHZ code is intentional: precedence must remain zero.
    definitions = {
        "XX": {
            "default": {
                "X": ["BH1 none 40.0", "BHE 00 20.0"],
                "Y": ["BH2 NULL 40.0", "BHN 00 20.0"],
                "Z": ["BHZ None 40.0", "BHZ None 40.0"],
            }
        }
    }
    path = tmp_path / "channels.yaml"
    path.write_text(yaml.safe_dump(definitions), encoding="utf-8")
    return ChannelPreferences(str(path))


@pytest.fixture
def provider():
    return S3SeismicProvider(
        {
            "Demo": {
                "XX": {
                    "region": "us-east-1",
                    "bucket": "example-bucket",
                    "prefix": "archive/",
                    "object_format": (
                        "{year}/{year}_{jday:03d}/{net}{sta}_{chan}_{loc}.ms"
                    ),
                    "blank_loc_code_value": "_",
                    "payload_structure": "channel_day",
                    "data_format": "miniseed",
                    "access": "open_s3",
                }
            }
        },
        provider="Demo",
    )


def test_channel_preferences_fallback_and_priority(preferences):
    assert preferences.preference("XX", "UNKNOWN", "x")[0] == (
        "BH1",
        "",
        40.0,
    )
    assert set(preferences.preference3C("XX", "AAA")) == {"X", "Y", "Z"}
    assert preferences.component("XX", "AAA", "BHZ", None) == "Z"
    assert preferences.precedence("XX", "AAA", "BHZ", "") == 0
    assert preferences.samprate("XX", "AAA", "BHE", "00") == 20.0
    assert preferences.component("ZZ", "AAA", "BHZ", None) == "undefined"
    assert preferences.precedence("ZZ", "AAA", "BHZ", None) == -1
    with pytest.raises(KeyError):
        preferences.preference("ZZ", "AAA", "X")


def test_channel_preferences_reject_missing_default(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(
        yaml.safe_dump(
            {"XX": {"AAA": {"X": ["BH1 00 40"], "Y": ["BH2 00 40"], "Z": ["BHZ 00 40"]}}}
        ),
        encoding="utf-8",
    )
    with pytest.raises(MsPASSError, match="missing required default"):
        ChannelPreferences(str(path))


def test_ensemble_matcher_uses_multiindex_and_rejects_missing_keys():
    frame = pd.DataFrame(
        [
            {"net": "XX", "sta": "AAA", "chan": "BH1", "member_index": 0},
            {"net": "XX", "sta": "AAA", "chan": "BH2", "member_index": 1},
        ]
    )
    matcher = EnsembleMatcher(frame, ["net", "sta"])
    matches, elog = matcher.find(Metadata({"net": "XX", "sta": "AAA"}))
    assert elog is None
    assert [row["chan"] for row in matches] == ["BH1", "BH2"]
    assert all(row["net"] == "XX" and row["sta"] == "AAA" for row in matches)

    missing, elog = matcher.find(Metadata({"net": "XX"}))
    assert missing is None
    assert elog is not None


def _trace(channel):
    trace = TimeSeries(4)
    trace.t0 = 0.0
    trace.dt = 1.0
    trace["net"] = "XX"
    trace["sta"] = "AAA"
    trace["chan"] = channel
    trace["loc"] = ""
    trace.set_live()
    return trace


def test_clean_ensemble_keeps_preferred_complete_triplet(preferences):
    ensemble = TimeSeriesEnsemble()
    for channel in ("BH1", "BH2", "BHZ", "BHE", "BHN"):
        ensemble.member.append(_trace(channel))
    ensemble.set_live()

    selected = clean_ensemble(ensemble, preferences)

    assert selected.live
    assert sorted(member["chan"] for member in selected.member) == [
        "BH1",
        "BH2",
        "BHZ",
    ]
    assert len(ensemble.member) == 5


def test_clean_ensemble_handles_missing_station_metadata(preferences):
    ensemble = TimeSeriesEnsemble()
    member = TimeSeries(4)
    member.set_live()
    ensemble.member.append(member)
    ensemble.set_live()

    selected = clean_ensemble(ensemble, preferences)

    assert len(selected.member) == 0


def test_provider_object_name_uses_explicit_and_blank_location(provider):
    assert provider.day_volume_object_name(
        "XX", "AAA", 2026, 15, "BHZ", "00"
    ) == "archive/2026/2026_015/XXAAA_BHZ_00.ms"
    assert provider.day_volume_object_name(
        "XX", "AAA", 2026, 15, "BHZ", None
    ) == "archive/2026/2026_015/XXAAA_BHZ__.ms"
    assert provider.day_volume_object_name(
        "XX", "AAA", 2026, 15, "BHZ", "00", include_prefix=False
    ) == "2026/2026_015/XXAAA_BHZ_00.ms"
    with pytest.raises(ValueError, match="chan"):
        provider.day_volume_object_name("XX", "AAA", 2026, 15)


def test_provider_accessors_and_unknown_network(provider):
    assert list(provider.networks()) == ["XX"]
    assert provider.bucket("XX") == "example-bucket"
    assert provider.region("XX") == "us-east-1"
    assert provider.access("XX") == DataAccess.open_aws_s3
    assert provider.is_open("XX")
    with pytest.raises(ValueError, match="S3SeismicProvider.bucket"):
        provider.bucket("ZZ")


def test_provider_database_round_trip_uses_named_collection(provider):
    collection = Mock()
    collection.insert_one.return_value = SimpleNamespace(inserted_id="record-id")
    database = {"custom": collection}

    assert provider.dbsave(database, collection="custom") == "record-id"
    saved = collection.insert_one.call_args.args[0]
    assert saved["provider"] == "Demo"
    assert saved["nets"] == provider.nets

    collection.find_one.return_value = saved
    restored = S3SeismicProvider.from_database(
        database, provider="Demo", collection="custom"
    )
    assert restored.nets == provider.nets
    assert restored.bucket("XX") == "example-bucket"
    collection.find_one.assert_called_once_with({"provider": "Demo"})


def test_provider_from_file_uses_yaml_definition(tmp_path, provider):
    path = tmp_path / "provider.yaml"
    path.write_text(yaml.safe_dump({"Demo": provider.nets}), encoding="utf-8")

    loaded = S3SeismicProvider.from_file(str(path), provider="Demo")

    assert loaded.nets == provider.nets


def test_s3_client_validation_handles_missing_service_model():
    assert not is_s3_client(None)
    assert not is_s3_client(SimpleNamespace(meta=SimpleNamespace()))
    assert not is_s3_client(
        SimpleNamespace(meta=SimpleNamespace(service_model=None))
    )


def test_leaf_content_paginates_and_deduplicates_objects(provider):
    client = Mock()
    client.meta.service_model.service_name = "s3"
    client.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [{"Key": "vol/one#etag"}, {"Key": "vol/two"}],
            "ResponseMetadata": {"RequestId": "first"},
        },
        {
            "Contents": [{"Key": "vol/three"}],
            "ResponseMetadata": {"RequestId": "second"},
        },
    ]

    objects, details = provider.leaf_content(
        client, {"net": "XX", "year": 2026, "jday": 15}
    )

    assert objects == ["vol/one", "vol/three", "vol/two"]
    assert details["server_data"]["RequestId"] == "second"
    client.get_paginator.assert_called_once_with("list_objects_v2")


def test_format_parsing_preserves_fields_and_diagnostics():
    assert parse_format_string("prefix/{year}/{net}", {"year": 2026, "net": "XX"}) == (
        "prefix/2026/XX"
    )
    with pytest.raises(ValueError, match="missing required"):
        parse_format_string("{year}/{net}", {"year": 2026})
    assert safe_parse("{net}/{year:d}", "XX/2026") == {
        "net": "XX",
        "year": 2026,
    }
    with pytest.raises(MsPASSError, match="safe_parse"):
        safe_parse("net/{net}", "different/prefix")
