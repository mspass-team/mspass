"""Offline FDSN workflow regressions using real MsPASS data objects.

The S3 responses and database lookups are local fakes.  In particular, these
tests must never depend on AWS credentials or external FDSN service uptime.
"""

import io
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
import yaml
from botocore.exceptions import ClientError
from obspy import Stream, Trace, UTCDateTime

import mspasspy.io.FDSN as fdsn
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import ErrorLogger, ErrorSeverity


@pytest.fixture
def provider():
    return fdsn.S3SeismicProvider(
        {
            "Demo": {
                "XX": {
                    "region": "us-east-1",
                    "bucket": "example-bucket",
                    "prefix": "archive/",
                    "object_format": "{year:04d}/{jday:03d}/{sta}.{chan}.{loc}",
                    "blank_loc_code_value": "_",
                    "payload_structure": "channel_day",
                    "data_format": "miniseed",
                    "access": "open_s3",
                }
            }
        },
        provider="Demo",
    )


def s3_client():
    client = Mock()
    client.meta.service_model.service_name = "s3"
    return client


def test_s3_day_listing_is_paginated_sorted_and_keyed_by_full_prefix(provider):
    client = s3_client()
    client.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "archive/2026/015/BBB.BHZ.00#metadata"},
                {"Key": "archive/2026/015/AAA.BHZ.00"},
            ],
            "ResponseMetadata": {"RequestId": "page1"},
        },
        {
            "Contents": [
                {"Key": "archive/2026/015/AAA.BHZ.00"},
                {"Key": "archive/2026/015/CCC.BHN.00"},
            ],
            "ResponseMetadata": {"RequestId": "page2"},
        },
    ]

    objects, details = provider.get_year_day_object_names(client, "XX", 2026, 15)

    assert objects == [
        "archive/2026/015/AAA.BHZ.00",
        "archive/2026/015/BBB.BHZ.00",
        "archive/2026/015/CCC.BHN.00",
    ]
    assert details["server_data"]["RequestId"] == "page2"
    client.get_paginator.assert_called_once_with("list_objects_v2")
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="example-bucket",
        Prefix="archive/2026/015/",
        Delimiter="/",
    )


@pytest.mark.parametrize("include_prefix", [True, False])
def test_s3_read_returns_bytes_and_closes_stream(provider, include_prefix):
    client = s3_client()
    body = io.BytesIO(b"miniseed-data")
    client.get_object.return_value = {"Body": body}

    raw, elog = provider.get_raw_object_data(
        client, "XX", "key.ms", use_net_prefix=include_prefix
    )

    assert raw == b"miniseed-data"
    assert elog.size() == 0
    assert body.closed
    expected_key = "archive/key.ms" if include_prefix else "key.ms"
    client.get_object.assert_called_once_with(
        Bucket="example-bucket", Key=expected_key
    )


@pytest.mark.parametrize(
    "failure, text",
    [
        (
            ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "object not found"}},
                "GetObject",
            ),
            "NoSuchKey",
        ),
        (OSError("network interrupted"), "network interrupted"),
    ],
)
def test_s3_read_errors_preserve_the_cause_in_elog(provider, failure, text):
    client = s3_client()
    client.get_object.side_effect = failure

    raw, elog = provider.get_raw_object_data(client, "XX", "missing.ms")

    assert raw is None
    assert elog.size() == 1
    assert text in elog.get_error_log()[0].message
    assert "missing.ms" in elog.get_error_log()[0].message


def test_s3_listing_propagates_remote_errors(provider):
    client = s3_client()
    client.get_paginator.return_value.paginate.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "ListObjectsV2",
    )

    with pytest.raises(ClientError):
        provider.get_year_day_object_names(client, "XX", 2026, 15)


@pytest.mark.parametrize(
    "data_type, access",
    [
        ("station_day", "open_s3"),
        ("channel_day", "open_aws_s3"),
        ("source_gather", "earthscope-sdk"),
        ("event_file", "earthscope-sdk"),
    ],
)
def test_provider_payload_types_and_access(provider, data_type, access):
    provider.nets["XX"]["payload_structure"] = data_type
    provider.nets["XX"]["access"] = access
    assert provider.payload_type("XX") == (
        fdsn.PayloadType.source_gather
        if data_type in ("source_gather", "event_file")
        else getattr(fdsn.PayloadType, data_type)
    )
    assert provider.payload_format("XX") == fdsn.PayloadFormat.miniseed
    assert provider.is_open("XX") == (access != "earthscope-sdk")


def test_provider_invalid_configuration_does_not_silently_use_bad_values(provider):
    provider.nets["XX"]["payload_structure"] = "unsupported"
    with pytest.raises(ValueError, match="invalid value"):
        provider.payload_type("XX")
    provider.nets["XX"]["data_format"] = "unknown"
    with pytest.raises(ValueError, match="miniseed"):
        provider.payload_format("XX")
    provider.nets["XX"]["access"] = "unknown"
    with pytest.raises(ValueError, match="illegal value"):
        provider.access("XX")
    with pytest.raises(ValueError, match="missing required attribute"):
        provider._fetch_subdoc_data("test", "XX", "missing_attribute")


def test_provider_serial_anonymous_client_has_unsigned_config(provider, monkeypatch):
    factory = Mock()
    monkeypatch.setattr(fdsn.boto3, "Session", factory)

    client = provider.s3client("XX")

    assert client is factory.return_value.client.return_value
    factory.assert_called_once_with(region_name="us-east-1")
    args, kwargs = factory.return_value.client.call_args
    assert args == ("s3",)
    assert kwargs["config"].signature_version is fdsn.UNSIGNED


def test_provider_dask_registration_uses_matching_worker_key(provider, monkeypatch):
    dask = Mock()
    monkeypatch.setattr(fdsn, "is_dask_client", lambda value: value is dask)
    monkeypatch.setattr(fdsn, "is_mspass_client", lambda value: False)

    provider.initialize_dask_workers(dask, "XX")

    plugin = dask.register_plugin.call_args.args[0]
    assert isinstance(plugin, fdsn.AnonymousS3Worker)
    assert plugin.worker_key == provider.get_worker_key("XX")
    with pytest.raises(ValueError, match="does not have data"):
        provider.initialize_dask_workers(dask, "ZZ")
    with pytest.raises(ValueError, match="client"):
        provider.initialize_dask_workers(object(), "XX")


def test_object_name_roundtrip_and_secondary_component_parse(provider):
    name = provider.day_volume_object_name("XX", "AAA", 2026, 15, "BHZ", "00")
    assert name == "archive/2026/015/AAA.BHZ.00"
    parsed = provider.objname2doc("XX", name)
    assert parsed["year"] == 2026
    assert parsed["jday"] == 15
    assert parsed["sta"] == "AAA"
    assert parsed["chan"] == "BHZ"
    assert parsed["loc"] == "00"

    pieces = provider.split_and_parse(
        "{net}/{year:04d}/{net}", "XX/2026/YY", "/"
    )
    assert pieces["net"] == "XX"
    assert pieces["net_v2"] == "YY"
    assert pieces["year"] == 2026
    assert provider.split_and_parse("{net}/{year}", "XX", "/") is None


def test_day_calculation_handles_leap_year_midnight_and_invalid_time():
    t0 = UTCDateTime(2024, 12, 31, 23, 59, 50)
    assert fdsn.days_needed(t0, 0, 20, pad=0) == [
        [2024, 366],
        [2025, 1],
    ]
    assert fdsn.days_needed(float(UTCDateTime(2026, 1, 15)), 0, 10, pad=0) == [
        [2026, 15]
    ]
    with pytest.raises(ValueError, match="illegal type"):
        fdsn.days_needed(1000, 0, 1)
    with pytest.raises(ValueError, match="days_needed"):
        fdsn.days_needed(t0, 20, 0)


def test_day_calculation_returns_all_days_across_longer_year_boundaries():
    t0 = UTCDateTime(2023, 12, 31, 23, 59, 0)
    result = fdsn.days_needed(t0, 0, 366 * 86400, pad=0)
    assert result[0] == [2023, 365]
    assert [2024, 366] in result
    assert result[-1][0] == 2025
    assert len(result) > 366


def test_index_day_separates_bad_names_filtered_channels_and_mismatched_nets(
    provider, monkeypatch
):
    client = s3_client()
    objects = ["ok", "skip", "wrong_net", "unreadable", "ok2"]
    parser_results = {
        "ok": {"net": "XX", "sta": "AAA", "chan": "BHZ", "loc": "_"},
        "skip": {"net": "XX", "sta": "AAA", "chan": "LOG", "loc": "_"},
        "wrong_net": {"net": "YY", "sta": "AAA", "chan": "BHZ"},
        "unreadable": {},
        "ok2": {"net": "XX", "sta": "BBB", "chan": "BHN", "loc": "00"},
    }
    monkeypatch.setattr(
        provider, "get_year_day_object_names",
        lambda *args: (objects, {"server_data": {"request": 3}}),
    )
    monkeypatch.setattr(
        provider, "objname2doc",
        lambda net, name, verbose=False: parser_results[name].copy(),
    )
    db = {"wf_s3": Mock(), "s3_parse_failures": Mock()}
    db["wf_s3"].insert_many.side_effect = lambda docs: SimpleNamespace(
        inserted_ids=list(range(len(docs)))
    )
    monkeypatch.setattr(fdsn, "fetch_dbhandle", lambda value: value)

    summary = fdsn.index_day(
        "XX", 2026, 15, db, provider, s3_client=client, channel_select="BH*"
    )

    assert summary == [2, 5, 1, 1, {"YY": 1}]
    records = db["wf_s3"].insert_many.call_args.args[0]
    assert [record["s3key"] for record in records] == ["ok", "ok2"]
    assert records[0]["sta"] == "AAA"
    assert "loc" not in records[0]
    assert records[1]["loc"] == "00"
    assert records[0]["bucket"] == "example-bucket"
    assert records[0]["starttime"] < records[0]["endtime"]
    assert records[0]["server_data"] == {"request": 3}
    db["s3_parse_failures"].insert_one.assert_called_once()
    assert db["s3_parse_failures"].insert_one.call_args.args[0]["s3key"] == (
        "unreadable"
    )


def test_processing_blocks_group_arrivals_without_duplicate_day_objects(
    monkeypatch,
):
    epoch = float(UTCDateTime(2026, 1, 15, 12))
    events = pd.DataFrame(
        [
            {"time": epoch + 40, "net": "XX", "sta": "AAA", "source_id": "event1"},
            {"time": epoch + 60, "net": "XX", "sta": "AAA", "source_id": "event2"},
            {"time": epoch + 80, "net": "XX", "sta": "BBB", "source_id": "missing"},
        ]
    )
    records = [
        {
            "net": "XX",
            "sta": "AAA",
            "year": 2026,
            "jday": 15,
            "s3key": "archive/2026/015/AAA.BHZ.00",
        }
    ]
    db = {"wf_s3": Mock()}
    db["wf_s3"].find.return_value = records
    monkeypatch.setattr(fdsn, "fetch_dbhandle", lambda value: value)

    groups, failed = fdsn.build_day_processing_blocks(
        events, -2.0, 3.0, db, pad=0.0, auxkeys=["source_id"]
    )

    assert len(groups) == 1
    assert groups[0][0] == "2026_15"
    assert len(groups[0][1]) == 1
    request = groups[0][1][0]
    assert request["s3objects"] == ["archive/2026/015/AAA.BHZ.00"]
    assert [entry["source_id"] for entry in request["arrival"]] == [
        "event1",
        "event2",
    ]
    assert len(request["start"]) == len(request["end"]) == 2
    assert request["jday_tag"] == "2026_15"
    assert failed == [
        {"net": "XX", "sta": "BBB", "$or": [{"year": 2026, "jday": 15}]}
    ]


def test_processing_blocks_reject_excessive_day_span(monkeypatch):
    monkeypatch.setattr(fdsn, "fetch_dbhandle", lambda value: {})
    events = pd.DataFrame(
        [{"time": float(UTCDateTime(2026, 1, 15)), "net": "XX", "sta": "AAA"}]
    )
    with pytest.raises(ValueError, match="maxdays"):
        fdsn.build_day_processing_blocks(
            events, -86400 * 2, 86400 * 2, "unused", pad=0, maxdays=2
        )


def trace(channel="BHZ", start=1000.0):
    datum = TimeSeries(20)
    datum.t0 = start
    datum.dt = 1.0
    datum["net"] = "XX"
    datum["sta"] = "AAA"
    datum["chan"] = channel
    datum["loc"] = ""
    for i in range(datum.npts):
        datum.data[i] = float(i + 1)
    datum.set_live()
    return datum


def test_s3_segments_cut_requested_window_and_preserve_auxiliary_metadata(
    provider, monkeypatch
):
    reader = Mock(return_value=(b"fake-miniseed", ErrorLogger()))
    decoder = Mock(return_value=trace())
    monkeypatch.setattr(provider, "get_raw_object_data", reader)
    monkeypatch.setattr(provider, "decode_waveform_data", decoder)
    request = {
        "net": "XX",
        "sta": "AAA",
        "s3objects": ["archive/day.ms"],
        "start": [1002.0],
        "end": [1006.0],
        "arrival": [{"source_id": "event1", "npts": 999}],
        "jday_tag": "2026_015",
    }

    result = fdsn.get_s3_segments(
        request, provider, s3_client=s3_client(), detrend_type="none"
    )

    assert result.live
    assert len(result.member) == 1
    member = result.member[0]
    assert member.live
    assert member.npts == 5
    assert member["source_id"] == "event1"
    assert member["chan"] == "BHZ"
    assert result["jday_tag"] == "2026_015"
    assert result["s3_request_document"]["Bucket"] == "example-bucket"
    reader.assert_called_once()
    decoder.assert_called_once()


def test_s3_segments_retain_retrieval_error_without_creating_waveforms(
    provider, monkeypatch
):
    read_error = ErrorLogger()
    read_error.log_error("fake-reader", "missing object", ErrorSeverity.Invalid)
    monkeypatch.setattr(
        provider, "get_raw_object_data", lambda *args: (None, read_error)
    )
    decoder = Mock(side_effect=AssertionError("must not decode missing data"))
    monkeypatch.setattr(provider, "decode_waveform_data", decoder)
    result = fdsn.get_s3_segments(
        {
            "net": "XX",
            "sta": "AAA",
            "s3objects": ["missing"],
            "start": [1000.0],
            "end": [1003.0],
        },
        provider,
        s3_client=s3_client(),
        detrend_type="none",
    )

    assert not result.live
    assert len(result.member) == 0
    assert result.elog.size() >= 1
    assert "missing object" in str(result.elog.get_error_log())
    decoder.assert_not_called()


@pytest.mark.parametrize("selection", [None, ["BHZ"]])
def test_decode_miniseed_channel_day_and_selection_validation(
    provider, monkeypatch, selection
):
    obspy_trace = Trace(
        data=np.arange(20, dtype=np.int32),
        header={
            "network": "XX",
            "station": "AAA",
            "channel": "BHZ",
            "location": "",
            "starttime": UTCDateTime(2026, 1, 15),
            "sampling_rate": 20.0,
        },
    )
    monkeypatch.setattr(
        fdsn.obspy,
        "read",
        lambda stream, format: Stream([obspy_trace.copy()]),
    )
    decoded = provider.decode_waveform_data(b"fake", "XX", chanlist=selection)
    assert isinstance(decoded, TimeSeries)
    assert decoded.live
    assert decoded.npts == 20
    assert decoded["chan"] == "BHZ"
    with pytest.raises(ValueError, match="Only one"):
        provider.decode_waveform_data(
            b"fake", "XX", chanlist=["BHZ"], obspy_select_string="BH*"
        )


def test_decode_miniseed_station_day_selects_channels_and_removes_log(
    provider, monkeypatch
):
    provider.nets["XX"]["payload_structure"] = "station_day"
    template = {
        "network": "XX",
        "station": "AAA",
        "location": "",
        "sampling_rate": 20.0,
        "starttime": UTCDateTime(2026, 1, 15),
    }
    data = np.arange(10, dtype=np.int32)
    stream = Stream(
        [
            Trace(data=data.copy(), header={**template, "channel": name})
            for name in ("BHZ", "BHN", "LOG")
        ]
    )
    monkeypatch.setattr(fdsn.obspy, "read", lambda stream, format: stream_data.copy())
    stream_data = stream

    result = provider.decode_waveform_data(b"fake", "XX", chanlist=["BHZ"])

    assert isinstance(result, TimeSeriesEnsemble)
    assert result.live
    assert len(result.member) == 1
    assert result.member[0]["chan"] == "BHZ"


@pytest.mark.parametrize("error", [TypeError("bad stream"), OSError("bad bytes")])
def test_decode_miniseed_reports_obspy_reader_errors(provider, monkeypatch, error):
    monkeypatch.setattr(fdsn.obspy, "read", Mock(side_effect=error))

    decoded = provider.decode_waveform_data(b"invalid", "XX")

    assert isinstance(decoded, TimeSeries)
    assert decoded.elog.size() > 0
    assert "bad" in str(decoded.elog.get_error_log())


def test_dispatcher_uses_configurations_and_exposes_serial_provider(
    provider, tmp_path, monkeypatch
):
    preferred_path = tmp_path / "preferred.yaml"
    preferred_path.write_text("XX: Demo\n", encoding="utf-8")
    providers_path = tmp_path / "providers.yaml"
    providers_path.write_text(
        yaml.safe_dump({"Demo": provider.nets}), encoding="utf-8"
    )
    client = s3_client()
    monkeypatch.setattr(fdsn.S3SeismicProvider, "s3client", lambda self, net: client)

    dispatch = fdsn.Dispatcher(
        ["XX"],
        preferred_provider_file=str(preferred_path),
        provider_file=str(providers_path),
    )

    assert set(dispatch.netcodes()) == {"XX"}
    assert dispatch.provider("XX").bucket("XX") == "example-bucket"
    assert dispatch.s3client("XX") is client
    with pytest.raises(ValueError, match="no provider"):
        dispatch.provider("ZZ")
    with pytest.raises(ValueError, match="no s3 client"):
        dispatch.s3client("ZZ")
    with pytest.raises(RuntimeError, match="serial mode"):
        dispatch.initialize_dask_workers(Mock())
    with pytest.raises(ValueError, match="Illegal type"):
        dispatch.add_net("XX", object())


def test_s3_read_error_records_can_be_saved_offline(monkeypatch):
    database = {"s3_read_failures": Mock()}
    monkeypatch.setattr(fdsn, "fetch_dbhandle", lambda value: value)
    datum = trace()
    datum.elog.log_error("reader", "incomplete day", ErrorSeverity.Invalid)

    fdsn.handle_s3_read_errors(datum, database)

    document = database["s3_read_failures"].insert_one.call_args.args[0]
    assert document["chan"] == "BHZ"
    assert document["elog_content"] is not None
