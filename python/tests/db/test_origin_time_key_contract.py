import pandas as pd
import pytest

from mspasspy.ccore.seismic import TimeSeries
import mspasspy.db.normalize as normalize_module
from mspasspy.db.normalize import OriginTimeDBMatcher, OriginTimeMatcher


def _datum(t0, data_key=None, data_time=None):
    datum = TimeSeries(1)
    datum.t0 = t0
    datum.set_live()
    if data_key is not None:
        datum[data_key] = data_time
    return datum


@pytest.mark.parametrize(
    "source_key,data_key",
    [("time", None), ("origin_epoch", "pick_epoch")],
)
@pytest.mark.parametrize(
    "data_time,tolerance,expected_count",
    [(200.0, 0.25, 0), (100.1, 0.25, 1), (101.0, 1.1, 2)],
)
def test_default_and_custom_keys_cover_zero_one_and_multiple_candidates(
    source_key, data_key, data_time, tolerance, expected_count
):
    sources = pd.DataFrame(
        [{source_key: 100.0, "label": "early"}, {source_key: 102.0, "label": "late"}]
    )
    matcher = OriginTimeMatcher(
        sources,
        tolerance=tolerance,
        attributes_to_load=[source_key, "label"],
        load_if_defined=[],
        prepend_collection_name=False,
        data_time_key=data_key,
        source_time_key=source_key,
    )
    datum = _datum(
        data_time if data_key is None else 999.0,
        data_key=data_key,
        data_time=data_time,
    )

    matches, elog = matcher.find(datum)

    if expected_count == 0:
        assert matches is None
        assert elog is not None
    else:
        assert elog is None
        assert len(matches) == expected_count


@pytest.mark.parametrize(
    "data_key,t0,data_time,expected_label",
    [
        (None, 101.9, None, "t0"),
        ("pick_epoch", 101.9, 100.1, "configured-key"),
    ],
)
def test_configured_data_time_controls_candidate_filtering_and_nearest_match(
    data_key, t0, data_time, expected_label
):
    sources = pd.DataFrame(
        [
            {"origin_epoch": 100.0, "label": "configured-key"},
            {"origin_epoch": 102.0, "label": "t0"},
        ]
    )
    matcher = OriginTimeMatcher(
        sources,
        tolerance=3.0,
        attributes_to_load=["origin_epoch", "label"],
        load_if_defined=[],
        prepend_collection_name=False,
        data_time_key=data_key,
        source_time_key="origin_epoch",
    )
    datum = _datum(t0, data_key=data_key, data_time=data_time)

    result, elog = matcher.find_one(datum)

    assert elog is None
    assert result["label"] == expected_label


@pytest.mark.parametrize(
    "source_key,data_key,t0,data_time",
    [(None, None, 100.0, None), ("origin_epoch", "pick_epoch", 999.0, 100.0)],
)
def test_database_query_uses_configured_source_and_data_keys(
    source_key, data_key, t0, data_time
):
    matcher = object.__new__(OriginTimeDBMatcher)
    matcher.t0offset = 2.0
    matcher.tolerance = 0.5
    matcher.query = {"kind": "event"}
    matcher.data_time_key = data_key
    matcher.source_time_key = "time" if source_key is None else source_key
    datum = _datum(t0, data_key=data_key, data_time=data_time)

    query = matcher.query_generator(datum)

    expected_time = (t0 if data_key is None else data_time) - 2.0
    assert query == {
        "kind": "event",
        matcher.source_time_key: {
            "$gte": expected_time - 0.5,
            "$lte": expected_time + 0.5,
        },
    }


@pytest.mark.parametrize(
    "source_time_key,expected", [(None, "time"), ("origin_epoch", "origin_epoch")]
)
def test_database_matcher_constructor_normalizes_the_default_source_key(
    monkeypatch, source_time_key, expected
):
    monkeypatch.setattr(
        normalize_module.DatabaseMatcher,
        "__init__",
        lambda self, *args, **kwargs: None,
    )

    matcher = OriginTimeDBMatcher(object(), source_time_key=source_time_key)

    assert matcher.source_time_key == expected
