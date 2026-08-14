import numpy as np
import pandas as pd
import pytest

import mspasspy.seismic.gather as gather_module
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import Metadata
from mspasspy.seismic.gather import Gather, SeismogramGather


def _member_metadata(size, npts, dt=0.25, starttimes=None):
    if starttimes is None:
        starttimes = [float(index) for index in range(size)]
    return pd.DataFrame(
        {
            "delta": [dt] * size,
            "starttime": starttimes,
            "npts": [npts] * size,
            "is_live": [True] * size,
            "x": list(range(size)),
            "station_code": [f"S{index}" for index in range(size)],
            "payload": [None] * size,
        }
    )


def _array_constructor(gather_class, component_count, dimensions):
    size, npts = 2, 4
    values = {
        "capacity": size,
        "size": size,
        "npts": npts,
        "num_components": component_count,
    }
    explicit = {key: dimensions(value) for key, value in values.items()}
    return gather_class(
        input_data=np.arange(size * component_count * npts, dtype=float).reshape(
            size, component_count, npts
        ),
        member_metadata=_member_metadata(size, npts),
        ensemble_metadata={"source": "array"},
        array_type="numpy",
        npartitions=1,
        **explicit,
    )


def _ensemble(gather_class, size=2, npts=4):
    if gather_class is Gather:
        ensemble = TimeSeriesEnsemble()
        datum_class = TimeSeries
    else:
        ensemble = SeismogramEnsemble()
        datum_class = Seismogram
    ensemble["source"] = "ensemble"
    ensemble["payload"] = {"tags": ["original"]}
    for index in range(size):
        datum = datum_class(npts)
        datum.dt = 0.25
        datum.t0 = float(index)
        datum["marker"] = index
        datum.set_live()
        ensemble.member.append(datum)
    ensemble.set_live()
    return ensemble


def _ensemble_constructor(gather_class, component_count, dimensions):
    size, npts = 2, 4
    values = {
        "capacity": size,
        "size": size,
        "npts": npts,
        "num_components": component_count,
    }
    explicit = {key: dimensions(value) for key, value in values.items()}
    source = _ensemble(gather_class, size=size, npts=npts)
    result = gather_class(
        input_obj=source,
        resample=False,
        array_type="numpy",
        npartitions=1,
        **explicit,
    )
    source["source"] = "mutated after construction"
    source["payload"]["tags"].append("caller mutation")
    return result


@pytest.mark.parametrize(
    "gather_class,component_count",
    [(Gather, 1), (SeismogramGather, 3)],
)
@pytest.mark.parametrize(
    "dimensions", [lambda _: 0, lambda _: None, lambda value: value]
)
@pytest.mark.parametrize("constructor", [_array_constructor, _ensemble_constructor])
def test_input_construction_derives_or_accepts_matching_dimensions(
    gather_class, component_count, dimensions, constructor
):
    result = constructor(gather_class, component_count, dimensions)

    assert result.capacity == 2
    assert result.size == 2
    assert result.npts == 4
    assert result.num_components == component_count
    assert result.member_data.shape == (2, component_count, 4)
    assert result.ensemble_metadata()["source"] in {"array", "ensemble"}
    if constructor is _ensemble_constructor:
        assert result.ensemble_metadata()["payload"] == {"tags": ["original"]}


@pytest.mark.parametrize(
    "gather_class,component_count",
    [(Gather, 1), (SeismogramGather, 3)],
)
@pytest.mark.parametrize("input_kind", ["array", "ensemble"])
@pytest.mark.parametrize(
    "field,conflict",
    [
        ("capacity", 3),
        ("size", 3),
        ("npts", 5),
        ("num_components", 2),
    ],
)
def test_input_dimension_conflicts_raise_before_assigning_fields(
    gather_class, component_count, input_kind, field, conflict
):
    if field == "num_components" and component_count == 3:
        conflict = 4
    kwargs = {
        "array_type": "numpy",
        "npartitions": 1,
        field: conflict,
    }
    if input_kind == "array":
        kwargs.update(
            input_data=np.zeros((2, component_count, 4)),
            member_metadata=_member_metadata(2, 4),
        )
    else:
        kwargs.update(input_obj=_ensemble(gather_class), resample=False)

    uninitialized = gather_class.__new__(gather_class)
    with pytest.raises(ValueError, match=f"{field}=.*conflicts"):
        gather_class.__init__(uninitialized, **kwargs)

    assert vars(uninitialized) == {}


@pytest.mark.parametrize(
    "gather_class,component_count",
    [(Gather, 1), (SeismogramGather, 3)],
)
def test_ensemble_dimensions_are_derived_after_resampling(
    gather_class, component_count, monkeypatch
):
    calls = []

    def resample_to_fifty_samples(ensemble, requested_dt):
        calls.append((id(ensemble), requested_dt))
        for datum in ensemble.member:
            datum.set_npts(50)
            datum.dt = requested_dt
        return ensemble

    monkeypatch.setattr(gather_module, "resample_ensemble", resample_to_fifty_samples)
    source = _ensemble(gather_class, size=2, npts=100)

    result = gather_class(
        input_obj=source,
        dt=0.5,
        resample=True,
        array_type="numpy",
        npartitions=1,
    )

    assert result.npts == 50
    assert result.member_data.shape == (2, component_count, 50)
    assert result.member_metadata["npts"].tolist() == [50, 50]

    conflicting = _ensemble(gather_class, size=2, npts=100)
    uninitialized = gather_class.__new__(gather_class)
    with pytest.raises(ValueError, match="npts=100 conflicts with the value 50"):
        gather_class.__init__(
            uninitialized,
            input_obj=conflicting,
            npts=100,
            dt=0.5,
            resample=True,
            array_type="numpy",
            npartitions=1,
        )

    assert vars(uninitialized) == {}
    assert calls == [(id(source), 0.5), (id(conflicting), 0.5)]


def _scalar_gather(ensemble_metadata=None, starttimes=None):
    return Gather(
        input_data=np.zeros((2, 1, 4)),
        member_metadata=_member_metadata(
            2, 4, dt=2.0, starttimes=starttimes or [0.0, 2.0]
        ),
        ensemble_metadata=ensemble_metadata,
        array_type="numpy",
        npartitions=1,
    )


@pytest.mark.parametrize("metadata_class", [dict, Metadata])
def test_ensemble_metadata_method_and_sync_broadcast_copied_values(metadata_class):
    source = metadata_class(
        {
            "x": 11,
            "station_code": "SYNC",
            "payload": {"tags": ["original"]},
        }
    )
    gather = _scalar_gather(source)

    assert callable(gather.ensemble_metadata)
    assert gather.ensemble_metadata()["x"] == 11
    assert isinstance(gather.ensemble_metadata("metadata"), Metadata)

    source["x"] = 99
    source["station_code"] = "MUTATED"
    source["payload"]["tags"].append("caller mutation")
    gather.sync_metadata()

    assert gather.member_metadata["x"].tolist() == [11, 11]
    assert gather.member_metadata["station_code"].tolist() == ["SYNC", "SYNC"]
    payloads = gather.member_metadata["payload"].tolist()
    assert payloads == [{"tags": ["original"]}, {"tags": ["original"]}]
    assert payloads[0] is not payloads[1]
    assert payloads[0]["tags"] is not payloads[1]["tags"]


def test_member_metadata_dict_and_metadata_edits_are_row_local_and_copied():
    gather = _scalar_gather()
    row_zero_before = gather.get_metadata(0)

    replacement = {
        "x": 21,
        "station_code": "DICT",
        "payload": {"tags": ["dict"]},
    }
    gather.set_metadata(1, replacement)
    replacement["x"] = 999
    replacement["station_code"] = "MUTATED"
    replacement["payload"]["tags"].append("caller mutation")

    assert gather.get_metadata(0) == row_zero_before
    assert gather.get_metadata(1)["x"] == 21
    assert gather.get_metadata(1)["station_code"] == "DICT"
    assert gather.get_metadata(1)["payload"] == {"tags": ["dict"]}
    assert pd.isna(gather.get_metadata(1)["delta"])
    assert pd.isna(gather.get_metadata(1)["starttime"])
    assert pd.isna(gather.get_metadata(1)["npts"])
    assert pd.isna(gather.get_metadata(1)["is_live"])

    edit = Metadata(
        {
            "x": 31,
            "station_code": "METADATA",
            "payload": {"tags": ["metadata"]},
        }
    )
    row_one_before = gather.member_metadata.loc[1].copy(deep=True)
    gather.edit_metadata(0, edit)
    edit["x"] = 888
    edit["station_code"] = "MUTATED"
    edit["payload"]["tags"].append("caller mutation")

    pd.testing.assert_series_equal(gather.member_metadata.loc[1], row_one_before)
    assert gather.get_metadata(0)["x"] == 31
    assert gather.get_metadata(0)["station_code"] == "METADATA"
    assert gather.get_metadata(0)["payload"] == {"tags": ["metadata"]}
    assert gather.get_metadata(0)["delta"] == row_zero_before["delta"]
    assert gather.get_metadata(0)["starttime"] == row_zero_before["starttime"]
    assert gather.get_metadata(0)["npts"] == row_zero_before["npts"]
    assert gather.get_metadata(0)["is_live"] == row_zero_before["is_live"]


def test_column_values_method_is_callable_and_copies_input():
    gather = _scalar_gather()

    assert callable(gather.column_values)
    assert gather.column_values() == [0, 1]

    labels = [["left"], ["right"]]
    gather.set_column_values(labels)
    labels[0].append("caller mutation")

    assert gather.column_values() == [["left"], ["right"]]


def test_sample_number_uses_half_away_from_zero_integer_rounding():
    gather = _scalar_gather(starttimes=[0.0, 2.0])

    default_columns = gather.sample_number(1.0, [0, 1])
    assert np.issubdtype(default_columns.dtype, np.integer)
    assert np.array_equal(default_columns, [1, -1])

    gather = _scalar_gather(starttimes=[-2.0, 4.0])
    gather.set_column_values(["positive", "negative"])
    named_columns = gather.sample_number(1.0, ["positive", "negative"])

    assert np.issubdtype(named_columns.dtype, np.integer)
    assert np.array_equal(named_columns, [2, -2])
