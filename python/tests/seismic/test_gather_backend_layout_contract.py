import os
import pickle
from pathlib import Path

import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr

import mspasspy.ccore.seismic as seismic_binding
import mspasspy.seismic.gather as gather_module
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.seismic.gather import Gather, SeismogramGather


def _canonical_values(size, components, npts):
    return np.arange(size * components * npts, dtype=float).reshape(
        size, components, npts
    )


def _member_metadata(size, npts):
    return pd.DataFrame(
        {
            "row": list(range(size)),
            "starttime": np.arange(size, dtype=float),
            "delta": [0.25] * size,
            "npts": [npts] * size,
            "is_live": [True] * size,
        }
    )


def _new_gather(
    gather_class,
    array_type,
    is_compact,
    size=3,
    npts=5,
    npartitions=2,
):
    components = 1 if gather_class is Gather else 3
    canonical = _canonical_values(size, components, npts)
    input_data = canonical if is_compact else canonical.transpose((0, 2, 1))
    result = gather_class(
        input_data=input_data,
        member_metadata=_member_metadata(size, npts),
        ensemble_metadata={"name": "contract", "nested": {"value": 7}},
        array_type=array_type,
        is_compact=is_compact,
        npartitions=npartitions,
    )
    return result, canonical


def _stored_values(gather):
    data = gather.member_data
    if isinstance(data, xr.DataArray):
        data = data.data
    if isinstance(data, da.Array):
        data = data.compute()
    return np.asarray(data)


def _expected_stored(canonical, is_compact):
    return canonical if is_compact else canonical.transpose((0, 2, 1))


def _assert_backend(gather, array_type):
    expected = {
        "numpy": np.ndarray,
        "dask": da.Array,
        "xarray": xr.DataArray,
    }[array_type]
    assert isinstance(gather.member_data, expected)


def _assert_dask_chunks(gather):
    data = (
        gather.member_data.data
        if isinstance(gather.member_data, xr.DataArray)
        else gather.member_data
    )
    expected_member_chunk = max(1, int(np.ceil(gather.capacity / gather.npartitions)))
    quotient, remainder = divmod(gather.size, expected_member_chunk)
    expected_axis_zero = (expected_member_chunk,) * quotient
    if remainder:
        expected_axis_zero += (remainder,)
    if not gather.size:
        expected_axis_zero = (0,)
    assert data.chunks[0] == expected_axis_zero
    assert data.chunks[1] == (data.shape[1],)
    assert data.chunks[2] == (data.shape[2],)
    if gather.size:
        assert all(chunk > 0 for dimension in data.chunks for chunk in dimension)


def _datum(gather_class, npts, offset):
    if gather_class is Gather:
        datum = TimeSeries(npts)
        for sample in range(npts):
            datum.data[sample] = offset + sample
    else:
        datum = Seismogram(npts)
        for component in range(3):
            for sample in range(npts):
                datum.data[component, sample] = offset + 10 * component + sample
    datum.t0 = offset
    datum.dt = 0.25
    datum["row"] = int(offset)
    datum.set_live()
    return datum


def test_contract_suite_uses_selected_source_and_real_binding():
    selected_source = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if selected_source:
        expected = Path(selected_source) / "mspasspy/seismic/gather.py"
        assert Path(gather_module.__file__).resolve() == expected.resolve()
    assert Path(seismic_binding.__file__).suffix == ".so"


@pytest.mark.parametrize("gather_class", [Gather, SeismogramGather])
@pytest.mark.parametrize("array_type", ["numpy", "dask"])
@pytest.mark.parametrize("is_compact", [True, False])
@pytest.mark.parametrize("size,npartitions", [(1, 3), (3, 3), (5, 3)])
def test_layout_and_partition_contract(
    gather_class, array_type, is_compact, size, npartitions
):
    result, canonical = _new_gather(
        gather_class,
        array_type,
        is_compact,
        size=size,
        npartitions=npartitions,
    )
    components = 1 if gather_class is Gather else 3
    expected_shape = (size, components, 5) if is_compact else (size, 5, components)

    _assert_backend(result, array_type)
    assert result.member_data.shape == expected_shape
    assert result.size == size
    assert result.capacity == size
    assert result.num_components == components
    assert result.npts == 5
    assert result.is_compact is is_compact
    assert np.array_equal(
        _stored_values(result), _expected_stored(canonical, is_compact)
    )
    if array_type == "dask":
        _assert_dask_chunks(result)


@pytest.mark.parametrize("gather_class", [Gather, SeismogramGather])
@pytest.mark.parametrize("array_type", ["numpy", "dask"])
@pytest.mark.parametrize("is_compact", [True, False])
def test_public_data_and_member_views_are_layout_independent(
    gather_class, array_type, is_compact
):
    result, canonical = _new_gather(
        gather_class, array_type, is_compact, size=2, npartitions=3
    )

    expected = canonical[1]
    if gather_class is Gather:
        expected = expected.reshape(-1)
    assert np.array_equal(np.asarray(result.data(1)), expected)
    assert np.array_equal(np.asarray(result.member(1).data), expected)


@pytest.mark.parametrize("gather_class", [Gather, SeismogramGather])
@pytest.mark.parametrize("array_type", ["numpy", "dask"])
@pytest.mark.parametrize("is_compact", [True, False])
def test_old_ensemble_conversion_uses_the_requested_layout(
    gather_class, array_type, is_compact
):
    ensemble = TimeSeriesEnsemble() if gather_class is Gather else SeismogramEnsemble()
    data = []
    for offset in (10.0, 20.0):
        datum = _datum(gather_class, 4, offset)
        ensemble.member.append(datum)
        values = np.asarray(datum.data)
        data.append(values.reshape(1, 4) if gather_class is Gather else values)
    canonical = np.stack(data)

    result = gather_class(
        input_obj=ensemble,
        resample=False,
        array_type=array_type,
        is_compact=is_compact,
        npartitions=3,
    )

    assert np.array_equal(
        _stored_values(result), _expected_stored(canonical, is_compact)
    )
    assert result.member_data.shape == _expected_stored(canonical, is_compact).shape


@pytest.mark.parametrize("value", [0, -1, True, 1.5, None])
def test_dask_rejects_invalid_partition_counts_before_array_creation(value):
    with pytest.raises(ValueError, match="npartitions must be a positive integer"):
        Gather(
            input_data=np.zeros((2, 1, 4)),
            member_metadata=_member_metadata(2, 4),
            array_type="dask",
            npartitions=value,
        )


def test_dask_accepts_positive_numpy_integer_partition_count():
    result = Gather(
        input_data=np.zeros((2, 1, 4)),
        member_metadata=_member_metadata(2, 4),
        array_type="dask",
        npartitions=np.int64(2),
    )

    assert result.npartitions == 2
    _assert_dask_chunks(result)


@pytest.mark.parametrize("gather_class", [Gather, SeismogramGather])
@pytest.mark.parametrize("array_type", ["numpy", "dask", "xarray"])
@pytest.mark.parametrize("is_compact", [True, False])
@pytest.mark.parametrize("capacity", [1, 3, 5])
def test_append_installs_axis_zero_result_and_updates_size_capacity(
    gather_class, array_type, is_compact, capacity
):
    components = 1 if gather_class is Gather else 3
    result = gather_class(
        capacity=capacity,
        size=0,
        npts=4,
        num_components=components,
        npartitions=3,
        member_metadata=pd.DataFrame(),
        ensemble_metadata={"dt": 0.25},
        dt=0.25,
        array_type=array_type,
        is_compact=is_compact,
    )
    first = _datum(gather_class, 4, 10.0)
    second = _datum(gather_class, 4, 20.0)

    result.append(first)
    first_snapshot = _stored_values(result).copy()
    result.append(second)

    assert result.size == 2
    assert result.capacity == max(capacity, 2)
    assert result.member_data.shape[0] == 2
    assert np.array_equal(_stored_values(result)[0:1], first_snapshot)
    expected = []
    for datum in (first, second):
        values = np.asarray(datum.data)
        if gather_class is Gather:
            values = values.reshape(1, 4) if is_compact else values.reshape(4, 1)
        elif not is_compact:
            values = values.transpose()
        expected.append(values)
    assert np.array_equal(_stored_values(result), np.stack(expected))
    assert result.member_metadata["row"].tolist() == [10, 20]
    _assert_backend(result, array_type)
    if array_type in ("dask", "xarray"):
        _assert_dask_chunks(result)


@pytest.mark.parametrize("gather_class", [Gather, SeismogramGather])
@pytest.mark.parametrize("array_type", ["numpy", "dask", "xarray"])
@pytest.mark.parametrize("is_compact", [True, False])
def test_subset_preserves_order_duplicates_empty_shape_and_backend(
    gather_class, array_type, is_compact
):
    source, canonical = _new_gather(
        gather_class, array_type, is_compact, size=4, npartitions=3
    )
    source.set_column_values(["a", "b", "c", "d"])

    selected = source.subset([2, 0, 2])
    expected = _expected_stored(canonical, is_compact)[[2, 0, 2]]
    _assert_backend(selected, array_type)
    assert np.array_equal(_stored_values(selected), expected)
    assert selected.member_metadata["row"].tolist() == [2, 0, 2]
    assert selected.column_values() == ["c", "a", "c"]
    assert selected.size == 3
    assert selected.capacity == 3
    assert selected.npartitions == source.npartitions

    empty = source.subset([])
    _assert_backend(empty, array_type)
    assert empty.member_data.shape == (0,) + source.member_data.shape[1:]
    assert empty.size == 0
    assert empty.capacity == 0
    assert len(empty.member_metadata) == 0
    assert empty.column_values() == []


@pytest.mark.parametrize("gather_class", [Gather, SeismogramGather])
@pytest.mark.parametrize("array_type", ["numpy", "dask", "xarray"])
@pytest.mark.parametrize("is_compact", [True, False])
def test_pickle_round_trip_preserves_backend_layout_and_partition_state(
    gather_class, array_type, is_compact
):
    source, canonical = _new_gather(
        gather_class, array_type, is_compact, size=4, npartitions=3
    )
    source.capacity = 7
    source.set_column_values(["a", "b", "c", "d"])
    source.elog.set_job_id(17)
    source.elog.log_error("contract", "round trip", ErrorSeverity.Complaint)

    restored = pickle.loads(pickle.dumps(source))

    _assert_backend(restored, array_type)
    assert restored.is_compact is is_compact
    assert restored.member_data.shape == source.member_data.shape
    assert np.array_equal(
        _stored_values(restored), _expected_stored(canonical, is_compact)
    )
    assert restored.size == 4
    assert restored.capacity == 7
    assert restored.npartitions == 3
    assert restored.is_parallel is (array_type in ("dask", "xarray"))
    assert restored.num_components == source.num_components
    assert restored.npts == source.npts
    assert restored.column_values() == ["a", "b", "c", "d"]
    assert restored.ensemble_metadata() == source.ensemble_metadata()
    pd.testing.assert_frame_equal(restored.member_metadata, source.member_metadata)
    assert restored.elog.get_job_id() == 17
    restored_log = restored.elog.get_error_log()
    assert len(restored_log) == 1
    assert restored_log[0].algorithm == "contract"
    assert restored_log[0].message == "round trip"
    assert restored_log[0].badness == ErrorSeverity.Complaint
    if array_type in ("dask", "xarray"):
        restored_data = (
            restored.member_data.data
            if isinstance(restored.member_data, xr.DataArray)
            else restored.member_data
        )
        source_data = (
            source.member_data.data
            if isinstance(source.member_data, xr.DataArray)
            else source.member_data
        )
        assert restored_data.chunks == source_data.chunks
