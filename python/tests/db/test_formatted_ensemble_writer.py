from pathlib import Path

import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    DoubleVector,
    Seismogram,
    SeismogramEnsemble,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import dmatrix
from mspasspy.db.database import Database


def make_timeseries(index):
    datum = TimeSeries(5 + index)
    datum.data = DoubleVector([index * 10.0 + sample for sample in range(datum.npts)])
    return finish_datum(datum, index)


def make_seismogram(index):
    datum = Seismogram(5 + index)
    values = dmatrix(3, datum.npts)
    for component in range(3):
        for sample in range(datum.npts):
            values[component, sample] = 100.0 * index + 10.0 * component + sample
    datum.data = values
    return finish_datum(datum, index)


def finish_datum(datum, index):
    datum.t0 = 1_700_000_000.0 + 10.0 * index
    datum.dt = 0.05
    datum.tref = TimeReferenceType.UTC
    datum.set_live()
    return datum


CASES = [
    (TimeSeriesEnsemble, make_timeseries, TimeSeries),
    (SeismogramEnsemble, make_seismogram, Seismogram),
]


def write_ensemble(tmp_path, ensemble_type, factory, indices, dead_positions=()):
    ensemble = ensemble_type()
    for position, index in enumerate(indices):
        member = factory(index)
        if position in dead_positions:
            member.kill()
        ensemble.member.append(member)
    ensemble.set_live()
    filename = f"{ensemble_type.__name__}-{len(indices)}.mseed"
    result = Database._save_sample_data_to_file(
        None,
        ensemble,
        dir=str(tmp_path),
        dfile=filename,
        format="MSEED",
    )
    return result, tmp_path / filename


def read_member_interval(member, atomic_type, path):
    reconstructed = atomic_type(member)
    reconstructed.kill()
    Database._read_data_from_dfile(
        reconstructed,
        str(path.parent),
        path.name,
        member["foff"],
        member["nbytes"],
        format="MSEED",
    )
    return reconstructed


def assert_round_trip(original, reconstructed):
    assert reconstructed.live
    assert reconstructed.npts == original.npts
    assert reconstructed.dt == original.dt
    assert reconstructed.t0 == original.t0
    np.testing.assert_allclose(
        np.asarray(reconstructed.data), np.asarray(original.data)
    )


@pytest.mark.parametrize("ensemble_type,factory,atomic_type", CASES)
@pytest.mark.parametrize("member_count", [2, 3])
def test_each_formatted_member_owns_one_independent_interval(
    tmp_path, ensemble_type, factory, atomic_type, member_count
):
    ensemble, path = write_ensemble(
        tmp_path, ensemble_type, factory, range(member_count)
    )

    expected_offset = 0
    total_bytes = 0
    for member in ensemble.member:
        assert member["foff"] == expected_offset
        assert member["nbytes"] > 0
        assert member["format"] == "MSEED"
        assert member["storage_mode"] == "file"
        reconstructed = read_member_interval(member, atomic_type, path)
        assert_round_trip(member, reconstructed)
        expected_offset += member["nbytes"]
        total_bytes += member["nbytes"]

    assert path.stat().st_size == total_bytes


@pytest.mark.parametrize("ensemble_type,factory,atomic_type", CASES)
def test_dead_members_do_not_change_later_offsets_or_intervals(
    tmp_path, ensemble_type, factory, atomic_type
):
    ensemble, path = write_ensemble(
        tmp_path,
        ensemble_type,
        factory,
        [0, 99, 1],
        dead_positions={1},
    )
    first, dead, last = ensemble.member

    assert "foff" not in dead
    assert "nbytes" not in dead
    assert last["foff"] == first["nbytes"]
    assert path.stat().st_size == first["nbytes"] + last["nbytes"]
    assert_round_trip(first, read_member_interval(first, atomic_type, path))
    assert_round_trip(last, read_member_interval(last, atomic_type, path))

    # The bytes and offsets are the same as writing the live members without
    # the dead member between them.
    control_dir = Path(tmp_path) / "control"
    control_dir.mkdir()
    control, control_path = write_ensemble(control_dir, ensemble_type, factory, [0, 1])
    assert [member["foff"] for member in control.member] == [
        first["foff"],
        last["foff"],
    ]
    assert [member["nbytes"] for member in control.member] == [
        first["nbytes"],
        last["nbytes"],
    ]
    assert path.read_bytes() == control_path.read_bytes()
