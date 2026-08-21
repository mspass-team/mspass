import inspect
import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path

import numpy as np
import pytest

import mspasspy.algorithms.MCXcorStacking as mcxcor_module
import mspasspy.ccore.seismic as seismic_binding
from mspasspy.algorithms.MCXcorStacking import (
    amplitude_relative_to_beam,
    beam_coherence,
    beam_correlation,
    remove_incident_wavefield,
)
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.seismic import DoubleVector, TimeReferenceType, TimeSeries

GRID_TOLERANCE = 1.0e-6


def _timeseries(values, t0=0.0, dt=1.0):
    result = TimeSeries(len(values))
    result.t0 = t0
    result.dt = dt
    result.tref = TimeReferenceType.Relative
    result.data[:] = DoubleVector(values)
    result.set_live()
    result["contract_marker"] = f"{t0}:{dt}:{len(values)}"
    return result


def _snapshot(datum):
    logs = [
        (entry.algorithm, entry.message, entry.badness)
        for entry in datum.elog.get_error_log()
    ]
    return (
        datum.t0,
        datum.dt,
        datum.npts,
        datum.tref,
        dict(datum),
        logs,
        np.asarray(datum.data).copy(),
        datum.live,
    )


def _assert_unchanged(datum, snapshot):
    t0, dt, npts, tref, metadata, logs, data, live = snapshot
    assert datum.t0 == t0
    assert datum.dt == dt
    assert datum.npts == npts
    assert datum.tref == tref
    assert dict(datum) == metadata
    assert [
        (entry.algorithm, entry.message, entry.badness)
        for entry in datum.elog.get_error_log()
    ] == logs
    np.testing.assert_array_equal(datum.data, data)
    assert datum.live is live


def _assert_module_from_selected_build(module, relative_path):
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if source_root:
        expected_module = Path(source_root) / relative_path
    else:
        expected_module = distribution("mspasspy").locate_file(relative_path)
        installed_version = version("mspasspy")
        installed_commit = installed_version.partition("+g")[2].partition(".")[0]
        assert installed_commit, "installed mspasspy version lacks a source commit"
        repository_root = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / ".git").exists()
        )
        checkout_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, text=True
        ).strip()
        assert checkout_commit.startswith(installed_commit)
    assert Path(module.__file__).resolve() == Path(expected_module).resolve()


def test_contract_suite_uses_selected_build_and_real_binding():
    _assert_module_from_selected_build(
        mcxcor_module, "mspasspy/algorithms/MCXcorStacking.py"
    )
    assert Path(seismic_binding.__file__).suffix == ".so"
    assert str(inspect.signature(remove_incident_wavefield)) == (
        "(d, beam, *args, handles_ensembles=True, **kwargs)"
    )


def test_aligned_metrics_use_the_full_inclusive_interval_without_mutation():
    datum = _timeseries([1.0, 2.0, 3.0])
    beam = _timeseries([1.0, 2.0, 3.0])
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert beam_correlation(datum, beam) == pytest.approx(1.0)
    assert beam_coherence(datum, beam) == pytest.approx(1.0)
    assert amplitude_relative_to_beam(datum, beam) == pytest.approx(np.sqrt(14.0) / 3.0)

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


def test_coherence_uses_projected_residual_relative_to_datum_norm():
    datum = _timeseries([1.0, 1.0])
    beam = _timeseries([1.0, 0.0])

    assert beam_coherence(datum, beam) == pytest.approx(1.0 - 1.0 / np.sqrt(2.0))


def test_unnormalized_relative_amplitude_is_the_documented_dot_product():
    datum = _timeseries([1.0, 2.0])
    beam = _timeseries([3.0, 4.0])

    assert amplitude_relative_to_beam(
        datum, beam, normalize_beam=False
    ) == pytest.approx(11.0 / 2.0)


def test_integer_shifted_partial_overlap_uses_independent_indices_and_endpoints():
    datum = _timeseries([100.0, 1.0, 2.0, 3.0], t0=0.0)
    beam = _timeseries([1.0, 2.0, 3.0, 100.0], t0=1.0)
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert beam_correlation(datum, beam) == pytest.approx(1.0)
    assert beam_coherence(datum, beam) == pytest.approx(1.0)
    assert amplitude_relative_to_beam(datum, beam) == pytest.approx(np.sqrt(14.0) / 3.0)

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)

    normalized_beam = _timeseries(
        [1.0 / np.sqrt(14.0), 2.0 / np.sqrt(14.0), 3.0 / np.sqrt(14.0), 100.0],
        t0=1.0,
    )
    beam_before = _snapshot(normalized_beam)
    output = remove_incident_wavefield(datum, normalized_beam)

    assert output is datum
    np.testing.assert_allclose(datum.data, [100.0, 0.0, 0.0, 0.0], atol=1.0e-14)
    _assert_unchanged(normalized_beam, beam_before)


def test_unaligned_correlation_shifts_only_a_copy():
    datum = _timeseries([0.0, 1.0, 2.0, 0.0])
    beam = _timeseries([1.0, 2.0, 0.0, 0.0])
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert beam_correlation(datum, beam, aligned=False) == pytest.approx(1.0)

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


def test_unaligned_correlation_aligns_initially_disjoint_time_ranges():
    datum = _timeseries([0.0, 1.0, 2.0, 0.0], t0=10.0)
    beam = _timeseries([1.0, 2.0, 0.0, 0.0], t0=0.0)
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert beam_correlation(datum, beam, aligned=False) == pytest.approx(1.0)

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


def test_incident_removal_outer_guard_preserves_dryrun_api_and_inputs():
    datum = _timeseries([1.0, 2.0, 3.0])
    beam = _timeseries([1.0, 2.0, 3.0])
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert remove_incident_wavefield(datum, beam, dryrun=True) == "OK"

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


def test_explicit_window_is_inclusive_and_does_not_add_padded_samples():
    datum = _timeseries([100.0, 1.0, 2.0, 100.0])
    beam = _timeseries([200.0, 1.0, 2.0, 200.0])
    window = TimeWindow(1.0, 2.0)

    assert beam_correlation(datum, beam, window=window) == pytest.approx(1.0)
    assert beam_coherence(datum, beam, window=window) == pytest.approx(1.0)
    assert amplitude_relative_to_beam(datum, beam, window=window) == pytest.approx(
        np.sqrt(5.0) / 2.0
    )


@pytest.mark.parametrize(
    "metric,sentinel",
    [
        pytest.param(beam_correlation, 0.0, id="correlation"),
        pytest.param(beam_coherence, 0.0, id="coherence"),
        pytest.param(amplitude_relative_to_beam, -1.0, id="amplitude"),
    ],
)
def test_window_with_no_physical_sample_returns_sentinel(metric, sentinel):
    datum = _timeseries([1.0, 2.0], t0=0.0, dt=1.0)
    beam = _timeseries([1.0, 2.0], t0=0.0, dt=1.0)
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert metric(datum, beam, window=TimeWindow(0.1, 0.4)) == sentinel

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


def test_one_sample_overlap_includes_the_shared_endpoint():
    datum = _timeseries([5.0], t0=2.0)
    beam = _timeseries([9.0, 8.0, 1.0], t0=0.0)

    assert beam_correlation(datum, beam) == pytest.approx(1.0)
    assert beam_coherence(datum, beam) == pytest.approx(1.0)
    assert amplitude_relative_to_beam(datum, beam) == pytest.approx(5.0)

    output = remove_incident_wavefield(datum, beam)
    assert output is datum
    np.testing.assert_allclose(datum.data, [0.0])


def test_no_overlap_returns_sentinels_and_incident_removal_identity():
    datum = _timeseries([1.0, 2.0], t0=0.0)
    beam = _timeseries([1.0, 2.0], t0=2.0)
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert beam_correlation(datum, beam) == 0.0
    assert beam_coherence(datum, beam) == 0.0
    assert amplitude_relative_to_beam(datum, beam) == -1.0
    assert remove_incident_wavefield(datum, beam) is datum

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


@pytest.mark.parametrize(
    "datum_values,beam_values,expected_amplitude",
    [
        ([0.0, 0.0, 0.0], [1.0, 2.0, 3.0], 0.0),
        ([1.0, 2.0, 3.0], [0.0, 0.0, 0.0], -1.0),
    ],
)
def test_zero_norm_overlap_preserves_metric_semantics_and_does_not_mutate(
    datum_values, beam_values, expected_amplitude
):
    datum = _timeseries(datum_values)
    beam = _timeseries(beam_values)
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert beam_correlation(datum, beam) == 0.0
    assert beam_coherence(datum, beam) == 0.0
    assert amplitude_relative_to_beam(datum, beam) == expected_amplitude
    assert remove_incident_wavefield(datum, beam) is datum

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


@pytest.mark.parametrize(
    "datum_values,beam_values",
    [
        ([0.0, 0.0, 0.0], [1.0, 2.0, 3.0]),
        ([1.0, 2.0, 3.0], [0.0, 0.0, 0.0]),
    ],
)
def test_unnormalized_relative_amplitude_does_not_require_nonzero_norm(
    datum_values, beam_values
):
    datum = _timeseries(datum_values)
    beam = _timeseries(beam_values)
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    assert amplitude_relative_to_beam(datum, beam, normalize_beam=False) == 0.0

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


METRIC_CALLS = [
    pytest.param(beam_correlation, id="correlation"),
    pytest.param(beam_coherence, id="coherence"),
    pytest.param(amplitude_relative_to_beam, id="amplitude"),
    pytest.param(remove_incident_wavefield, id="incident-removal"),
]


@pytest.mark.parametrize("metric", METRIC_CALLS)
def test_accumulated_dt_drift_within_grid_tolerance_is_accepted(metric):
    compatible_dt = 1.0 + 0.4 * GRID_TOLERANCE
    datum = _timeseries([1.0, 2.0, 3.0], dt=1.0)
    beam = _timeseries([1.0, 2.0, 3.0], dt=compatible_dt)

    metric(datum, beam)


@pytest.mark.parametrize("metric", METRIC_CALLS)
def test_accumulated_dt_drift_beyond_tolerance_raises_before_mutation(metric):
    incompatible_dt = 1.0 + 0.6 * GRID_TOLERANCE
    datum = _timeseries([1.0, 2.0, 3.0], dt=1.0)
    beam = _timeseries([1.0, 2.0, 3.0], dt=incompatible_dt)
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    with pytest.raises(ValueError, match="sample intervals are incompatible"):
        metric(datum, beam)

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)


@pytest.mark.parametrize("metric", METRIC_CALLS)
def test_start_grid_tolerance_is_symmetric_for_compatible_dt(metric):
    larger_dt = 1.0 + GRID_TOLERANCE / 2.0
    offset = np.nextafter(GRID_TOLERANCE * larger_dt, 0.0)
    first = _timeseries([0.0, 0.0], t0=offset, dt=1.0)
    second = _timeseries([0.0, 0.0], t0=0.0, dt=larger_dt)
    first_before = _snapshot(first)
    second_before = _snapshot(second)

    metric(first, second)
    metric(second, first)

    _assert_unchanged(first, first_before)
    _assert_unchanged(second, second_before)


@pytest.mark.parametrize("metric", METRIC_CALLS)
def test_start_grid_offset_at_tolerance_is_accepted(metric):
    datum = _timeseries([1.0, 2.0, 3.0], t0=GRID_TOLERANCE)
    beam = _timeseries([1.0, 2.0, 3.0], t0=0.0)

    metric(datum, beam)


@pytest.mark.parametrize("metric", METRIC_CALLS)
def test_start_grid_offset_beyond_tolerance_raises_before_mutation(metric):
    invalid_offset = np.nextafter(GRID_TOLERANCE, np.inf)
    datum = _timeseries([1.0, 2.0, 3.0], t0=invalid_offset)
    beam = _timeseries([1.0, 2.0, 3.0], t0=0.0)
    datum_before = _snapshot(datum)
    beam_before = _snapshot(beam)

    with pytest.raises(
        ValueError, match="start times are on incompatible sample grids"
    ):
        metric(datum, beam)

    _assert_unchanged(datum, datum_before)
    _assert_unchanged(beam, beam_before)
