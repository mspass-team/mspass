import numpy as np
import pytest

from mspasspy.ccore.algorithms.basic import _WindowData, _WindowData3C
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.seismic import Seismogram, TimeSeries
from mspasspy.ccore.utility import AtomicType, ErrorSeverity


def _make_timeseries():
    data = TimeSeries(5)
    data.t0 = 10.0
    data.dt = 0.5
    data.set_live()
    data["boundary_marker"] = "preserve-me"
    data.set_jobname("boundary-job")
    data.set_jobid("boundary-job-id")
    data.set_as_origin(
        "boundary-source",
        "source-id",
        "boundary-uuid",
        AtomicType.TIMESERIES,
        True,
    )
    data.elog.log_error(
        "fixture", "preexisting informational entry", ErrorSeverity.Informational
    )
    for sample in range(data.npts):
        data.data[sample] = sample + 1.0
    return data


def _make_seismogram():
    data = Seismogram(5)
    data.t0 = 10.0
    data.dt = 0.5
    data.set_live()
    data["boundary_marker"] = "preserve-me"
    data.set_jobname("boundary-job")
    data.set_jobid("boundary-job-id")
    data.set_as_origin(
        "boundary-source",
        "source-id",
        "boundary-uuid",
        AtomicType.SEISMOGRAM,
        True,
    )
    data.elog.log_error(
        "fixture", "preexisting informational entry", ErrorSeverity.Informational
    )
    for component in range(3):
        data.data[component, :] = 100.0 * (component + 1) + np.arange(5)
    return data


def _history_signature(data):
    node = data.current_nodedata()
    return (
        data.jobname(),
        data.jobid(),
        data.stage(),
        data.id(),
        node.status,
        node.uuid,
        node.algorithm,
        node.algid,
        node.stage,
        node.type,
        len(data.get_nodes()),
    )


@pytest.mark.parametrize(
    ("factory", "window_data"),
    [(_make_timeseries, _WindowData), (_make_seismogram, _WindowData3C)],
)
def test_windowdata_binding_accepts_last_sample_and_value_below_half_tie(
    factory, window_data
):
    parent = factory()
    original = np.array(parent.data, copy=True)
    half_sample_tie = parent.endtime() + 0.5 * parent.dt
    below_tie = np.nextafter(half_sample_tie, -np.inf)

    for end in (parent.endtime(), below_tie):
        result = window_data(parent, TimeWindow(parent.t0, end))
        assert result.live
        assert result.npts == parent.npts
        assert result.t0 == parent.t0
        assert result.endtime() == parent.endtime()
        assert result["boundary_marker"] == "preserve-me"
        assert _history_signature(result) == _history_signature(parent)
        assert result.elog.size() == parent.elog.size()
        np.testing.assert_array_equal(result.data, original)


@pytest.mark.parametrize(
    ("factory", "window_data"),
    [(_make_timeseries, _WindowData), (_make_seismogram, _WindowData3C)],
)
def test_windowdata_binding_rejects_half_tie_and_later_without_fabricating_data(
    factory, window_data
):
    parent = factory()
    original = np.array(parent.data, copy=True)
    original_history = _history_signature(parent)
    half_sample_tie = parent.endtime() + 0.5 * parent.dt

    for end in (half_sample_tie, parent.endtime() + parent.dt):
        result = window_data(parent, TimeWindow(parent.t0, end))
        assert result.dead()
        assert result.npts == 0
        assert np.asarray(result.data).size == 0
        assert result.t0 == parent.t0
        assert result.dt == parent.dt
        assert result["boundary_marker"] == "preserve-me"
        assert _history_signature(result) == original_history
        errors = result.elog.get_error_log()
        assert len(errors) == parent.elog.size() + 1
        assert sum(error.badness == ErrorSeverity.Invalid for error in errors) == 1
        assert errors[-1].badness == ErrorSeverity.Invalid

    assert parent.live
    assert parent.npts == 5
    assert _history_signature(parent) == original_history
    np.testing.assert_array_equal(parent.data, original)
