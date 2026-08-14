from types import SimpleNamespace
from unittest.mock import Mock

import matplotlib.pyplot as plt
import numpy as np
import pytest

import mspasspy.graphics as graphics
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble


@pytest.fixture(autouse=True)
def close_figures():
    plt.close("all")
    yield
    plt.close("all")


@pytest.mark.parametrize("t0", (3.5, -4.5))
def test_wtva_raw_uses_exact_sample_coordinates_and_bounds(t0):
    section = np.array([[1.0, -1.0], [2.0, -2.0], [3.0, -3.0]])

    graphics.wtva_raw(section, t0, 0.25, color=None)

    expected = t0 + np.arange(3) * 0.25
    axes = plt.gca()
    assert len(axes.lines) == 2
    for line in axes.lines:
        np.testing.assert_array_equal(line.get_ydata(), expected)
    assert axes.get_ylim() == pytest.approx((expected[-1], expected[0]))


@pytest.mark.parametrize("t0", (3.5, -4.5))
def test_image_raw_uses_exact_first_and_last_sample_bounds(t0):
    section = np.arange(6.0).reshape(3, 2)

    graphics.image_raw(section, t0, 0.25, ranges=(10.0, 20.0), aspect=1.0)

    assert plt.gca().images[0].get_extent() == pytest.approx((9.5, 20.5, t0 + 0.5, t0))


@pytest.mark.parametrize("t0", (3.5, -4.5))
def test_atomic_wiggle_uses_exact_sample_coordinates_and_bounds(t0):
    datum = TimeSeries(4)
    datum.t0 = t0
    datum.dt = 0.25
    for index, value in enumerate((1.0, 2.0, 3.0, 4.0)):
        datum.data[index] = value
    plotter = graphics.SeismicPlotter()

    plotter._wtva_TimeSeries(datum, False)

    expected = t0 + np.arange(4) * 0.25
    axes = plt.gca()
    np.testing.assert_array_equal(axes.lines[0].get_xdata(), expected)
    assert axes.get_xlim() == pytest.approx((expected[0], expected[-1]))


@pytest.mark.parametrize("t0", (3.5, -4.5))
def test_atomic_image_uses_exact_first_and_last_sample_bounds(t0):
    datum = TimeSeries(4)
    datum.t0 = t0
    datum.dt = 0.25
    for index, value in enumerate((1.0, 2.0, 3.0, 4.0)):
        datum.data[index] = value
    plotter = graphics.SeismicPlotter()

    plotter._imageplot_TimeSeries(datum)

    assert plt.gca().images[0].get_extent() == pytest.approx(
        (t0, t0 + 3 * 0.25, -1.0, 1.0)
    )


def test_constant_normalization_produces_only_finite_zero_offsets():
    section = np.full((4, 2), 7.0)

    graphics.wtva_raw(section, -2.0, 0.5, ranges=(0.0, 2.0), color=None, normalize=True)

    for trace_number, line in enumerate(plt.gca().lines):
        offsets = np.asarray(line.get_xdata()) - trace_number
        assert np.isfinite(offsets).all()
        np.testing.assert_array_equal(offsets, np.zeros(4))


@pytest.mark.parametrize("function", (graphics.wtva_raw, graphics.image_raw))
@pytest.mark.parametrize("shape", ((0, 2), (2, 0)))
def test_raw_plotters_reject_empty_input(function, shape):
    with pytest.raises(IndexError, match="Nothing to plot|empty"):
        function(np.empty(shape), 1.0, 0.5)


@pytest.mark.parametrize("function", (graphics.wtva_raw, graphics.image_raw))
@pytest.mark.parametrize("dt", (0.0, -1.0, np.nan, np.inf, -np.inf))
def test_raw_plotters_reject_invalid_sample_intervals(function, dt):
    with pytest.raises(ValueError, match="finite and positive"):
        function(np.ones((2, 1)), 1.0, dt)


@pytest.mark.parametrize(
    "converter,datum_type",
    ((graphics.ts2nparray, TimeSeries), (graphics.seis2nparray, Seismogram)),
)
def test_atomic_converters_preserve_empty_array_result(converter, datum_type):
    t0, dt, data = converter(datum_type())

    assert np.asarray(data).size == 0
    assert isinstance(t0, float)
    assert isinstance(dt, float)


@pytest.mark.parametrize(
    "converter,datum_type",
    ((graphics.ts2nparray, TimeSeries), (graphics.seis2nparray, Seismogram)),
)
@pytest.mark.parametrize("dt", (0.0, -1.0, np.nan, np.inf, -np.inf))
def test_atomic_converters_reject_invalid_sample_intervals(converter, datum_type, dt):
    datum = datum_type(2)
    datum.dt = dt

    with pytest.raises(ValueError, match="finite and positive"):
        converter(datum)


def test_ensemble_converter_rejects_empty_input():
    with pytest.raises(IndexError, match="empty ensemble"):
        graphics.tse2nparray(TimeSeriesEnsemble())


@pytest.mark.parametrize("dt", (0.0, -1.0, np.nan, np.inf, -np.inf))
def test_ensemble_converter_rejects_invalid_sample_intervals(dt):
    ensemble = TimeSeriesEnsemble()
    member = TimeSeries(2)
    member.dt = dt
    ensemble.member.append(member)

    with pytest.raises(ValueError, match="finite and positive"):
        graphics.tse2nparray(ensemble)


def test_plotter_atomic_paths_reject_empty_and_invalid_grids():
    plotter = graphics.SeismicPlotter()

    with pytest.raises(IndexError, match="empty"):
        plotter._wtva(TimeSeries(), False)
    invalid = TimeSeries(2)
    invalid.dt = 0.0
    with pytest.raises(ValueError, match="finite and positive"):
        plotter._imageplot(invalid)


def test_checked_allocation_does_not_invent_a_512_mib_policy_limit(monkeypatch):
    zeros = Mock(return_value=object())
    monkeypatch.setattr(graphics.numpy, "zeros", zeros)
    one_byte_over_old_issue_limit = 536870913

    result = graphics._allocate_plot_matrix(
        one_byte_over_old_issue_limit, 1, dtype=np.uint8
    )

    assert result is zeros.return_value
    zeros.assert_called_once_with(
        shape=(one_byte_over_old_issue_limit, 1), dtype=np.dtype(np.uint8)
    )


@pytest.mark.parametrize(
    "rows,columns,dtype",
    (
        (np.iinfo(np.intp).max, 2, np.uint8),
        (np.iinfo(np.intp).max // np.dtype(np.float64).itemsize + 1, 1, np.float64),
    ),
)
def test_checked_allocation_rejects_integer_multiplication_overflow(
    monkeypatch, rows, columns, dtype
):
    zeros = Mock()
    monkeypatch.setattr(graphics.numpy, "zeros", zeros)

    with pytest.raises(MemoryError, match="overflow"):
        graphics._allocate_plot_matrix(rows, columns, dtype=dtype)

    zeros.assert_not_called()


def test_ensemble_conversion_preserves_ten_million_row_sanity_limit(monkeypatch):
    assert (
        graphics._validate_ensemble_matrix_rows(graphics._MAX_ENSEMBLE_MATRIX_ROWS)
        is None
    )
    zeros = Mock()
    monkeypatch.setattr(graphics.numpy, "zeros", zeros)
    rows = graphics._MAX_ENSEMBLE_MATRIX_ROWS + 1
    member = SimpleNamespace(
        npts=2,
        dt=1.0,
        t0=0.0,
        endtime=lambda: float(rows - 1),
    )
    ensemble = SimpleNamespace(member=[member])

    with pytest.raises(RuntimeError, match="irrational computed time range"):
        graphics.tse2nparray(ensemble)

    zeros.assert_not_called()


@pytest.mark.parametrize("converter", (graphics.ts2nparray, graphics.seis2nparray))
def test_atomic_conversion_has_no_new_policy_size_limit(monkeypatch, converter):
    array = Mock(return_value=object())
    monkeypatch.setattr(graphics.numpy, "array", array)
    npts = 536870913
    datum = SimpleNamespace(t0=0.0, dt=1.0, npts=npts, data=object())

    _, _, result = converter(datum)

    assert result is array.return_value
    array.assert_called_once_with(datum.data)


def test_atomic_image_rejects_platform_size_overflow_before_allocation(monkeypatch):
    zeros = Mock()
    monkeypatch.setattr(graphics.numpy, "zeros", zeros)
    npts = np.iinfo(np.intp).max // np.dtype(np.float64).itemsize + 1
    datum = SimpleNamespace(t0=0.0, dt=1.0, npts=npts, data=object())

    with pytest.raises(MemoryError, match="overflow"):
        graphics.SeismicPlotter()._imageplot_TimeSeries(datum)

    zeros.assert_not_called()


def test_ensemble_image_rejects_platform_size_overflow_before_allocation(monkeypatch):
    zeros = Mock()
    monkeypatch.setattr(graphics.numpy, "zeros", zeros)
    columns = np.iinfo(np.intp).max // (2 * np.dtype(np.float64).itemsize) + 1
    member = SimpleNamespace(dt=1.0 / (columns - 1))
    ensemble = SimpleNamespace(member=[member, member])
    plotter = graphics.SeismicPlotter()
    monkeypatch.setattr(plotter, "_get_ensemble_size", lambda _: (2, 0.0, 1.0))

    with pytest.raises(MemoryError, match="overflow"):
        plotter._imageplot_TimeSeriesEnsemble(ensemble)

    zeros.assert_not_called()
