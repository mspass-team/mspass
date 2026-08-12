import pytest

from mspasspy.algorithms.snr import FD_snr_estimator, visualize_qcdata
from mspasspy.ccore.seismic import TimeSeries
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError


def _assert_no_output(capsys):
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


@pytest.mark.parametrize("bad", [object(), [], "timeseries"])
def test_visualize_wrong_type_reports_actual_type_without_output(bad, capsys):
    with pytest.raises(TypeError) as captured:
        visualize_qcdata(bad)

    message = str(captured.value)
    assert "visualize_qcdata" in message
    assert "TimeSeries or Seismogram" in message
    assert str(type(bad)) in message
    _assert_no_output(capsys)


@pytest.mark.parametrize(
    "subdocument,missing",
    [
        (None, ("signal_spectrum", "noise_spectrum")),
        ({}, ("signal_spectrum", "noise_spectrum")),
        ({"signal_spectrum": b"unused"}, ("noise_spectrum",)),
        ({"noise_spectrum": b"unused"}, ("signal_spectrum",)),
    ],
)
def test_visualize_missing_spectrum_raises_complete_invalid_error(
    subdocument, missing, capsys
):
    datum = TimeSeries(4)
    datum.set_live()
    if subdocument is not None:
        datum["Parrival"] = subdocument

    with pytest.raises(MsPASSError) as captured:
        visualize_qcdata(datum)

    error = captured.value
    assert error.severity == ErrorSeverity.Invalid
    assert "visualize_qcdata" in error.message
    assert "missing required spectrum metadata" in error.message
    for key in missing:
        assert key in error.message
    for key in {"signal_spectrum", "noise_spectrum"} - set(missing):
        assert key not in error.message.splitlines()[0]
    _assert_no_output(capsys)


def test_taper_count_equal_to_boundary_is_accepted_without_output(capsys):
    datum = TimeSeries(4)
    datum.kill()

    result, elog = FD_snr_estimator(datum, tbp=4.0, ntapers=8)

    assert result == {}
    assert elog.size() == 1
    _assert_no_output(capsys)


def test_taper_count_above_boundary_reports_values_and_inequality(capsys):
    datum = TimeSeries(4)
    datum.kill()

    with pytest.raises(MsPASSError) as captured:
        FD_snr_estimator(datum, tbp=4.0, ntapers=9)

    error = captured.value
    assert error.severity == ErrorSeverity.Fatal
    assert "ntapers=9" in error.message
    assert "tbp=4.0" in error.message
    assert "ntapers must be <= round(2*tbp)=8" in error.message
    _assert_no_output(capsys)
