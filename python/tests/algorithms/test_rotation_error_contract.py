import inspect

import numpy as np
import pytest

from mspasspy.algorithms.basic import transform_to_LQT, transform_to_RTZ
from mspasspy.ccore.seismic import Seismogram, SeismogramEnsemble
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError


def _seismogram():
    datum = Seismogram(4)
    datum.t0 = 1.25
    datum.dt = 0.1
    datum.set_live()
    datum["tag"] = "unchanged"
    datum.data[0, :] = [1.0, 2.0, 3.0, 4.0]
    datum.data[1, :] = [-1.0, -2.0, -3.0, -4.0]
    datum.data[2, :] = [0.5, 1.5, 2.5, 3.5]
    return datum


def _snapshot(datum):
    return {
        "metadata": dict(datum),
        "data": np.array(datum.data, copy=True),
        "npts": datum.npts,
        "t0": datum.t0,
        "dt": datum.dt,
        "live": datum.live,
        "cardinal": datum.cardinal(),
        "orthogonal": datum.orthogonal(),
        "tmatrix": np.array(datum.tmatrix, copy=True),
        "is_utc": datum.time_is_UTC(),
        "elog_size": datum.elog.size(),
    }


def _assert_unchanged(datum, before):
    assert dict(datum) == before["metadata"]
    np.testing.assert_array_equal(np.array(datum.data), before["data"])
    assert datum.npts == before["npts"]
    assert datum.t0 == before["t0"]
    assert datum.dt == before["dt"]
    assert datum.live == before["live"]
    assert datum.cardinal() == before["cardinal"]
    assert datum.orthogonal() == before["orthogonal"]
    np.testing.assert_array_equal(np.array(datum.tmatrix), before["tmatrix"])
    assert datum.time_is_UTC() == before["is_utc"]
    assert datum.elog.size() == before["elog_size"]


def test_rtz_treats_zero_as_an_explicit_angle():
    datum = _seismogram()
    expected_samples = np.array(datum.data, copy=True)
    datum["seaz"] = 37.0
    result = transform_to_RTZ(datum, phi=np.float64(0.0), angle_units="degrees")

    assert result is datum
    assert result.live
    assert result.elog.size() == 0
    np.testing.assert_allclose(np.array(result.data), expected_samples)


@pytest.mark.parametrize("key_is_backazimuth", [True, False])
@pytest.mark.parametrize("handles_ensembles", [True, False])
def test_rtz_ensemble_forwards_angle_convention(key_is_backazimuth, handles_ensembles):
    source = _seismogram()
    source["angle"] = 37.0
    expected_phi_degrees = -127.0 if key_is_backazimuth else 37.0
    expected = Seismogram(source)
    expected.rotate_to_standard()
    expected.rotate(np.radians(expected_phi_degrees))
    alternate = Seismogram(source)
    alternate.rotate_to_standard()
    alternate.rotate(np.radians(37.0 if key_is_backazimuth else -127.0))
    assert not np.allclose(np.array(expected.data), np.array(alternate.data))

    atomic = transform_to_RTZ(
        Seismogram(source),
        key="angle",
        key_is_backazimuth=key_is_backazimuth,
    )
    np.testing.assert_allclose(np.array(atomic.data), np.array(expected.data))

    ensemble = SeismogramEnsemble()
    ensemble.member.append(Seismogram(source))
    ensemble.member.append(Seismogram(source))
    ensemble.set_live()
    result = transform_to_RTZ(
        ensemble,
        key="angle",
        key_is_backazimuth=key_is_backazimuth,
        handles_ensembles=handles_ensembles,
    )

    assert result is ensemble
    assert result.live
    assert len(result.member) == 2
    for member in result.member:
        assert member.live
        np.testing.assert_allclose(np.array(member.data), np.array(expected.data))


@pytest.mark.parametrize(
    "phi,theta",
    [
        (None, 0.0),
        (0.0, None),
        ("0", 0.0),
        (0.0, object()),
        (True, 0.0),
        (0.0, False),
        (float("nan"), 0.0),
        (0.0, float("inf")),
        (float("-inf"), 0.0),
    ],
)
def test_lqt_rejects_invalid_explicit_angle_pairs_before_mutation(phi, theta):
    datum = _seismogram()
    before = _snapshot(datum)

    with pytest.raises(TypeError):
        transform_to_LQT(datum, phi=phi, theta=theta)

    _assert_unchanged(datum, before)


@pytest.mark.parametrize("handles_ensembles", [False, True])
def test_lqt_rejects_invalid_pair_before_mutating_an_ensemble(handles_ensembles):
    ensemble = SeismogramEnsemble()
    ensemble["tag"] = "unchanged"
    for index in range(2):
        member = _seismogram()
        member["member_index"] = index
        ensemble.member.append(member)
    ensemble.set_live()
    ensemble_metadata = dict(ensemble)
    before = [_snapshot(member) for member in ensemble.member]

    with pytest.raises(TypeError):
        transform_to_LQT(
            ensemble,
            phi=0.0,
            theta=None,
            handles_ensembles=handles_ensembles,
        )

    assert dict(ensemble) == ensemble_metadata
    assert ensemble.live
    assert ensemble.elog.size() == 0
    for member, member_before in zip(ensemble.member, before):
        _assert_unchanged(member, member_before)


def test_lqt_guard_validates_before_dryrun_and_valid_dryrun_is_nonmutating():
    datum = _seismogram()
    before = _snapshot(datum)

    with pytest.raises(TypeError):
        transform_to_LQT(datum, phi=0.0, theta=None, dryrun=True)
    _assert_unchanged(datum, before)

    assert transform_to_LQT(datum, phi=0.0, theta=0.0, dryrun=True) == "OK"
    _assert_unchanged(datum, before)


def test_lqt_accepts_metadata_or_two_finite_numeric_angles():
    metadata_datum = _seismogram()
    metadata_datum["seaz"] = 240.0
    metadata_datum["ema"] = 10.0
    metadata_result = transform_to_LQT(metadata_datum, phi=None, theta=None)
    assert metadata_result is metadata_datum
    assert metadata_result.live

    for phi, theta in [(0.0, 0.0), (np.float64(15.0), np.float32(5.0))]:
        datum = _seismogram()
        result = transform_to_LQT(datum, phi=phi, theta=theta)
        assert result is datum
        assert result.live
        assert result.elog.size() == 0


class _BrokenRotation(Seismogram):
    def __init__(self, message, severity):
        super().__init__(3)
        self.set_live()
        self._message = message
        self._severity = severity

    def rotate_to_standard(self):
        raise MsPASSError(self._message, self._severity)


@pytest.mark.parametrize(
    "transform,kwargs,algorithm",
    [
        (transform_to_RTZ, {"phi": 0.0}, "transform_to_RTZ"),
        (transform_to_LQT, {"phi": 0.0, "theta": 0.0}, "transform_to_LQT"),
    ],
)
@pytest.mark.parametrize(
    "severity", [ErrorSeverity.Complaint, ErrorSeverity.Invalid, ErrorSeverity.Fatal]
)
def test_rotation_mspass_error_preserves_message_severity_and_identity(
    transform, kwargs, algorithm, severity
):
    message = "complete injected rotation failure\nwith details"
    datum = _BrokenRotation(message, severity)

    result = transform(datum, **kwargs)

    assert result is datum
    assert result.dead()
    assert result.elog.size() == 1
    error = result.elog.get_error_log()[0]
    assert error.algorithm == algorithm
    assert error.message == message
    assert error.badness == severity


def test_lqt_outer_guard_preserves_the_public_signature():
    signature = inspect.signature(transform_to_LQT)
    assert signature.parameters["phi"].default is None
    assert signature.parameters["theta"].default is None
