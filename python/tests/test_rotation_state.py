import numpy as np
import pytest

from mspasspy.ccore.seismic import Seismogram
from mspasspy.ccore.utility import SphericalCoordinate


def make_test_seismogram():
    result = Seismogram(2)
    result.dt = 0.125
    result.t0 = -2.0
    result.set_live()
    result.data[:, :] = np.array([[1.0, -4.0], [2.0, 5.0], [3.0, -6.0]])
    return result


@pytest.mark.parametrize(
    "theta, expected_transform, restore",
    [
        (0.0, [[0.0, -1.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, 1.0]], False),
        (
            np.pi / 2.0,
            [[0.0, -1.0, 0.0], [0.0, 0.0, -1.0], [1.0, 0.0, 0.0]],
            False,
        ),
        (np.pi, [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, -1.0]], True),
    ],
)
def test_spherical_rotation_state(theta, expected_transform, restore):
    original = make_test_seismogram()
    rotated = Seismogram(original)
    direction = SphericalCoordinate()
    direction.radius = 1.0
    direction.phi = 0.0
    direction.theta = theta

    rotated.rotate(direction)

    expected_transform = np.asarray(expected_transform)
    np.testing.assert_allclose(rotated.tmatrix, expected_transform, atol=1.0e-12)
    np.testing.assert_allclose(
        rotated.data, expected_transform @ np.asarray(original.data), atol=1.0e-12
    )
    assert rotated.cardinal() is False
    assert rotated.orthogonal() is True

    if restore:
        rotated.rotate_to_standard()
        np.testing.assert_allclose(rotated.data, original.data, atol=1.0e-12)
        np.testing.assert_allclose(rotated.tmatrix, original.tmatrix, atol=1.0e-12)
        assert rotated.npts == original.npts
        assert rotated.dt == original.dt
        assert rotated.t0 == original.t0
        assert rotated.live == original.live
        assert rotated.tref == original.tref
        assert rotated.cardinal() == original.cardinal()
        assert rotated.orthogonal() == original.orthogonal()
