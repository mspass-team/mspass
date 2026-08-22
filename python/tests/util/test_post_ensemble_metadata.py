import pytest

from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.util.converter import post_ensemble_metadata


@pytest.fixture(
    params=[
        (TimeSeries, TimeSeriesEnsemble),
        (Seismogram, SeismogramEnsemble),
    ],
    ids=["TimeSeriesEnsemble", "SeismogramEnsemble"],
)
def ensemble_types(request):
    return request.param


def _member(datum_type, live=True, **metadata):
    datum = datum_type(2)
    if live:
        datum.set_live()
    else:
        datum.kill()
    for key, value in metadata.items():
        datum[key] = value
    return datum


def _ensemble(ensemble_type, *members, **metadata):
    result = ensemble_type()
    for key, value in metadata.items():
        result[key] = value
    for member in members:
        result.member.append(member)
    return result


def _snapshot(ensemble):
    return dict(ensemble), [(datum.live, dict(datum)) for datum in ensemble.member]


@pytest.mark.parametrize("clean_members", [False, True])
def test_post_ensemble_metadata_uses_first_live_member_only(
    ensemble_types, clean_members
):
    datum_type, ensemble_type = ensemble_types
    ensemble = _ensemble(
        ensemble_type,
        _member(datum_type, False, shared="dead"),
        _member(datum_type, True, shared="first"),
        _member(datum_type, True, shared="last"),
    )

    result = post_ensemble_metadata(ensemble, ["shared"], clean_members=clean_members)

    assert result is None
    assert ensemble["shared"] == "first"
    assert [datum["shared"] for datum in ensemble.member] == [
        "dead",
        "first",
        "last",
    ]


def test_post_ensemble_metadata_leaves_all_dead_ensemble_unchanged(ensemble_types):
    datum_type, ensemble_type = ensemble_types
    ensemble = _ensemble(
        ensemble_type,
        _member(datum_type, False, shared="one"),
        _member(datum_type, False, shared="two"),
        original="metadata",
    )
    before = _snapshot(ensemble)

    result = post_ensemble_metadata(
        ensemble, ["shared"], check_all_members=True, clean_members=True
    )

    assert result is None
    assert _snapshot(ensemble) == before


@pytest.mark.parametrize("check_all_members", [False, True])
def test_post_ensemble_metadata_missing_key_is_atomic(
    ensemble_types, check_all_members
):
    datum_type, ensemble_type = ensemble_types
    members = [
        _member(datum_type, False, stable="dead", shared="dead"),
        _member(datum_type, True, stable="common"),
    ]
    if check_all_members:
        members = [
            _member(datum_type, False, stable="dead", shared="dead"),
            _member(datum_type, True, stable="common", shared="first"),
            _member(datum_type, True, stable="common"),
        ]
    ensemble = _ensemble(ensemble_type, *members, original="metadata")
    before = _snapshot(ensemble)

    with pytest.raises(MsPASSError) as excinfo:
        post_ensemble_metadata(
            ensemble,
            ["stable", "shared"],
            check_all_members=check_all_members,
            clean_members=True,
        )

    assert excinfo.value.severity == ErrorSeverity.Invalid
    assert "shared" in str(excinfo.value)
    assert _snapshot(ensemble) == before


def test_post_ensemble_metadata_mismatch_is_atomic(ensemble_types):
    datum_type, ensemble_type = ensemble_types
    ensemble = _ensemble(
        ensemble_type,
        _member(datum_type, False, stable="dead", shared="dead"),
        _member(datum_type, True, stable="common", shared="first"),
        _member(datum_type, True, stable="common", shared="last"),
        original="metadata",
    )
    before = _snapshot(ensemble)

    with pytest.raises(MsPASSError) as excinfo:
        post_ensemble_metadata(
            ensemble,
            ["stable", "shared"],
            check_all_members=True,
            clean_members=True,
        )

    assert excinfo.value.severity == ErrorSeverity.Invalid
    assert "shared" in str(excinfo.value)
    assert _snapshot(ensemble) == before


@pytest.mark.parametrize("clean_members", [False, True])
def test_post_ensemble_metadata_all_member_success(ensemble_types, clean_members):
    datum_type, ensemble_type = ensemble_types
    ensemble = _ensemble(
        ensemble_type,
        _member(datum_type, False),
        _member(datum_type, False, shared="dead"),
        _member(datum_type, True, shared="common"),
        _member(datum_type, True, shared="common"),
    )
    member_key_presence = ["shared" in member for member in ensemble.member]

    result = post_ensemble_metadata(
        ensemble,
        ["shared"],
        check_all_members=True,
        clean_members=clean_members,
    )

    assert result is None
    assert ensemble["shared"] == "common"
    expected_presence = (
        [False] * len(ensemble.member) if clean_members else member_key_presence
    )
    assert ["shared" in member for member in ensemble.member] == expected_presence
