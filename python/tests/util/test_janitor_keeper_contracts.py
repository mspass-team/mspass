import importlib
from pathlib import Path

import pytest
import yaml

from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)

janitor_module = importlib.import_module("mspasspy.util.Janitor")
Janitor = janitor_module.Janitor


def _write_keepers(path, prefix):
    keepers = {
        "TimeSeries": [f"{prefix}_timeseries"],
        "Seismogram": [f"{prefix}_seismogram"],
        "Ensemble": [f"{prefix}_ensemble"],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(keepers), encoding="utf-8")
    return keepers


def _assert_loaded(janitor, expected):
    assert janitor.TimeSeries_keepers == expected["TimeSeries"]
    assert janitor.Seismogram_keepers == expected["Seismogram"]
    assert janitor.ensemble_keepers == expected.get("Ensemble", [])


@pytest.fixture
def package_data_directory(tmp_path, monkeypatch):
    module_path = tmp_path / "installed/mspasspy/util/Janitor.py"
    module_path.parent.mkdir(parents=True)
    monkeypatch.setattr(janitor_module, "__file__", str(module_path))
    return module_path.parent.parent / "data/yaml"


def test_existing_relative_keeper_file_is_resolved_from_caller_cwd(
    tmp_path, monkeypatch
):
    expected = _write_keepers(tmp_path / "config/keepers.yaml", "caller")
    monkeypatch.chdir(tmp_path)

    _assert_loaded(Janitor(keepers_file="config/keepers.yaml"), expected)


def test_existing_bare_relative_file_wins_over_package_fallback(
    tmp_path, monkeypatch, package_data_directory
):
    _write_keepers(package_data_directory / "Janitor.yaml", "package")
    caller_directory = tmp_path / "caller"
    expected = _write_keepers(caller_directory / "Janitor.yaml", "caller")
    monkeypatch.chdir(caller_directory)

    _assert_loaded(Janitor(keepers_file="Janitor.yaml"), expected)


def test_absolute_keeper_file_is_used_directly(tmp_path, monkeypatch):
    expected = _write_keepers(tmp_path / "absolute.yaml", "absolute")
    other_directory = tmp_path / "other"
    other_directory.mkdir()
    monkeypatch.chdir(other_directory)

    _assert_loaded(Janitor(keepers_file=tmp_path / "absolute.yaml"), expected)


def test_missing_bare_name_falls_back_to_package_data(
    tmp_path, monkeypatch, package_data_directory
):
    expected = _write_keepers(package_data_directory / "keepers.yaml", "package")
    mspass_home = tmp_path / "mspass-home"
    _write_keepers(mspass_home / "data/yaml/keepers.yaml", "mspass_home")
    monkeypatch.setenv("MSPASS_HOME", str(mspass_home))
    caller_directory = tmp_path / "caller"
    caller_directory.mkdir()
    monkeypatch.chdir(caller_directory)

    _assert_loaded(Janitor(keepers_file="keepers.yaml"), expected)


def test_default_file_honors_mspass_home(tmp_path, monkeypatch, package_data_directory):
    _write_keepers(package_data_directory / "Janitor.yaml", "package")
    mspass_home = tmp_path / "mspass-home"
    expected = _write_keepers(mspass_home / "data/yaml/Janitor.yaml", "mspass_home")
    monkeypatch.setenv("MSPASS_HOME", str(mspass_home))

    _assert_loaded(Janitor(), expected)


def test_mspass_home_supplies_package_data_for_a_source_layout(
    tmp_path, monkeypatch, package_data_directory
):
    assert not package_data_directory.exists()
    mspass_home = tmp_path / "mspass-home"
    expected = _write_keepers(mspass_home / "data/yaml/source.yaml", "mspass_home")
    monkeypatch.setenv("MSPASS_HOME", str(mspass_home))
    caller_directory = tmp_path / "caller"
    caller_directory.mkdir()
    monkeypatch.chdir(caller_directory)

    _assert_loaded(Janitor(keepers_file="source.yaml"), expected)


def test_missing_nested_relative_path_does_not_use_package_fallback(
    tmp_path, monkeypatch, package_data_directory
):
    _write_keepers(package_data_directory / "missing/nested.yaml", "package")
    caller_directory = tmp_path / "caller"
    caller_directory.mkdir()
    monkeypatch.chdir(caller_directory)

    with pytest.raises(FileNotFoundError):
        Janitor(keepers_file="missing/nested.yaml")


def test_missing_bare_keeper_file_raises_file_not_found(
    tmp_path, monkeypatch, package_data_directory
):
    package_data_directory.mkdir(parents=True)
    caller_directory = tmp_path / "caller"
    caller_directory.mkdir()
    monkeypatch.chdir(caller_directory)

    with pytest.raises(FileNotFoundError):
        Janitor(keepers_file="missing.yaml")


def test_missing_absolute_keeper_file_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        Janitor(keepers_file=tmp_path / "missing.yaml")


def test_none_empty_and_nonempty_overrides_are_distinct(tmp_path):
    keeper_file = tmp_path / "defaults.yaml"
    expected_defaults = _write_keepers(keeper_file, "yaml")

    defaults = Janitor(keepers_file=keeper_file)
    _assert_loaded(defaults, expected_defaults)

    empty = Janitor(
        keepers_file=keeper_file,
        TimeSeries_keepers=[],
        Seismogram_keepers=[],
        ensemble_keepers=[],
    )
    assert empty.TimeSeries_keepers == []
    assert empty.Seismogram_keepers == []
    assert empty.ensemble_keepers == []

    custom = Janitor(
        keepers_file=keeper_file,
        TimeSeries_keepers=["member"],
        Seismogram_keepers=["three_component"],
        ensemble_keepers=["ensemble"],
    )
    assert custom.TimeSeries_keepers == ["member"]
    assert custom.Seismogram_keepers == ["three_component"]
    assert custom.ensemble_keepers == ["ensemble"]


@pytest.mark.parametrize(
    "member_type,ensemble_type,member_keeper",
    [
        (TimeSeries, TimeSeriesEnsemble, "timeseries_keep"),
        (Seismogram, SeismogramEnsemble, "seismogram_keep"),
    ],
)
def test_member_and_ensemble_keeper_namespaces_are_independent(
    tmp_path, member_type, ensemble_type, member_keeper
):
    keeper_file = tmp_path / "defaults.yaml"
    _write_keepers(keeper_file, "yaml")
    janitor = Janitor(
        keepers_file=keeper_file,
        TimeSeries_keepers=["timeseries_keep"],
        Seismogram_keepers=["seismogram_keep"],
        ensemble_keepers=["ensemble_keep"],
        process_ensemble_members=True,
    )
    member = member_type(1)
    member.set_live()
    member[member_keeper] = 1
    member["ensemble_keep"] = "member trash"
    member["trash"] = True
    ensemble = ensemble_type()
    ensemble.member.append(member)
    ensemble.set_live()
    ensemble["ensemble_keep"] = 2
    ensemble[member_keeper] = "ensemble trash"
    ensemble["trash"] = True

    result = janitor.clean(ensemble)

    assert set(result.keys()) == {"ensemble_keep"}
    assert result["ensemble_keep"] == 2
    assert set(result.member[0].keys()) == {member_keeper}
    assert result.member[0][member_keeper] == 1


@pytest.mark.parametrize(
    "member_type,ensemble_type",
    [
        (TimeSeries, TimeSeriesEnsemble),
        (Seismogram, SeismogramEnsemble),
    ],
)
def test_empty_keeper_lists_remove_all_user_metadata(
    tmp_path, member_type, ensemble_type
):
    keeper_file = tmp_path / "defaults.yaml"
    _write_keepers(keeper_file, "yaml")
    janitor = Janitor(
        keepers_file=keeper_file,
        TimeSeries_keepers=[],
        Seismogram_keepers=[],
        ensemble_keepers=[],
        process_ensemble_members=True,
    )
    member = member_type(1)
    member.set_live()
    member["member_value"] = 1
    ensemble = ensemble_type()
    ensemble.member.append(member)
    ensemble.set_live()
    ensemble["ensemble_value"] = 2

    result = janitor.clean(ensemble)

    assert list(result.keys()) == []
    assert list(result.member[0].keys()) == []
