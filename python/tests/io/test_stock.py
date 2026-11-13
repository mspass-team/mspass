"""Offline tests for locating MsPASS configuration and sample data files."""

from pathlib import Path

import pytest

import mspasspy.io.stock as stock


def test_current_directory_overrides_data_directories(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    local = tmp_path / "ChannelPreferences.yaml"
    local.write_text("local", encoding="utf-8")
    fallback = tmp_path / "data" / "yaml" / local.name
    fallback.parent.mkdir(parents=True)
    fallback.write_text("fallback", encoding="utf-8")

    assert stock.datafile(local.name, verbose=True) == local
    assert f"Trying path= {tmp_path}" in capsys.readouterr().out


def test_project_data_directory_is_searched(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    filename = tmp_path / "data" / "yaml" / "example.yaml"
    filename.parent.mkdir(parents=True)
    filename.write_text("example", encoding="utf-8")
    monkeypatch.delenv("MSPASS_HOME", raising=False)

    assert stock.datafile(filename.name) == filename


def test_mspass_home_data_directory_is_searched(tmp_path, monkeypatch):
    workdir = tmp_path / "work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    home = tmp_path / "home"
    file = home / "data" / "pf" / "example.pf"
    file.parent.mkdir(parents=True)
    file.write_text("parameter", encoding="utf-8")
    monkeypatch.setenv("MSPASS_HOME", str(home))

    assert stock.datafile("example.pf", filetype="pf") == file


def test_packaged_data_directory_is_searched(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MSPASS_HOME", raising=False)
    package = tmp_path / "installed" / "mspasspy"
    module = package / "io" / "stock.py"
    module.parent.mkdir(parents=True)
    monkeypatch.setattr(stock, "__file__", str(module))
    bundled = package / "data" / "yaml" / "packaged.yaml"
    bundled.parent.mkdir(parents=True)
    bundled.write_text("bundled", encoding="utf-8")

    assert stock.datafile(bundled.name) == bundled


def test_missing_datafile_lists_searched_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MSPASS_HOME", raising=False)

    with pytest.raises(FileNotFoundError, match="missing.yaml") as error:
        stock.datafile("missing.yaml")

    assert str(Path(tmp_path) / "data" / "yaml") in str(error.value)
