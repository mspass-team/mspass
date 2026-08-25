import os
import subprocess
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
START_MSPASS_GEOLAB = REPOSITORY_ROOT / "scripts" / "start-mspass-geolab.sh"


def _write_fake_commands(tmp_path, fake_rm):
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_mongosh = fake_bin / "mongosh"
    fake_mongosh.write_text("#!/bin/sh\nexit 0\n")
    fake_mongosh.chmod(0o755)
    if fake_rm:
        rm_command = fake_bin / "rm"
        rm_command.write_text(
            '#!/bin/sh\nprintf \'%s\\n\' "$@" >> "$RM_LOG"\nexit 99\n'
        )
        rm_command.chmod(0o755)
    return fake_bin


def _snapshot(root):
    snapshot = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if path.is_symlink():
            snapshot.append((str(relative), "symlink", os.readlink(path)))
        elif path.is_dir():
            snapshot.append((str(relative), "directory", None))
        else:
            snapshot.append((str(relative), "file", path.read_bytes()))
    return snapshot


def _base_environment(tmp_path, db_root, data_target, fake_rm=False):
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    log_dir = tmp_path / "logs"
    worker_dir = tmp_path / "workers"
    environment = os.environ.copy()
    environment.update(
        {
            "MONGO_DATA_DIR": data_target,
            "MSPASS_DB_DIR": str(db_root),
            "MSPASS_ENABLE_LOCAL_DASK": "false",
            "MSPASS_LOG_DIR": str(log_dir),
            "MSPASS_RESET_MONGO_DB": "true",
            "MSPASS_SKIP_LOCAL_MONGO": "true",
            "MSPASS_WORKDIR": str(workspace),
            "MSPASS_WORKER_DIR": str(worker_dir),
        }
    )
    fake_bin = _write_fake_commands(tmp_path, fake_rm)
    environment["PATH"] = f"{fake_bin}:/usr/bin:/bin"
    if fake_rm:
        environment["RM_LOG"] = str(tmp_path / "rm.log")
    return environment


def _unsafe_target(tmp_path, case):
    db_root = tmp_path / "db"
    db_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    workspace = tmp_path / "workspace"

    if case == "empty":
        return db_root, "", "MONGO_DATA_DIR is empty."
    if case == "relative":
        return db_root, "db/data", "MONGO_DATA_DIR must be an absolute path"
    if case == "root":
        return Path("/"), "/", "unsafe target: /"
    if case == "home":
        return Path("/home"), "/home/jovyan", "unsafe target: /home/jovyan"
    if case == "workspace":
        return tmp_path, str(workspace), f"unsafe target: {workspace}"
    if case == "db_root":
        return db_root, str(db_root), f"unsafe target: {db_root}"
    if case == "file":
        file_target = db_root / "not-a-directory"
        file_target.write_text("keep me")
        return db_root, str(file_target), "target is not a directory"
    if case == "traversal":
        return (
            db_root,
            f"{db_root}/data/../../outside",
            "target escapes MongoDB root",
        )
    if case == "symlink_escape":
        (db_root / "escape").symlink_to(outside, target_is_directory=True)
        return (
            db_root,
            str(db_root / "escape" / "mongo"),
            "target escapes MongoDB root",
        )
    raise AssertionError(f"unknown test case: {case}")


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "relative",
        "root",
        "home",
        "workspace",
        "db_root",
        "file",
        "traversal",
        "symlink_escape",
    ],
)
def test_geolab_reset_rejects_unsafe_target_before_any_delete(tmp_path, case):
    db_root, data_target, expected_diagnostic = _unsafe_target(tmp_path, case)
    environment = _base_environment(tmp_path, db_root, data_target, fake_rm=True)
    before = _snapshot(tmp_path)

    result = subprocess.run(
        ["sh", str(START_MSPASS_GEOLAB), "true"],
        cwd=tmp_path,
        capture_output=True,
        env=environment,
        text=True,
        timeout=10,
    )

    assert result.returncode != 0
    assert "refusing to reset MongoDB data directory" in result.stderr
    assert expected_diagnostic in result.stderr
    assert not (tmp_path / "rm.log").exists()
    assert _snapshot(tmp_path) == before


def test_geolab_reset_deletes_only_canonical_nested_data_directory(tmp_path):
    db_root = tmp_path / "db"
    data_target = db_root / "mongo" / "data"
    data_target.mkdir(parents=True)
    (data_target / "old-database-file").write_text("delete me")
    sibling = db_root / "keep" / "sibling"
    sibling.mkdir(parents=True)
    (sibling / "keep").write_text("keep me")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("keep me too")
    environment = _base_environment(tmp_path, db_root, str(data_target), fake_rm=False)

    result = subprocess.run(
        ["sh", str(START_MSPASS_GEOLAB), "true"],
        cwd=tmp_path,
        capture_output=True,
        env=environment,
        text=True,
        timeout=10,
    )

    assert "refusing to reset MongoDB data directory" not in result.stderr
    assert f"Resetting MongoDB data directory: {data_target.resolve()}" in result.stdout
    assert data_target.is_dir()
    assert not (data_target / "old-database-file").exists()
    assert (sibling / "keep").read_text() == "keep me"
    assert (outside / "keep").read_text() == "keep me too"


def test_empty_data_target_without_reset_keeps_the_default_directory(tmp_path):
    db_root = tmp_path / "db"
    environment = _base_environment(tmp_path, db_root, "", fake_rm=False)
    environment["MSPASS_RESET_MONGO_DB"] = "false"

    result = subprocess.run(
        ["sh", str(START_MSPASS_GEOLAB), "true"],
        cwd=tmp_path,
        capture_output=True,
        env=environment,
        text=True,
        timeout=10,
    )

    assert "refusing to reset MongoDB data directory" not in result.stderr
    assert (db_root / "data").is_dir()
    assert "Resetting MongoDB data directory" not in result.stdout
