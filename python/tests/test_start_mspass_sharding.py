import os
import subprocess
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
START_MSPASS = REPOSITORY_ROOT / "scripts" / "start-mspass.sh"
SHARDING_COMPOSE = REPOSITORY_ROOT / "data" / "yaml" / "docker-compose_sharding.yaml"


def _write_executable(path, content):
    path.write_text(content)
    path.chmod(0o755)


def _run_dbmanager(tmp_path, successful_add_shard_attempt):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    command_log = tmp_path / "mongosh.log"

    for command in ("mongod", "mongos", "tail"):
        _write_executable(fake_bin / command, "#!/bin/bash\nexit 0\n")

    _write_executable(
        fake_bin / "mongosh",
        """#!/bin/bash
printf '%s\n' "$*" >> "$MONGOSH_LOG"
if [[ "$*" == *'sh.addShard'* ]]; then
  attempts=$(grep -c 'sh.addShard' "$MONGOSH_LOG")
  if ((attempts < SUCCESSFUL_ADD_SHARD_ATTEMPT)); then
    exit 1
  fi
fi
exit 0
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "MONGODB_PORT": "27017",
            "MONGOSH_LOG": str(command_log),
            "MSPASS_DB_DIR": str(tmp_path / "db"),
            "MSPASS_LOG_DIR": str(tmp_path / "logs"),
            "MSPASS_ROLE": "dbmanager",
            "MSPASS_SHARD_COLLECTIONS": "wf_miniseed:_id",
            "MSPASS_SHARD_DATABASE": "usarray2012",
            "MSPASS_SHARD_LIST": "rs0/shard0:27017",
            "MSPASS_SLEEP_TIME": "0",
            "MSPASS_WORK_DIR": str(tmp_path / "work"),
            "SUCCESSFUL_ADD_SHARD_ATTEMPT": str(successful_add_shard_attempt),
        }
    )
    result = subprocess.run(
        ["bash", str(START_MSPASS)],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )
    commands = command_log.read_text().splitlines()
    return result, commands


def test_dbmanager_waits_for_shard_before_enabling_sharding(tmp_path):
    result, commands = _run_dbmanager(tmp_path, successful_add_shard_attempt=3)

    assert result.returncode == 0, result.stderr
    add_shard_commands = [line for line in commands if "sh.addShard" in line]
    assert len(add_shard_commands) == 3
    assert next(
        i for i, line in enumerate(commands) if "sh.enableSharding" in line
    ) > max(i for i, line in enumerate(commands) if "sh.addShard" in line)


def test_dbmanager_stops_when_no_shard_becomes_ready(tmp_path):
    result, commands = _run_dbmanager(tmp_path, successful_add_shard_attempt=21)

    assert result.returncode != 0
    assert sum("sh.addShard" in line for line in commands) == 20
    assert all("sh.enableSharding" not in line for line in commands)
    assert "failed after 20 attempts" in result.stderr


def test_sharding_compose_uses_the_initialized_replica_set_names():
    compose = yaml.safe_load(SHARDING_COMPOSE.read_text())
    shard_list = compose["services"]["mspass-dbmanager"]["environment"][
        "MSPASS_SHARD_LIST"
    ]

    assert shard_list.split() == [
        "rs0/mspass-shard-0:27017",
        "rs1/mspass-shard-1:27017",
    ]
