import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DASK_COMPOSE_FILES = (
    REPOSITORY_ROOT / "data" / "yaml" / "compose.yaml",
    REPOSITORY_ROOT / "data" / "yaml" / "docker-compose_sharding.yaml",
    REPOSITORY_ROOT
    / "scripts"
    / "IU_examples"
    / "python"
    / "configuration_docker.yaml",
)
PROBE_CODE = (
    "import distributed; "
    "client = distributed.Client('tcp://127.0.0.1:8786', timeout='2s'); "
    "client.scheduler_info(); client.close()"
)
PROBE_COMMAND = ["CMD", "python", "-c", PROBE_CODE]


def _load_compose(path):
    return yaml.safe_load(path.read_text())


def _resolve_compose(path):
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker Compose is required to inspect the resolved configuration")

    compose_version = subprocess.run(
        [docker, "compose", "version"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if compose_version.returncode != 0:
        pytest.skip("Docker Compose is not available in this environment")

    result = subprocess.run(
        [docker, "compose", "-f", str(path), "config", "--format", "json"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _assert_scheduler_contract(compose):
    services = compose["services"]
    assert services["mspass-scheduler"]["healthcheck"]["test"] == PROBE_COMMAND
    assert (
        services["mspass-worker"]["depends_on"]["mspass-scheduler"]["condition"]
        == "service_healthy"
    )
    assert (
        services["mspass-frontend"]["depends_on"]["mspass-scheduler"]["condition"]
        == "service_healthy"
    )


def test_all_dask_compose_topologies_are_covered():
    discovered = set()
    tracked = subprocess.run(
        ["git", "ls-files", "--", "*.yaml", "*.yml"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    paths = (REPOSITORY_ROOT / relative for relative in tracked.stdout.splitlines())
    for path in paths:
        source = path.read_text()
        if "MSPASS_SCHEDULER: dask" not in source:
            continue
        compose = yaml.safe_load(source)
        scheduler = compose.get("services", {}).get("mspass-scheduler", {})
        if scheduler.get("environment", {}).get("MSPASS_SCHEDULER") == "dask":
            discovered.add(path)

    assert discovered == set(DASK_COMPOSE_FILES)


@pytest.mark.parametrize("compose_path", DASK_COMPOSE_FILES)
def test_dask_compose_source_uses_protocol_probe_and_health_gating(compose_path):
    _assert_scheduler_contract(_load_compose(compose_path))


@pytest.mark.parametrize("compose_path", DASK_COMPOSE_FILES)
def test_resolved_dask_compose_uses_protocol_probe_and_health_gating(compose_path):
    _assert_scheduler_contract(_resolve_compose(compose_path))


def _write_fake_distributed_module(tmp_path):
    (tmp_path / "distributed.py").write_text("""import os
from pathlib import Path


def _record(event):
    path = Path(os.environ["PROBE_LOG"])
    with path.open("a") as stream:
        stream.write(event + "\\n")


class Client:
    def __init__(self, address, timeout):
        _record(f"connect {address} {timeout}")
        if os.environ["PROBE_FAILURE"] == "connect":
            raise RuntimeError("connection failed")

    def scheduler_info(self):
        _record("scheduler_info")
        if os.environ["PROBE_FAILURE"] == "scheduler_info":
            raise RuntimeError("scheduler info failed")

    def close(self):
        _record("close")
        if os.environ["PROBE_FAILURE"] == "close":
            raise RuntimeError("close failed")
""")


@pytest.mark.parametrize(
    "failure, expected_returncode, expected_events",
    [
        ("", 0, ["connect tcp://127.0.0.1:8786 2s", "scheduler_info", "close"]),
        ("connect", 1, ["connect tcp://127.0.0.1:8786 2s"]),
        (
            "scheduler_info",
            1,
            ["connect tcp://127.0.0.1:8786 2s", "scheduler_info"],
        ),
        (
            "close",
            1,
            ["connect tcp://127.0.0.1:8786 2s", "scheduler_info", "close"],
        ),
    ],
)
def test_dask_health_probe_is_single_attempt_and_propagates_failures(
    tmp_path, failure, expected_returncode, expected_events
):
    compose = _load_compose(DASK_COMPOSE_FILES[0])
    command = compose["services"]["mspass-scheduler"]["healthcheck"]["test"]
    assert command == PROBE_COMMAND
    _write_fake_distributed_module(tmp_path)
    log = tmp_path / "probe.log"
    env = os.environ.copy()
    env.update(
        {
            "PROBE_FAILURE": failure,
            "PROBE_LOG": str(log),
            "PYTHONPATH": str(tmp_path),
        }
    )

    result = subprocess.run(
        [sys.executable, *command[2:]],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == expected_returncode
    assert log.read_text().splitlines() == expected_events


def test_live_compose_workflow_is_path_filtered_and_enables_integration_tests():
    workflow = yaml.safe_load(
        (REPOSITORY_ROOT / ".github" / "workflows" / "compose-health.yml").read_text()
    )
    expected_paths = [
        ".github/workflows/compose-health.yml",
        "data/yaml/compose.yaml",
        "data/yaml/docker-compose_sharding.yaml",
        "scripts/IU_examples/python/configuration_docker.yaml",
        "python/tests/test_dask_compose_healthcheck.py",
    ]
    assert workflow["on"]["pull_request"]["paths"] == expected_paths
    assert workflow["on"]["push"] == {
        "branches": ["master"],
        "paths": expected_paths,
    }
    job = workflow["jobs"]["compose-health"]
    assert job["env"]["MSPASS_RUN_COMPOSE_HEALTHCHECK_TESTS"] == "1"
    assert job["timeout-minutes"] == 60
    assert job["steps"][-1]["run"] == (
        "python -m pytest -q python/tests/test_dask_compose_healthcheck.py"
    )


RUN_COMPOSE_TESTS = os.environ.get("MSPASS_RUN_COMPOSE_HEALTHCHECK_TESTS") == "1"


def _run_compose(base_command, *arguments, timeout=180, check=True):
    return subprocess.run(
        [*base_command, *arguments],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
    )


def _wait_for_scheduler_health(base_command, expected, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        container_id = _run_compose(
            base_command, "ps", "-q", "mspass-scheduler"
        ).stdout.strip()
        if container_id:
            result = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{if .State.Health}}{{.State.Health.Status}}{{end}}",
                    container_id,
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=True,
            )
            if result.stdout.strip() == expected:
                return
        time.sleep(1)
    raise AssertionError(f"scheduler did not become {expected} within {timeout}s")


@pytest.mark.skipif(
    not RUN_COMPOSE_TESTS,
    reason="set MSPASS_RUN_COMPOSE_HEALTHCHECK_TESTS=1 for Docker integration tests",
)
@pytest.mark.parametrize("compose_path", DASK_COMPOSE_FILES)
def test_running_dask_compose_health_and_dependency_gating(compose_path):
    project = f"mspass-health-{compose_path.stem.replace('_', '-')}"
    base_command = [
        "docker",
        "compose",
        "--project-name",
        project,
        "-f",
        str(compose_path),
    ]
    try:
        _run_compose(base_command, "up", "-d", "--wait", "--wait-timeout", "240")
        _wait_for_scheduler_health(base_command, "healthy", 30)
        task = _run_compose(
            base_command,
            "exec",
            "-T",
            "mspass-scheduler",
            "python",
            "-c",
            (
                "from distributed import Client; "
                "client=Client('tcp://127.0.0.1:8786', timeout='2s'); "
                "client.wait_for_workers(1, timeout=60); "
                "assert client.submit(lambda value: value + 1, 41).result(timeout=30) "
                "== 42; client.close()"
            ),
            timeout=120,
        )
        assert task.returncode == 0

        _run_compose(
            base_command,
            "exec",
            "-T",
            "mspass-scheduler",
            "bash",
            "-c",
            "pkill -TERM -f '[d]ask scheduler'",
        )
        _wait_for_scheduler_health(base_command, "unhealthy", 90)
        _run_compose(
            base_command,
            "rm",
            "-s",
            "-f",
            "mspass-worker",
            "mspass-frontend",
        )

        restart = _run_compose(
            base_command,
            "up",
            "-d",
            "mspass-worker",
            "mspass-frontend",
            timeout=30,
            check=False,
        )
        assert restart.returncode != 0
        assert "unhealthy" in (restart.stdout + restart.stderr).lower()
        for service in ("mspass-worker", "mspass-frontend"):
            assert not _run_compose(base_command, "ps", "-q", service).stdout.strip()
    finally:
        _run_compose(
            base_command,
            "down",
            "--volumes",
            "--remove-orphans",
            check=False,
        )
