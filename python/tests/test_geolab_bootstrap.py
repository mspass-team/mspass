import os
from pathlib import Path
import signal
import subprocess
import sys
import textwrap
import time

import pytest

REPOSITORY_ROOT = Path(
    os.environ.get(
        "MSPASS_TEST_REPOSITORY_ROOT",
        str(Path(__file__).resolve().parents[2]),
    )
)
START_SCRIPT = REPOSITORY_ROOT / "scripts" / "start-mspass-geolab.sh"
ENTRYPOINT_SCRIPT = REPOSITORY_ROOT / "scripts" / "start-mspass-geolab-entrypoint.sh"
DOCKERFILE = REPOSITORY_ROOT / "Dockerfile"


def _write_executable(path, source):
    path.write_text(textwrap.dedent(source).lstrip())
    path.chmod(0o755)


def _make_bootstrap_environment(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    event_log = tmp_path / "events.log"
    mongo_pid = tmp_path / "mongo-pid"
    mongo_count = tmp_path / "mongo-count"
    dask_count = tmp_path / "dask-count"
    date_count = tmp_path / "date-count"

    _write_executable(
        fake_bin / "mongod",
        """
        #!/bin/sh
        printf 'mongo-start|%s|%s\n' "$$" "$*" >> "$EVENT_LOG"
        printf '%s\n' "$$" > "$MONGO_PID_FILE"
        printf 'fake mongo log\n' > "$MONGO_LOG"
        if [ "${MONGO_CHILD_EXIT_EARLY:-false}" = "true" ]; then
            printf 'mongo-exit|%s\n' "$$" >> "$EVENT_LOG"
            exit 41
        fi
        trap 'printf "mongo-term|%s\n" "$$" >> "$EVENT_LOG"; exit 0' TERM INT
        while :; do
            /bin/sleep 0.05
        done
        """,
    )
    _write_executable(
        fake_bin / "mongosh",
        """
        #!/bin/sh
        case "$*" in
            *shutdownServer*)
                kill "$(cat "$MONGO_PID_FILE")" 2>/dev/null || true
                exit 0
                ;;
        esac
        count=0
        if [ -f "$MONGO_COUNT_FILE" ]; then
            count=$(cat "$MONGO_COUNT_FILE")
        fi
        count=$((count + 1))
        printf '%s\n' "$count" > "$MONGO_COUNT_FILE"
        printf 'mongo-ping|%s|%s\n' "$count" "$*" >> "$EVENT_LOG"
        [ "$count" -ge "${MONGO_READY_AFTER:-1}" ]
        """,
    )
    _write_executable(
        fake_bin / "dask",
        """
        #!/bin/sh
        mode=$1
        shift
        case "$mode" in
            scheduler)
                printf 'dask-scheduler-start|%s|%s\n' "$$" "$*" >> "$EVENT_LOG"
                if [ "${DASK_SCHEDULER_EXIT_EARLY:-false}" = "true" ]; then
                    printf 'dask-scheduler-exit|%s\n' "$$" >> "$EVENT_LOG"
                    exit 42
                fi
                trap 'printf "dask-scheduler-term|%s\n" "$$" >> "$EVENT_LOG"; exit 0' TERM INT
                ;;
            worker)
                printf 'dask-worker-start|%s|%s\n' "$$" "$*" >> "$EVENT_LOG"
                trap 'printf "dask-worker-term|%s\n" "$$" >> "$EVENT_LOG"; exit 0' TERM INT
                ;;
            *)
                exit 2
                ;;
        esac
        while :; do
            /bin/sleep 0.05
        done
        """,
    )
    _write_executable(
        fake_bin / "python",
        """
        #!/bin/sh
        exec "$TEST_PYTHON" "$@"
        """,
    )
    _write_executable(
        fake_bin / "date",
        """
        #!/bin/sh
        value=0
        if [ -f "$DATE_COUNT_FILE" ]; then
            value=$(cat "$DATE_COUNT_FILE")
        fi
        printf '%s\n' "$value"
        printf '%s\n' "$((value + ${FAKE_DATE_STEP:-1}))" > "$DATE_COUNT_FILE"
        """,
    )
    _write_executable(
        fake_bin / "sleep",
        """
        #!/bin/sh
        printf 'sleep|%s\n' "$1" >> "$EVENT_LOG"
        /bin/sleep "${TEST_SLEEP_DELAY_SECONDS:-0.02}"
        """,
    )
    _write_executable(
        fake_bin / "frontend",
        """
        #!/bin/sh
        printf 'frontend-start|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
            "$$" "$NB_HOME" "$HOME" "$MSPASS_WORKDIR" \
            "$MSPASS_WORK_DIR" "$(pwd)" "$MSPASS_STARTUP_TIMEOUT_SECONDS" \
            "$MSPASS_STARTUP_POLL_SECONDS" "$*" >> "$EVENT_LOG"
        if [ "${FRONTEND_BLOCK:-false}" = "true" ]; then
            trap 'printf "frontend-term|%s\n" "$$" >> "$EVENT_LOG"; exit 0' TERM INT
            while :; do
                /bin/sleep 0.05
            done
        fi
        /bin/sleep "${FRONTEND_DELAY_SECONDS:-0.1}"
        exit "${FRONTEND_STATUS:-0}"
        """,
    )

    support = tmp_path / "support"
    support.mkdir()
    distributed_module = """
        import os
        from pathlib import Path


        def _append(message):
            with Path(os.environ["EVENT_LOG"]).open("a") as stream:
                stream.write(message + "\\n")


        class Client:
            def __init__(self, address, timeout):
                self.address = address
                _append(f"client-init|{address}|{timeout}")

            def scheduler_info(self):
                counter = Path(os.environ["DASK_COUNT_FILE"])
                count = int(counter.read_text()) if counter.exists() else 0
                count += 1
                counter.write_text(str(count))
                _append(f"client-info|{count}")
                if count < int(os.environ.get("DASK_READY_AFTER", "1")):
                    raise RuntimeError("scheduler is not ready")
                return {"address": self.address}

            def close(self):
                _append("client-close")
    """
    (support / "distributed.py").write_text(
        textwrap.dedent(distributed_module).lstrip()
    )

    nb_home = tmp_path / "configured-nb-home"
    home = tmp_path / "configured-home"
    work_dir = tmp_path / "configured-work-dir"
    workdir = tmp_path / "configured-workdir"
    for directory in (nb_home, home, work_dir, workdir):
        directory.mkdir()

    environment = os.environ.copy()
    for name in (
        "MSPASS_RESET_MONGO_DB",
        "MSPASS_SKIP_LOCAL_MONGO",
        "MSPASS_STARTUP_TIMEOUT_SECONDS",
        "MSPASS_STARTUP_POLL_SECONDS",
        "MONGO_CHILD_EXIT_EARLY",
        "DASK_SCHEDULER_EXIT_EARLY",
        "FRONTEND_BLOCK",
    ):
        environment.pop(name, None)
    environment.update(
        PATH=f"{fake_bin}:/usr/bin:/bin",
        PYTHONPATH=str(support),
        PYTHONDONTWRITEBYTECODE="1",
        TEST_PYTHON=sys.executable,
        EVENT_LOG=str(event_log),
        MONGO_PID_FILE=str(mongo_pid),
        MONGO_COUNT_FILE=str(mongo_count),
        DASK_COUNT_FILE=str(dask_count),
        DATE_COUNT_FILE=str(date_count),
        MONGO_READY_AFTER="1",
        DASK_READY_AFTER="1",
        NB_HOME=str(nb_home),
        HOME=str(home),
        MSPASS_WORK_DIR=str(work_dir),
        MSPASS_WORKDIR=str(workdir),
        MSPASS_DB_DIR=str(tmp_path / "db"),
        MONGO_DATA_DIR=str(tmp_path / "db" / "data"),
        MSPASS_LOG_DIR=str(tmp_path / "logs"),
        MONGO_LOG=str(tmp_path / "logs" / "mongo.log"),
        MSPASS_WORKER_DIR=str(tmp_path / "workers"),
        MSPASS_DB_ADDRESS="mongo.configured",
        MONGODB_PORT="29017",
        MSPASS_ENABLE_LOCAL_DASK="true",
        MSPASS_SCHEDULER="dask",
        MSPASS_SCHEDULER_ADDRESS="dask.configured",
        DASK_SCHEDULER_PORT="9876",
        MSPASS_DASK_WORKER_COUNT="2",
        MSPASS_DASK_WORKER_THREADS="1",
        MSPASS_DASK_WORKER_MEMORY_LIMIT="0",
        TEST_SLEEP_DELAY_SECONDS="0.02",
    )
    return environment


def _run_bootstrap(environment, *frontend_arguments):
    fake_frontend = Path(environment["PATH"].split(os.pathsep)[0]) / "frontend"
    return subprocess.run(
        ["/bin/sh", str(START_SCRIPT), str(fake_frontend), *frontend_arguments],
        env=environment,
        text=True,
        capture_output=True,
        timeout=10,
    )


def _events(environment):
    event_log = Path(environment["EVENT_LOG"])
    return event_log.read_text().splitlines() if event_log.exists() else []


def _started_pids(events):
    prefixes = (
        "mongo-start|",
        "dask-scheduler-start|",
        "dask-worker-start|",
        "frontend-start|",
    )
    return [
        int(event.split("|", 2)[1]) for event in events if event.startswith(prefixes)
    ]


def _assert_children_reaped(events):
    for pid in _started_pids(events):
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def _event_index(events, prefix):
    return next(index for index, event in enumerate(events) if event.startswith(prefix))


def test_geolab_image_default_uses_the_dispatching_entrypoint():
    geolab_stage = DOCKERFILE.read_text().split(
        "FROM ${GEOLAB_BASE_IMAGE} AS geolab", 1
    )[1]
    geolab_stage = geolab_stage.split("\nFROM dev-package AS dev", 1)[0]
    assert 'ENTRYPOINT ["/usr/sbin/start-mspass-geolab-entrypoint.sh"]' in geolab_stage
    assert 'CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser"]' in geolab_stage


@pytest.mark.parametrize(
    "command_name,arguments,expected_route",
    (
        ("jupyter", ("lab", "--no-browser"), "bootstrap"),
        ("jupyterhub-singleuser", ("--port=8888",), "bootstrap"),
        ("dask-scheduler", ("--port", "8786"), "direct"),
        ("dask-worker", ("tcp://scheduler:8786",), "direct"),
        ("dask", ("scheduler", "--port", "8786"), "direct"),
        ("dask-gateway-server", ("jupyter", "lab"), "direct"),
        ("dask-gateway", ("jupyterhub-singleuser",), "direct"),
    ),
)
def test_entrypoint_routes_only_jupyter_frontends(
    tmp_path, command_name, arguments, expected_route
):
    event_log = tmp_path / "entrypoint-events"
    bootstrap = tmp_path / "bootstrap"
    _write_executable(
        bootstrap,
        """
        #!/bin/sh
        printf 'bootstrap|%s\n' "$*" >> "$ENTRYPOINT_EVENT_LOG"
        exit 17
        """,
    )
    command = tmp_path / command_name
    _write_executable(
        command,
        """
        #!/bin/sh
        printf 'direct|%s|%s\n' "${0##*/}" "$*" >> "$ENTRYPOINT_EVENT_LOG"
        exit 23
        """,
    )
    entrypoint = tmp_path / "entrypoint"
    source = ENTRYPOINT_SCRIPT.read_text()
    source = source.replace("/usr/sbin/start-mspass-geolab.sh", str(bootstrap))
    _write_executable(entrypoint, source)
    environment = os.environ.copy()
    environment["ENTRYPOINT_EVENT_LOG"] = str(event_log)

    result = subprocess.run(
        [str(entrypoint), str(command), *arguments],
        env=environment,
        text=True,
        capture_output=True,
    )

    events = event_log.read_text().splitlines()
    if expected_route == "bootstrap":
        assert result.returncode == 17
        assert len(events) == 1
        assert events[0].startswith(f"bootstrap|{command}")
    else:
        assert result.returncode == 23
        assert events == [f"direct|{command_name}|{' '.join(arguments)}"]


def test_bootstrap_preserves_paths_and_checks_configured_endpoints(tmp_path):
    environment = _make_bootstrap_environment(tmp_path)
    environment.update(MONGO_READY_AFTER="2", DASK_READY_AFTER="2")

    result = _run_bootstrap(environment, "--frontend-argument")

    assert result.returncode == 0, result.stderr
    events = _events(environment)
    frontend = next(event for event in events if event.startswith("frontend-start|"))
    assert (
        f"|{environment['NB_HOME']}|{environment['HOME']}|"
        f"{environment['MSPASS_WORKDIR']}|{environment['MSPASS_WORK_DIR']}|"
        f"{environment['MSPASS_WORKDIR']}|120|2|--frontend-argument"
    ) in frontend
    mongo_ping = next(event for event in events if event.startswith("mongo-ping|"))
    assert "--host mongo.configured --port 29017" in mongo_ping
    mongo_start = next(event for event in events if event.startswith("mongo-start|"))
    assert f"--dbpath {environment['MONGO_DATA_DIR']}" in mongo_start
    assert f"--logpath {environment['MONGO_LOG']}" in mongo_start
    assert events.count("client-init|tcp://dask.configured:9876|2s") == 2
    assert len([event for event in events if event.startswith("client-info|")]) == 2
    assert events.count("client-close") == 2
    assert "sleep|2" in events
    workers = [event for event in events if event.startswith("dask-worker-start|")]
    assert len(workers) == 2
    assert all("tcp://dask.configured:9876" in event for event in workers)
    assert all(
        f"--local-directory {environment['MSPASS_WORKER_DIR']}/worker-{index}" in worker
        for index, worker in enumerate(workers, start=1)
    )
    assert (Path(environment["MSPASS_LOG_DIR"]) / "dask-scheduler.log").is_file()
    assert (Path(environment["MSPASS_LOG_DIR"]) / "dask-worker-1.log").is_file()
    assert (Path(environment["MSPASS_LOG_DIR"]) / "dask-worker-2.log").is_file()
    readiness_index = max(
        index
        for index, event in enumerate(events)
        if event.startswith(("mongo-ping|", "client-close"))
    )
    assert all(events.index(worker) > readiness_index for worker in workers)
    assert _event_index(events, "frontend-start|") > readiness_index
    _assert_children_reaped(events)


def test_default_startup_timeout_is_120_seconds(tmp_path):
    environment = _make_bootstrap_environment(tmp_path)
    environment.update(
        MONGO_READY_AFTER="999",
        DASK_READY_AFTER="999",
        FAKE_DATE_STEP="120",
    )

    result = _run_bootstrap(environment)

    assert result.returncode != 0
    assert "did not become ready within 120 seconds" in result.stderr
    events = _events(environment)
    assert not any(event.startswith("dask-worker-start|") for event in events)
    assert not any(event.startswith("frontend-start|") for event in events)
    _assert_children_reaped(events)


@pytest.mark.parametrize(
    "variable,value,diagnostic",
    (
        (
            "MSPASS_STARTUP_TIMEOUT_SECONDS",
            "0",
            "must be a positive integer",
        ),
        (
            "MSPASS_STARTUP_TIMEOUT_SECONDS",
            "1.5",
            "must be a positive integer",
        ),
        (
            "MSPASS_STARTUP_TIMEOUT_SECONDS",
            "invalid",
            "must be a positive integer",
        ),
        (
            "MSPASS_STARTUP_POLL_SECONDS",
            "0",
            "must be a positive number",
        ),
        (
            "MSPASS_STARTUP_POLL_SECONDS",
            "-0.5",
            "must be a positive number",
        ),
        (
            "MSPASS_STARTUP_POLL_SECONDS",
            "invalid",
            "must be a positive number",
        ),
        (
            "MSPASS_ENABLE_LOCAL_DASK",
            "sometimes",
            "must be a boolean value",
        ),
    ),
)
def test_invalid_startup_configuration_fails_before_starting_children(
    tmp_path, variable, value, diagnostic
):
    environment = _make_bootstrap_environment(tmp_path)
    environment[variable] = value

    result = _run_bootstrap(environment)

    assert result.returncode == 2
    assert diagnostic in result.stderr
    assert _events(environment) == []


def test_readiness_returning_after_deadline_starts_nothing_later(tmp_path):
    environment = _make_bootstrap_environment(tmp_path)
    environment.update(
        MSPASS_STARTUP_TIMEOUT_SECONDS="2",
        FAKE_DATE_STEP="2",
    )

    result = _run_bootstrap(environment)

    assert result.returncode != 0
    assert "did not become ready within 2 seconds" in result.stderr
    events = _events(environment)
    assert any(event.startswith("mongo-ping|") for event in events)
    assert any(event.startswith("client-info|") for event in events)
    assert not any(event.startswith("dask-worker-start|") for event in events)
    assert not any(event.startswith("frontend-start|") for event in events)
    _assert_children_reaped(events)


def test_bootstrap_polls_until_both_services_are_ready_and_closes_each_client(
    tmp_path,
):
    environment = _make_bootstrap_environment(tmp_path)
    environment.update(
        MONGO_READY_AFTER="3",
        DASK_READY_AFTER="2",
        MSPASS_STARTUP_TIMEOUT_SECONDS="9",
        MSPASS_STARTUP_POLL_SECONDS="0.125",
    )

    result = _run_bootstrap(environment)

    assert result.returncode == 0, result.stderr
    events = _events(environment)
    assert [event for event in events if event.startswith("sleep|")] == [
        "sleep|0.125",
        "sleep|0.125",
    ]
    assert len([event for event in events if event.startswith("mongo-ping|")]) == 3
    assert len([event for event in events if event.startswith("client-init|")]) == 3
    assert len([event for event in events if event.startswith("client-info|")]) == 3
    assert events.count("client-close") == 3
    frontend = next(event for event in events if event.startswith("frontend-start|"))
    assert "|9|0.125|" in frontend
    readiness_index = max(
        index
        for index, event in enumerate(events)
        if event.startswith(("mongo-ping|", "client-close"))
    )
    assert all(
        index > readiness_index
        for index, event in enumerate(events)
        if event.startswith(("dask-worker-start|", "frontend-start|"))
    )
    _assert_children_reaped(events)


@pytest.mark.parametrize(
    "failure_variable,expected_error",
    (
        ("MONGO_CHILD_EXIT_EARLY", "mongod exited during startup"),
        ("DASK_SCHEDULER_EXIT_EARLY", "Dask scheduler exited during startup"),
    ),
)
def test_owned_child_early_exit_starts_no_worker_or_frontend(
    tmp_path, failure_variable, expected_error
):
    environment = _make_bootstrap_environment(tmp_path)
    environment.update(
        MONGO_READY_AFTER="999",
        DASK_READY_AFTER="999",
        MSPASS_STARTUP_TIMEOUT_SECONDS="20",
        MSPASS_STARTUP_POLL_SECONDS="0.01",
        **{failure_variable: "true"},
    )

    result = _run_bootstrap(environment)

    assert result.returncode != 0
    assert expected_error in result.stderr
    events = _events(environment)
    assert not any(event.startswith("dask-worker-start|") for event in events)
    assert not any(event.startswith("frontend-start|") for event in events)
    _assert_children_reaped(events)


def test_startup_timeout_cleans_owned_children_and_starts_nothing_later(tmp_path):
    environment = _make_bootstrap_environment(tmp_path)
    environment.update(
        MONGO_READY_AFTER="999",
        DASK_READY_AFTER="999",
        MSPASS_STARTUP_TIMEOUT_SECONDS="2",
        MSPASS_STARTUP_POLL_SECONDS="0.01",
    )

    result = _run_bootstrap(environment)

    assert result.returncode != 0
    assert "did not become ready within 2 seconds" in result.stderr
    events = _events(environment)
    assert not any(event.startswith("dask-worker-start|") for event in events)
    assert not any(event.startswith("frontend-start|") for event in events)
    assert any(event.startswith("mongo-term|") for event in events)
    assert any(event.startswith("dask-scheduler-term|") for event in events)
    _assert_children_reaped(events)


def test_scheduler_none_requires_only_mongo_before_frontend(tmp_path):
    environment = _make_bootstrap_environment(tmp_path)
    environment.update(
        MSPASS_ENABLE_LOCAL_DASK="false",
        MSPASS_SCHEDULER="none",
    )

    result = _run_bootstrap(environment)

    assert result.returncode == 0, result.stderr
    events = _events(environment)
    assert any(event.startswith("mongo-ping|") for event in events)
    assert not any(event.startswith("client-init|") for event in events)
    assert not any(event.startswith("dask-scheduler-start|") for event in events)
    assert not any(event.startswith("dask-worker-start|") for event in events)
    assert any(event.startswith("frontend-start|") for event in events)
    _assert_children_reaped(events)


def test_signal_terminates_and_reaps_every_owned_child(tmp_path):
    environment = _make_bootstrap_environment(tmp_path)
    environment["FRONTEND_BLOCK"] = "true"
    fake_frontend = Path(environment["PATH"].split(os.pathsep)[0]) / "frontend"
    process = subprocess.Popen(
        ["/bin/sh", str(START_SCRIPT), str(fake_frontend)],
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            events = _events(environment)
            if (
                any(event.startswith("frontend-start|") for event in events)
                and len(
                    [
                        event
                        for event in events
                        if event.startswith("dask-worker-start|")
                    ]
                )
                == 2
            ):
                break
            assert process.poll() is None
            time.sleep(0.02)
        else:
            pytest.fail("GeoLab frontend and workers did not start")

        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=5)

    assert process.returncode == 143, (stdout, stderr)
    events = _events(environment)
    assert any(event.startswith("frontend-term|") for event in events)
    assert (
        len([event for event in events if event.startswith("dask-worker-term|")]) == 2
    )
    assert any(event.startswith("dask-scheduler-term|") for event in events)
    assert any(event.startswith("mongo-term|") for event in events)
    _assert_children_reaped(events)
