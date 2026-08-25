import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
LAUNCHER_PATH = REPOSITORY_ROOT / "scripts" / "IU_examples" / "python" / "launcher.py"


@pytest.fixture
def launcher_module():
    spec = importlib.util.spec_from_file_location(
        "mspass_iu_launcher_test", LAUNCHER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    assert Path(module.__file__).resolve() == LAUNCHER_PATH.resolve()
    return module


def _configuration(**cluster_updates):
    cluster = {
        "primary_host": "primary",
        "database_host": "database:27017",
        "scheduler_host": "tcp://scheduler:8786",
        "worker_hosts": ["worker1", "worker2"],
        "job_scheduler": "slurm",
        "task_scheduler": "dask",
        "container_run_command": "apptainer run",
        "container_run_args": "-B /scratch --home /work",
        "container_env_flag": "--env",
        "worker_run_command": "mpiexec --label",
        "setup_tunnel": False,
        "tunnel_setup_command": "setup-tunnel --quiet",
    }
    cluster.update(cluster_updates)
    return {
        "container": "/containers/mspass.sif",
        "working_directory": "/work",
        "log_directory": "/work/logs",
        "database_directory": "/work/db",
        "worker_directory": "/work/worker",
        "workers_per_node": 8,
        "primary_node_workers": 2,
        "cluster_subnet_name": "cluster.example",
        "HPC_cluster": cluster,
    }


def _write_configuration(tmp_path, **cluster_updates):
    path = tmp_path / "configuration.yaml"
    path.write_text(yaml.safe_dump(_configuration(**cluster_updates)))
    return path


class Process:
    def __init__(self, status=None):
        self.status = status
        self.poll_count = 0
        self.terminate_count = 0
        self.kill_count = 0
        self.wait_calls = []

    def poll(self):
        self.poll_count += 1
        return self.status

    def terminate(self):
        self.terminate_count += 1
        self.status = 0

    def kill(self):
        self.kill_count += 1
        self.status = -9

    def wait(self, timeout):
        self.wait_calls.append(timeout)
        return self.status


def _launcher(launcher_module, tmp_path, **cluster_updates):
    return launcher_module.HPCClusterLauncher(
        _write_configuration(tmp_path, **cluster_updates), auto_launch=False
    )


def _assert_process_handles_initialized(launcher):
    assert launcher.scheduler_process is None
    assert launcher.dbserver_process is None
    assert launcher.primary_worker_process is None
    assert launcher.remote_worker_process is None
    assert launcher.jupyter_process is None


def test_all_explicit_hosts_initialize_without_scheduler_discovery(
    launcher_module, monkeypatch, tmp_path
):
    run_calls = []
    monkeypatch.setattr(
        launcher_module.subprocess,
        "run",
        lambda *args, **kwargs: run_calls.append((args, kwargs)),
    )

    launcher = _launcher(launcher_module, tmp_path)

    assert launcher.primary_node == "primary"
    assert launcher.database_host == "database:27017"
    assert launcher.scheduler_host == "tcp://scheduler:8786"
    assert launcher.worker_hosts == ["worker1", "worker2"]
    assert run_calls == []
    _assert_process_handles_initialized(launcher)


def test_mixed_auto_hosts_use_exact_slurm_discovery(
    launcher_module, monkeypatch, tmp_path
):
    calls = []

    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(stdout="node1\nnode2\nnode3\n")

    monkeypatch.setattr(launcher_module.subprocess, "run", run)
    launcher = _launcher(
        launcher_module,
        tmp_path,
        primary_host="auto",
        database_host="mongo.example:27017",
        scheduler_host="auto",
        worker_hosts="auto",
    )

    assert calls == [
        (
            ["scontrol", "show", "hostname"],
            {"capture_output": True, "text": True},
        )
    ]
    assert launcher.primary_node == "node1"
    assert launcher.database_host == "mongo.example:27017"
    assert launcher.scheduler_host == "node1"
    assert launcher.worker_hosts == ["node2", "node3"]
    _assert_process_handles_initialized(launcher)


def test_mixed_hosts_keep_explicit_primary_and_scheduler(
    launcher_module, monkeypatch, tmp_path
):
    calls = []

    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(stdout="node1\nnode2\nnode3\n")

    monkeypatch.setattr(launcher_module.subprocess, "run", run)
    launcher = _launcher(
        launcher_module,
        tmp_path,
        primary_host="node2",
        database_host="auto",
        scheduler_host="tcp://explicit-scheduler:8786",
        worker_hosts="auto",
    )

    assert calls == [
        (
            ["scontrol", "show", "hostname"],
            {"capture_output": True, "text": True},
        )
    ]
    assert launcher.primary_node == "node2"
    assert launcher.database_host == "node2"
    assert launcher.scheduler_host == "tcp://explicit-scheduler:8786"
    assert launcher.worker_hosts == ["node1", "node3"]
    _assert_process_handles_initialized(launcher)


def test_all_auto_hosts_use_primary_and_remaining_allocated_nodes(
    launcher_module, monkeypatch, tmp_path
):
    monkeypatch.setattr(
        launcher_module.subprocess,
        "run",
        lambda args, **kwargs: SimpleNamespace(stdout="node1 node2"),
    )
    launcher = _launcher(
        launcher_module,
        tmp_path,
        primary_host="auto",
        database_host="auto",
        scheduler_host="auto",
        worker_hosts="auto",
    )

    assert launcher.primary_node == "node1"
    assert launcher.database_host == "node1"
    assert launcher.scheduler_host == "node1"
    assert launcher.worker_hosts == ["node2"]
    _assert_process_handles_initialized(launcher)


def test_tunnel_executes_newly_constructed_argv(launcher_module, monkeypatch, tmp_path):
    calls = []

    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(launcher_module.subprocess, "run", run)
    _launcher(
        launcher_module,
        tmp_path,
        setup_tunnel=True,
        tunnel_setup_command="setup-tunnel --quiet",
    )

    assert calls == [
        (
            ["setup-tunnel", "--quiet", "primary"],
            {"capture_output": True, "text": True, "check": True},
        )
    ]


def test_repository_iu_configuration_is_valid_yaml():
    configuration_path = (
        REPOSITORY_ROOT / "scripts" / "IU_examples" / "python" / "configuration.yaml"
    )
    configuration = yaml.safe_load(configuration_path.read_text())
    assert isinstance(configuration, dict)
    assert isinstance(configuration["HPC_cluster"], dict)


def test_worker_argv_contains_complete_endpoints_without_shell_syntax(
    launcher_module, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    args = launcher._build_worker_run_args()

    assert args == [
        "mpiexec",
        "--label",
        "-n",
        "2",
        "-ppn",
        "1",
        "-hosts",
        "worker1",
        "worker2",
        "apptainer",
        "run",
        "-B",
        "/scratch",
        "--home",
        "/work",
        "--env",
        "MSPASS_ROLE=worker,MSPASS_WORK_DIR=/work,"
        "MSPASS_SCHEDULER_ADDRESS=tcp://scheduler:8786,"
        "MSPASS_DB_ADDRESS=database:27017,"
        "MSPASS_WORKER_ARG=--nworkers=8 --nthreads 1",
        "/containers/mspass.sif",
    ]
    assert "&" not in args


def test_hpc_launcher_never_uses_shell_metacharacters_or_communicate(
    launcher_module,
):
    source = LAUNCHER_PATH.read_text()
    source = source[
        source.index("class HPCClusterLauncher") : source.index("class DesktopLauncher")
    ]
    assert "shell=True" not in source
    assert '.append("&")' not in source
    assert ".communicate(" not in source


def test_popen_executes_the_argv_list_without_a_shell(launcher_module, monkeypatch):
    calls = []
    process = Process(None)

    def popen(*args, **kwargs):
        calls.append((args, kwargs))
        return process

    monkeypatch.setattr(launcher_module.subprocess, "Popen", popen)
    argv = ["program", "argument with spaces"]

    assert launcher_module.HPCClusterLauncher._popen(argv) is process
    assert calls == [
        (
            (argv,),
            {"close_fds": True},
        )
    ]


def test_auto_scheduler_host_uses_complete_default_endpoint(
    launcher_module, monkeypatch, tmp_path
):
    monkeypatch.setattr(
        launcher_module.subprocess,
        "run",
        lambda args, **kwargs: SimpleNamespace(stdout="node1 node2"),
    )
    launcher = _launcher(
        launcher_module,
        tmp_path,
        primary_host="auto",
        database_host="auto",
        scheduler_host="auto",
        worker_hosts="auto",
    )

    assert launcher.scheduler_host == "node1"
    assert launcher._scheduler_endpoint(launcher.scheduler_host) == "tcp://node1:8786"
    monkeypatch.setenv("DASK_SCHEDULER_PORT", "9876")
    assert launcher._scheduler_endpoint(launcher.scheduler_host) == "tcp://node1:9876"
    assert (
        launcher._scheduler_endpoint("tcp://scheduler:8786") == "tcp://scheduler:8786"
    )


def test_status_uses_none_as_only_running_value_and_skips_absent_handles(
    launcher_module, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    running = Process(None)
    successful_exit = Process(0)
    failed_exit = Process(9)
    launcher.dbserver_process = running
    launcher.scheduler_process = successful_exit

    assert launcher.status("db", verbose=False) == 1
    assert launcher.status("scheduler", verbose=False) == 0
    launcher.scheduler_process = failed_exit
    assert launcher.status("scheduler", verbose=False) == 0
    assert launcher.status("primary_worker", verbose=False) == 1
    assert launcher.status("remote_worker", verbose=False) == 1
    assert launcher.status("frontend", verbose=False) == 1
    assert launcher.status("all", verbose=False) == 0
    assert running.poll_count == 2
    assert successful_exit.poll_count == 1
    assert failed_exit.poll_count == 2
    assert launcher.primary_worker_process is None


def test_status_treats_absent_required_services_as_stopped(launcher_module, tmp_path):
    launcher = _launcher(launcher_module, tmp_path)
    assert launcher.status("db", verbose=False) == 0
    assert launcher.status("scheduler", verbose=False) == 0


def test_status_all_ignores_absent_optional_processes(launcher_module, tmp_path):
    launcher = _launcher(launcher_module, tmp_path)
    database = Process(None)
    scheduler = Process(None)
    launcher.dbserver_process = database
    launcher.scheduler_process = scheduler

    assert launcher.status("all", verbose=False) == 1
    assert database.poll_count == 1
    assert scheduler.poll_count == 1


def test_launch_waits_for_both_services_before_any_worker(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    events = []
    processes = []

    def popen(args):
        process = Process(None)
        processes.append((args, process))
        role = next(value for value in args if "MSPASS_ROLE=" in value)
        events.append(("start", role))
        return process

    def ready():
        assert len(processes) == 2
        events.append(("ready",))

    monkeypatch.setattr(launcher, "_popen", popen)
    monkeypatch.setattr(launcher, "_wait_for_services", ready)
    launcher.launch()

    assert events[0][1].startswith("MSPASS_ROLE=scheduler")
    assert events[1][1].startswith("MSPASS_ROLE=db")
    assert events[2] == ("ready",)
    assert events[3][1].startswith("MSPASS_ROLE=worker")
    assert events[4][1].startswith("MSPASS_ROLE=worker")
    assert len(processes) == 4
    assert all("&" not in event[1] for event in events if event[0] == "start")
    scheduler_args = processes[0][0]
    database_args = processes[1][0]
    remote_worker_args = processes[2][0]
    primary_worker_args = processes[3][0]
    assert scheduler_args == [
        "apptainer",
        "run",
        "-B",
        "/scratch",
        "--home",
        "/work",
        "--env",
        "MSPASS_ROLE=scheduler,MSPASS_WORK_DIR=/work,MSPASS_SCHEDULER=dask,MSPASS_SCHEDULER_ADDRESS=tcp://scheduler:8786",
        "/containers/mspass.sif",
    ]
    assert database_args == [
        "apptainer",
        "run",
        "-B",
        "/scratch",
        "--home",
        "/work",
        "--env",
        "MSPASS_ROLE=db,MSPASS_WORK_DIR=/work,MSPASS_DB_DIR=/work/db",
        "/containers/mspass.sif",
    ]
    assert remote_worker_args == launcher._build_worker_run_args()
    assert primary_worker_args == [
        "apptainer",
        "run",
        "-B",
        "/scratch",
        "--home",
        "/work",
        "--env",
        "MSPASS_ROLE=worker,MSPASS_WORK_DIR=/work,"
        "MSPASS_SCHEDULER_ADDRESS=tcp://scheduler:8786,"
        "MSPASS_DB_ADDRESS=database:27017,"
        "MSPASS_WORKER_ARG=--nworkers=2 --nthreads 1",
        "/containers/mspass.sif",
    ]


def test_readiness_retries_closes_clients_and_uses_exact_endpoints(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    launcher.scheduler_process = Process(None)
    launcher.dbserver_process = Process(None)
    mongo_instances = []
    dask_instances = []
    attempts = {"mongo": 0, "dask": 0}

    class Mongo:
        def __init__(self, host, **kwargs):
            assert host == "database:27017"
            assert kwargs == {"serverSelectionTimeoutMS": 2000}
            self.closed = False
            self.admin = self
            mongo_instances.append(self)

        def command(self, command):
            assert command == "ping"
            attempts["mongo"] += 1
            if attempts["mongo"] == 1:
                raise RuntimeError("not ready")

        def close(self):
            self.closed = True

    class Dask:
        def __init__(self, endpoint, timeout):
            assert endpoint == "tcp://scheduler:8786"
            assert timeout == "2s"
            self.closed = False
            dask_instances.append(self)

        def scheduler_info(self):
            attempts["dask"] += 1
            if attempts["dask"] == 1:
                raise RuntimeError("not ready")

        def close(self):
            self.closed = True

    clock = iter([0.0, 0.0, 0.1])
    sleep_calls = []
    monkeypatch.setenv("MSPASS_STARTUP_TIMEOUT_SECONDS", "3.5")
    monkeypatch.setenv("MSPASS_STARTUP_POLL_SECONDS", "0.125")
    monkeypatch.setattr(launcher_module, "MongoClient", Mongo)
    monkeypatch.setattr(launcher_module, "Client", Dask)
    monkeypatch.setattr(launcher_module.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(
        launcher_module.time, "sleep", lambda delay: sleep_calls.append(delay)
    )
    launcher._wait_for_services()

    assert attempts == {"mongo": 2, "dask": 2}
    assert sleep_calls == [0.125]
    assert all(instance.closed for instance in mongo_instances + dask_instances)


def test_timeout_starts_no_workers_and_reaps_all_owned_children(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    started = []

    def popen(args):
        process = Process(None)
        started.append(process)
        return process

    monotonic = iter([0.0, 1.0])
    monkeypatch.setenv("MSPASS_STARTUP_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("MSPASS_STARTUP_POLL_SECONDS", "0.25")
    monkeypatch.setattr(launcher, "_popen", popen)
    monkeypatch.setattr(
        launcher, "_probe_database", lambda: (_ for _ in ()).throw(RuntimeError())
    )
    monkeypatch.setattr(
        launcher, "_probe_scheduler", lambda: (_ for _ in ()).throw(RuntimeError())
    )
    monkeypatch.setattr(launcher_module.time, "monotonic", lambda: next(monotonic))

    with pytest.raises(RuntimeError):
        launcher.launch()

    assert len(started) == 2
    assert all(process.terminate_count == 1 for process in started)
    assert all(process.wait_calls == [10] for process in started)
    assert launcher.remote_worker_process is None
    assert launcher.primary_worker_process is None


def test_owned_child_early_exit_reaps_every_started_process(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    scheduler = Process(17)
    started = []
    monkeypatch.setattr(
        launcher, "_popen", lambda args: started.append(scheduler) or scheduler
    )

    with pytest.raises(RuntimeError, match="scheduler.*17"):
        launcher.launch()

    assert started == [scheduler]
    assert scheduler.wait_calls == [10]
    assert scheduler.terminate_count == 0
    assert launcher.dbserver_process is None
    assert launcher.remote_worker_process is None
    assert launcher.primary_worker_process is None


@pytest.mark.parametrize("failed_role", ["database", "remote", "primary"])
def test_each_other_owned_child_early_exit_reaps_every_started_process(
    launcher_module, monkeypatch, tmp_path, failed_role
):
    launcher = _launcher(launcher_module, tmp_path)
    started = []

    def popen(args):
        environment = next(value for value in args if "MSPASS_ROLE=" in value)
        if "MSPASS_ROLE=db" in environment:
            role = "database"
        elif "MSPASS_ROLE=scheduler" in environment:
            role = "scheduler"
        elif args[0] == "mpiexec":
            role = "remote"
        else:
            role = "primary"
        process = Process(23 if role == failed_role else None)
        started.append((role, process))
        return process

    monkeypatch.setattr(launcher, "_popen", popen)
    monkeypatch.setattr(launcher, "_wait_for_services", lambda: None)

    with pytest.raises(RuntimeError, match="23"):
        launcher.launch()

    expected_roles = {
        "database": ["scheduler", "database"],
        "remote": ["scheduler", "database", "remote"],
        "primary": ["scheduler", "database", "remote", "primary"],
    }
    assert [role for role, _ in started] == expected_roles[failed_role]
    assert all(process.wait_calls == [10] for _, process in started)
    assert all(
        process.terminate_count == (0 if role == failed_role else 1)
        for role, process in started
    )


def test_launch_preserves_system_exception_identity_after_cleanup(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    scheduler = Process(None)
    failure = OSError("container runtime unavailable")
    calls = 0

    def popen(args):
        nonlocal calls
        calls += 1
        if calls == 1:
            return scheduler
        raise failure

    monkeypatch.setattr(launcher, "_popen", popen)

    with pytest.raises(OSError) as error:
        launcher.launch()

    assert error.value is failure
    assert scheduler.terminate_count == 1
    assert scheduler.wait_calls == [10]
    assert launcher.scheduler_process is None


def test_cleanup_failure_retains_process_handle_for_retry(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    scheduler = Process(None)
    database = Process(None)
    launcher.scheduler_process = scheduler
    launcher.dbserver_process = database
    original_stop = launcher._stop_process

    def stop(process):
        if process is scheduler:
            raise OSError("terminate failed")
        original_stop(process)

    monkeypatch.setattr(launcher, "_stop_process", stop)

    with pytest.raises(OSError, match="terminate failed"):
        launcher._cleanup_owned_processes()

    assert launcher.scheduler_process is scheduler
    assert launcher.dbserver_process is None
    assert database.terminate_count == 1
    assert database.wait_calls == [10]


def test_batch_and_interactive_frontends_include_both_endpoints_without_shell(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    ready_calls = []
    run_calls = []
    popen_calls = []
    frontend = Process(None)
    monkeypatch.setattr(
        launcher, "_wait_for_services", lambda: ready_calls.append(True)
    )
    monkeypatch.setattr(
        launcher_module.subprocess,
        "run",
        lambda args, **kwargs: run_calls.append((args, kwargs))
        or SimpleNamespace(stdout="ok", stderr=""),
    )
    monkeypatch.setattr(
        launcher,
        "_popen",
        lambda args: popen_calls.append(args) or frontend,
    )

    launcher.run("analysis.py")
    result = launcher.interactive_session()

    assert ready_calls == [True, True]
    frontend_args = [
        "apptainer",
        "run",
        "-B",
        "/scratch",
        "--home",
        "/work",
        "--env",
        "MSPASS_ROLE=frontend,MSPASS_WORK_DIR=/work,"
        "MSPASS_DB_ADDRESS=database:27017,"
        "MSPASS_SCHEDULER_ADDRESS=tcp://scheduler:8786",
        "/containers/mspass.sif",
    ]
    assert run_calls[0][0] == frontend_args + ["--batch", "analysis.py"]
    assert popen_calls == [frontend_args]
    assert run_calls[0][1] == {"capture_output": True, "text": True}
    assert result is frontend
    assert launcher.jupyter_process is frontend
    assert frontend.wait_calls == []


def test_interactive_early_exit_reaps_all_owned_processes(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    services = [Process(None), Process(None), Process(None), Process(None)]
    (
        launcher.scheduler_process,
        launcher.dbserver_process,
        launcher.remote_worker_process,
        launcher.primary_worker_process,
    ) = services
    frontend = Process(31)
    monkeypatch.setattr(launcher, "_wait_for_services", lambda: None)
    monkeypatch.setattr(launcher, "_popen", lambda args: frontend)

    with pytest.raises(RuntimeError, match="frontend.*31"):
        launcher.interactive_session()

    assert frontend.terminate_count == 0
    assert frontend.wait_calls == [10]
    assert all(process.terminate_count == 1 for process in services)
    assert all(process.wait_calls == [10] for process in services)
    assert launcher.jupyter_process is None


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("MSPASS_STARTUP_TIMEOUT_SECONDS", "0"),
        ("MSPASS_STARTUP_TIMEOUT_SECONDS", "nan"),
        ("MSPASS_STARTUP_POLL_SECONDS", "-1"),
        ("MSPASS_STARTUP_POLL_SECONDS", "invalid"),
    ],
)
def test_startup_settings_have_fixed_defaults_and_reject_invalid_values(
    launcher_module, monkeypatch, name, value
):
    monkeypatch.delenv("MSPASS_STARTUP_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("MSPASS_STARTUP_POLL_SECONDS", raising=False)
    assert launcher_module.HPCClusterLauncher._startup_settings() == (120.0, 2.0)
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        launcher_module.HPCClusterLauncher._startup_settings()


def test_startup_settings_accept_valid_overrides(launcher_module, monkeypatch):
    monkeypatch.setenv("MSPASS_STARTUP_TIMEOUT_SECONDS", "3.5")
    monkeypatch.setenv("MSPASS_STARTUP_POLL_SECONDS", "0.125")
    assert launcher_module.HPCClusterLauncher._startup_settings() == (3.5, 0.125)


def test_invalid_startup_settings_start_no_children(
    launcher_module, monkeypatch, tmp_path
):
    launcher = _launcher(launcher_module, tmp_path)
    started = []
    monkeypatch.setenv("MSPASS_STARTUP_TIMEOUT_SECONDS", "invalid")
    monkeypatch.setattr(launcher, "_popen", lambda args: started.append(args))

    with pytest.raises(ValueError):
        launcher.launch()

    assert started == []
