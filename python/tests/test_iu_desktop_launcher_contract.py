import importlib.util
import inspect
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
LAUNCHER_PATH = REPOSITORY_ROOT / "scripts" / "IU_examples" / "python" / "launcher.py"
COMPOSE_SERVICES = (
    "mspass-db\n" "mspass-scheduler\n" "mspass-worker\n" "mspass-frontend\n"
)


@pytest.fixture
def launcher_module():
    spec = importlib.util.spec_from_file_location(
        "mspass_iu_desktop_launcher_test", LAUNCHER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class BrowserProcess:
    def __init__(self, status=None):
        self.status = status
        self.terminate_count = 0
        self.wait_count = 0

    def poll(self):
        return self.status

    def terminate(self):
        self.terminate_count += 1
        self.status = 0

    def wait(self, timeout=None):
        self.wait_count += 1
        return self.status

    def kill(self):
        self.terminate_count += 1
        self.status = -9


class PollFailureBrowser(BrowserProcess):
    def poll(self):
        raise OSError("browser poll failed")


class SubprocessScript:
    def __init__(self, results):
        self.results = list(results)
        self.run_calls = []
        self.popen_calls = []
        self.browser = BrowserProcess()
        self.popen_error = None

    def run(self, argv, **kwargs):
        self.run_calls.append((argv, kwargs))
        assert self.results, f"unexpected subprocess.run call: {argv}"
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    def popen(self, argv, **kwargs):
        self.popen_calls.append((argv, kwargs))
        if self.popen_error is not None:
            raise self.popen_error
        return self.browser


def completed(stdout="", stderr="", returncode=0):
    return SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode)


def install_subprocess_script(monkeypatch, launcher_module, results):
    script = SubprocessScript(results)
    monkeypatch.setattr(launcher_module.subprocess, "run", script.run)
    monkeypatch.setattr(launcher_module.subprocess, "Popen", script.popen)
    return script


def compose(configuration, *arguments):
    return ["docker", "compose", "-f", configuration, *arguments]


def bare_launcher(launcher_module, *, owned=False):
    launcher = launcher_module.DesktopLauncher.__new__(launcher_module.DesktopLauncher)
    launcher.configuration_file = "custom.yaml"
    launcher.host_os = "Linux"
    launcher.browser = "browser"
    launcher.verbose = False
    launcher.browser_process = None
    launcher._owns_stack = owned
    launcher._url = None
    launcher._startup_timeout = 120.0
    launcher._startup_poll = 2.0
    return launcher


@pytest.mark.parametrize(
    "host_os,browser_argv",
    [
        ("Linux", ["browser", "https://127.0.0.1:8888/lab?token=abc"]),
        (
            "Darwin",
            ["open", "-a", "browser", "https://127.0.0.1:8888/lab?token=abc"],
        ),
        (
            "Windows",
            [
                "cmd",
                "/c",
                "start",
                "",
                "browser",
                "https://127.0.0.1:8888/lab?token=abc",
            ],
        ),
    ],
)
def test_launch_uses_exact_compose_browser_and_timing_contract(
    launcher_module, monkeypatch, host_os, browser_argv
):
    configuration = "data/yaml/compose.yaml"
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
            completed("starting\n"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed("Jupyter: https://127.0.0.1:8888/lab?token=abc\n"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed(),
        ],
    )
    monotonic = iter([10.0, 10.5])
    sleeps = []
    monkeypatch.setattr(launcher_module.time, "monotonic", lambda: next(monotonic))
    monkeypatch.setattr(launcher_module.time, "sleep", sleeps.append)

    launcher = launcher_module.DesktopLauncher(
        host_os=host_os, browser="browser", verbose=False
    )

    expected_url = "https://127.0.0.1:8888/lab?token=abc"
    assert launcher.configuration_file == configuration
    assert launcher._startup_timeout == 120.0
    assert launcher._startup_poll == 2.0
    assert launcher.url() == expected_url
    assert launcher.launch() == expected_url
    assert script.run_calls == [
        (
            compose(configuration, "config", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(configuration, "ps", "--status", "running", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(configuration, "up", "-d"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(configuration, "logs", "mspass-frontend"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(configuration, "config", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(configuration, "ps", "--status", "running", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(configuration, "logs", "mspass-frontend"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(configuration, "config", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(configuration, "ps", "--status", "running", "--services"),
            {"capture_output": True, "text": True},
        ),
    ]
    assert script.popen_calls == [(browser_argv, {})]
    assert sleeps == [2.0]
    assert launcher._owns_stack is True

    launcher.shutdown()
    launcher.shutdown()
    assert script.browser.terminate_count == 1
    assert script.browser.wait_count == 1
    assert script.run_calls[-1] == (
        compose(configuration, "down"),
        {"capture_output": True, "text": True},
    )
    assert script.results == []


def test_default_os_comes_from_platform_system(launcher_module, monkeypatch):
    monkeypatch.setattr(launcher_module.platform, "system", lambda: "Linux")
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed("http://localhost:8888/lab?token=abc"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
        ],
    )
    launcher = launcher_module.DesktopLauncher(browser="browser", verbose=False)
    assert launcher.host_os == "Linux"
    assert script.popen_calls == [
        (["browser", "http://localhost:8888/lab?token=abc"], {})
    ]
    launcher.shutdown()


def test_unsupported_os_and_bad_timing_fail_before_compose(
    launcher_module, monkeypatch
):
    run_calls = []
    popen_calls = []
    monkeypatch.setattr(
        launcher_module.subprocess,
        "run",
        lambda *args, **kwargs: run_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        launcher_module.subprocess,
        "Popen",
        lambda *args, **kwargs: popen_calls.append((args, kwargs)),
    )

    with pytest.raises(ValueError):
        launcher_module.DesktopLauncher(host_os="Plan9", verbose=False)

    for value in ("0", "-1", "nan", "inf", "not-a-number"):
        monkeypatch.setenv("MSPASS_STARTUP_TIMEOUT_SECONDS", value)
        with pytest.raises(ValueError):
            launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    assert run_calls == []
    assert popen_calls == []


def test_caller_owned_stack_is_preserved_and_launch_is_idempotent(
    launcher_module, monkeypatch
):
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed("http://localhost:8888/lab?token=caller"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
        ],
    )
    launcher = launcher_module.DesktopLauncher(
        host_os="Linux", browser="browser", verbose=False
    )
    assert launcher._owns_stack is False
    call_count = len(script.run_calls)
    assert launcher.launch() == "http://localhost:8888/lab?token=caller"
    assert len(script.run_calls) == call_count

    launcher.shutdown()
    launcher.shutdown()
    assert all(call[0][-1] != "down" for call in script.run_calls)
    assert script.browser.terminate_count == 1
    assert script.browser.wait_count == 1


def test_early_exit_and_timeout_clean_only_owned_stack(launcher_module, monkeypatch):
    early = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
            completed("starting"),
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
        ],
    )
    with pytest.raises(RuntimeError, match="exited"):
        launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    assert early.run_calls[-1][0] == compose("data/yaml/compose.yaml", "down")
    assert early.popen_calls == []

    timeout = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
            completed("still starting"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed("still starting"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed(),
        ],
    )
    monkeypatch.setenv("MSPASS_STARTUP_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("MSPASS_STARTUP_POLL_SECONDS", "0.25")
    monotonic = iter([0.0, 0.5, 1.0])
    sleeps = []
    monkeypatch.setattr(launcher_module.time, "monotonic", lambda: next(monotonic))
    monkeypatch.setattr(launcher_module.time, "sleep", sleeps.append)
    with pytest.raises(RuntimeError, match="timed out"):
        launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    assert timeout.run_calls[-1][0] == compose("data/yaml/compose.yaml", "down")
    assert timeout.popen_calls == []
    assert sleeps == [0.25]


def test_failed_attach_preserves_caller_owned_stack(launcher_module, monkeypatch):
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed("still starting"),
            completed(COMPOSE_SERVICES),
            completed(),
        ],
    )
    with pytest.raises(RuntimeError, match="exited"):
        launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    assert all(call[0][-1] != "down" for call in script.run_calls)
    assert script.popen_calls == []


@pytest.mark.parametrize("failure", [OSError("missing"), RuntimeError("broken")])
def test_browser_failure_raises_runtime_error_and_cleans_owned_stack(
    launcher_module, monkeypatch, failure
):
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
            completed("http://localhost:8888/lab?token=abc"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed(),
        ],
    )
    script.popen_error = failure
    with pytest.raises(RuntimeError, match="browser"):
        launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    assert script.run_calls[-1][0] == compose("data/yaml/compose.yaml", "down")


@pytest.mark.parametrize("operation", ["config", "ps", "up", "logs"])
def test_compose_startup_failures_raise_runtime_error(
    launcher_module, monkeypatch, operation
):
    failure = completed(stderr="compose failed", returncode=7)
    if operation == "config":
        results = [failure]
    elif operation == "ps":
        results = [completed(COMPOSE_SERVICES), failure]
    elif operation == "up":
        results = [completed(COMPOSE_SERVICES), completed(), failure]
    else:
        results = [
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
            failure,
            completed(),
        ]
    script = install_subprocess_script(monkeypatch, launcher_module, results)

    with pytest.raises(RuntimeError, match="command failed"):
        launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    if operation == "logs":
        assert script.run_calls[-1][0] == compose("data/yaml/compose.yaml", "down")
    else:
        assert all(call[0][-1] != "down" for call in script.run_calls)


def test_post_up_status_failure_raises_runtime_error_and_cleans_owned_stack(
    launcher_module, monkeypatch
):
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
            completed("still starting"),
            completed(COMPOSE_SERVICES),
            completed(stderr="ps failed", returncode=7),
            completed(),
        ],
    )

    with pytest.raises(RuntimeError, match="command failed"):
        launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    assert script.run_calls[-1][0] == compose("data/yaml/compose.yaml", "down")


def test_status_run_and_shutdown_have_exact_result_contracts(
    launcher_module, monkeypatch
):
    launcher = bare_launcher(launcher_module, owned=True)
    browser = BrowserProcess()
    launcher.browser_process = browser
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES + "other\n"),
            completed(COMPOSE_SERVICES),
            completed("other\n"),
            completed(stdout="ran"),
            completed(),
        ],
    )

    assert launcher.status() == 1
    assert launcher.status() == 0
    result = launcher.run("analysis.py")
    assert result.stdout == "ran"
    launcher.shutdown()
    assert script.run_calls == [
        (
            compose("custom.yaml", "config", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose("custom.yaml", "ps", "--status", "running", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose("custom.yaml", "config", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose("custom.yaml", "ps", "--status", "running", "--services"),
            {"capture_output": True, "text": True},
        ),
        (
            compose(
                "custom.yaml",
                "exec",
                "-T",
                "mspass-frontend",
                "python",
                "analysis.py",
            ),
            {"capture_output": True, "text": True},
        ),
        (
            compose("custom.yaml", "down"),
            {"capture_output": True, "text": True},
        ),
    ]
    assert browser.terminate_count == 1
    assert browser.wait_count == 1
    assert launcher._owns_stack is False


def test_status_requires_frontend_and_every_configured_service(
    launcher_module, monkeypatch
):
    launcher = bare_launcher(launcher_module)
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [completed("mspass-db\nmspass-worker\n")],
    )
    with pytest.raises(RuntimeError, match="no mspass-frontend"):
        launcher.status()
    assert len(script.run_calls) == 1

    script.results.extend(
        [
            completed(COMPOSE_SERVICES),
            completed("mspass-db\nmspass-frontend\n"),
        ]
    )
    assert launcher.status() == 0


@pytest.mark.parametrize("operation", ["status", "run", "shutdown"])
def test_public_compose_failures_raise_runtime_error(
    launcher_module, monkeypatch, operation
):
    launcher = bare_launcher(launcher_module, owned=operation == "shutdown")
    install_subprocess_script(
        monkeypatch,
        launcher_module,
        [completed(stderr=f"{operation} failed", returncode=9)],
    )
    with pytest.raises(RuntimeError):
        if operation == "status":
            launcher.status()
        elif operation == "run":
            launcher.run("analysis.py")
        else:
            launcher.shutdown()
    if operation == "shutdown":
        assert launcher._owns_stack is False


def test_browser_nonzero_exit_is_waited_and_stack_is_cleaned(
    launcher_module, monkeypatch
):
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
            completed("http://localhost:8888/lab?token=abc"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed(),
        ],
    )
    script.browser.status = 3
    with pytest.raises(RuntimeError, match="browser"):
        launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    assert script.browser.terminate_count == 0
    assert script.browser.wait_count == 1
    assert script.run_calls[-1][0] == compose("data/yaml/compose.yaml", "down")


def test_browser_poll_failure_terminates_waits_and_cleans_owned_stack(
    launcher_module, monkeypatch
):
    script = install_subprocess_script(
        monkeypatch,
        launcher_module,
        [
            completed(COMPOSE_SERVICES),
            completed(),
            completed(),
            completed("http://localhost:8888/lab?token=abc"),
            completed(COMPOSE_SERVICES),
            completed(COMPOSE_SERVICES),
            completed(),
        ],
    )
    script.browser = PollFailureBrowser()

    with pytest.raises(RuntimeError, match="browser poll failed"):
        launcher_module.DesktopLauncher(host_os="Linux", verbose=False)
    assert script.browser.terminate_count == 1
    assert script.browser.wait_count == 1
    assert script.run_calls[-1][0] == compose("data/yaml/compose.yaml", "down")


def test_url_parser_never_returns_a_fallback(launcher_module):
    assert launcher_module.extract_jupyter_url("not ready") is None
    assert launcher_module.extract_jupyter_url(None) is None
    assert (
        launcher_module.extract_jupyter_url(
            "prefix http://127.0.0.1:8888/lab?token=one suffix"
        )
        == "http://127.0.0.1:8888/lab?token=one"
    )
    assert (
        launcher_module.extract_jupyter_url("https://localhost:8888/tree?token=two")
        == "https://localhost:8888/tree?token=two"
    )


def test_desktop_implements_base_method_signatures(launcher_module):
    base = launcher_module.BasicMsPASSLauncher
    desktop = launcher_module.DesktopLauncher
    assert inspect.signature(desktop.launch) == inspect.signature(base.launch)
    assert inspect.signature(desktop.status) == inspect.signature(base.status)
    assert inspect.signature(desktop.run) == inspect.signature(base.run)
