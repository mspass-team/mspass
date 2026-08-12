import json
import os
import re
import subprocess
import uuid
from pathlib import Path
from unittest import mock

import pytest

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
COMPOSE_FILES = (
    REPOSITORY_ROOT / "data" / "yaml" / "compose.yaml",
    REPOSITORY_ROOT / "data" / "yaml" / "docker-compose_spark.yaml",
    REPOSITORY_ROOT / "data" / "yaml" / "docker-compose_sharding.yaml",
    REPOSITORY_ROOT
    / "scripts"
    / "IU_examples"
    / "python"
    / "configuration_docker.yaml",
)
START_MSPASS = REPOSITORY_ROOT / "scripts" / "start-mspass.sh"
COMPOSE_GUIDES = (
    REPOSITORY_ROOT
    / "docs"
    / "source"
    / "getting_started"
    / "deploy_mspass_with_docker_compose.rst",
    REPOSITORY_ROOT
    / "docs"
    / "source"
    / "getting_started"
    / "command_line_desktop.rst",
)
RUN_REAL_COMPOSE_TESTS = os.environ.get("MSPASS_RUN_COMPOSE_SECURITY_TESTS") == "1"


def _compose_config(path, username=None, password=None, token=None):
    env = os.environ.copy()
    for key in (
        "MONGO_INITDB_ROOT_USERNAME",
        "MONGO_INITDB_ROOT_PASSWORD",
        "JUPYTER_TOKEN",
    ):
        env.pop(key, None)
    if username is not None:
        env["MONGO_INITDB_ROOT_USERNAME"] = username
    if password is not None:
        env["MONGO_INITDB_ROOT_PASSWORD"] = password
    if token is not None:
        env["JUPYTER_TOKEN"] = token
    return subprocess.run(
        ["docker", "compose", "-f", str(path), "config", "--format", "json"],
        capture_output=True,
        env=env,
        text=True,
    )


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
@pytest.mark.parametrize(
    ("username", "password"),
    ((None, "secret"), ("root", None), ("", "secret"), ("root", "")),
)
def test_compose_requires_nonempty_mongo_credentials(compose_file, username, password):
    result = _compose_config(compose_file, username, password)

    assert result.returncode != 0
    assert "required variable MONGO_INITDB_ROOT_" in result.stderr


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_compose_security_contract(compose_file):
    result = _compose_config(compose_file, username="root user", password="p@ss:/ word")
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)

    for service in config["services"].values():
        for binding in service.get("ports", []):
            assert binding["host_ip"] == "127.0.0.1"

    mongo_services = [
        service
        for service in config["services"].values()
        if service.get("environment", {}).get("MSPASS_ROLE")
        in ("db", "dbmanager", "shard")
    ]
    assert mongo_services
    for service in mongo_services:
        environment = service["environment"]
        assert environment["MSPASS_MONGO_AUTH"] == "true"
        assert environment["MONGO_INITDB_ROOT_USERNAME"] == "root user"
        assert environment["MONGO_INITDB_ROOT_PASSWORD"] == "p@ss:/ word"
        healthcheck = " ".join(service["healthcheck"]["test"])
        assert "db.auth(process.env.MONGO_INITDB_ROOT_USERNAME" in healthcheck
        assert "process.env.MONGO_INITDB_ROOT_PASSWORD" in healthcheck
        assert "--password" not in healthcheck

    frontend = config["services"]["mspass-frontend"]["environment"]
    assert "MSPASS_JUPYTER_PWD" not in frontend
    assert frontend["JUPYTER_TOKEN"] == ""
    assert frontend["MONGO_INITDB_ROOT_USERNAME"] == "root user"
    assert frontend["MONGO_INITDB_ROOT_PASSWORD"] == "p@ss:/ word"


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_compose_preserves_explicit_jupyter_token(compose_file):
    result = _compose_config(compose_file, "root", "secret", "chosen-token")

    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)
    assert (
        config["services"]["mspass-frontend"]["environment"]["JUPYTER_TOKEN"]
        == "chosen-token"
    )


def test_start_script_uses_authentication_without_exposing_plaintext_password():
    script = START_MSPASS.read_text()

    assert "MONGO_SERVER_SECURITY_ARGS=(--auth)" in script
    assert "process.env.MONGO_INITDB_ROOT_PASSWORD" in script
    assert "${MONGO_INITDB_ROOT_PASSWORD}" not in script
    assert '"${MONGO_CLIENT_AUTH_ARGS[@]}"' in script
    assert '--password "$MONGO_INITDB_ROOT_PASSWORD"' not in script
    assert 'quote(os.environ["MONGO_INITDB_ROOT_PASSWORD"]' in script


def test_start_script_leaves_jupyter_token_generation_to_jupyter_by_default():
    script = START_MSPASS.read_text()

    assert "if [[ -n ${JUPYTER_TOKEN:-} ]]; then" in script
    assert 'NOTEBOOK_ARGS+=("--NotebookApp.token=${JUPYTER_TOKEN}")' in script
    assert "MSPASS_JUPYTER_PWD: mspass" not in "\n".join(
        path.read_text() for path in COMPOSE_FILES
    )


def test_compose_guides_require_credentials_and_describe_token_login():
    for guide in COMPOSE_GUIDES:
        text = guide.read_text()
        assert "MONGO_INITDB_ROOT_USERNAME" in text
        assert "MONGO_INITDB_ROOT_PASSWORD" in text
        assert "docker compose logs mspass-frontend" in text
        assert "password ``mspass``" not in text


def _write_executable(path, content):
    path.write_text(content)
    path.chmod(0o755)


def _run_start_script(tmp_path, role, extra_environment=None, user_exists=False):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    command_log = tmp_path / "commands.log"
    user_state = tmp_path / "root-user-created"
    if user_exists:
        user_state.touch()
    _write_executable(
        fake_bin / "mongod",
        "#!/usr/bin/env bash\n"
        "line=mongod\n"
        "printf -v arguments ' <%s>' \"$@\"\n"
        'printf \'%s\\n\' "${line}${arguments}" >> "$COMMAND_LOG"\n',
    )
    _write_executable(
        fake_bin / "mongos",
        "#!/usr/bin/env bash\n"
        "line=mongos\n"
        "printf -v arguments ' <%s>' \"$@\"\n"
        'printf \'%s\\n\' "${line}${arguments}" >> "$COMMAND_LOG"\n',
    )
    _write_executable(
        fake_bin / "mongosh",
        """#!/usr/bin/env bash
line=mongosh
printf -v arguments ' <%s>' "$@"
printf '%s\n' "${line}${arguments}" >> "$COMMAND_LOG"
if [[ "$*" == *'db.createUser'* ]]; then
  [[ "$MONGO_INITDB_ROOT_USERNAME" == 'root user' ]]
  [[ "$MONGO_INITDB_ROOT_PASSWORD" == 'p@ss:/ word' ]]
  [[ ! -f "$MONGO_USER_STATE" ]] || exit 1
  touch "$MONGO_USER_STATE"
  exit 0
fi
if [[ "$*" == *'connectionStatus'* || "$*" == *'adminCommand({ping: 1})'* ]]; then
  attempts=0
  if [[ -f "$MONGO_SERVER_PROBE_STATE" ]]; then
    attempts=$(<"$MONGO_SERVER_PROBE_STATE")
  fi
  printf '%s\n' "$((attempts + 1))" > "$MONGO_SERVER_PROBE_STATE"
  if ((attempts < ${MONGO_SERVER_FAILURES_BEFORE_READY:-0})); then
    exit 1
  fi
fi
if [[ "$*" == *'connectionStatus'* ]]; then
  [[ -f "$MONGO_USER_STATE" ]]
  exit $?
fi
exit 0
""",
    )
    _write_executable(
        fake_bin / "jupyter",
        "#!/usr/bin/env bash\n"
        "line=jupyter\n"
        "printf -v arguments ' <%s>' \"$@\"\n"
        'printf \'%s\\n\' "${line}${arguments}" >> "$COMMAND_LOG"\n',
    )
    _write_executable(
        fake_bin / "pyspark",
        "#!/usr/bin/env bash\n"
        "previous=\n"
        'for argument in "$@"; do\n'
        '  if [[ "$previous" = --properties-file ]]; then\n'
        '    cp -p -- "$argument" "$SPARK_PROPERTIES_CAPTURE"\n'
        "  fi\n"
        "  previous=$argument\n"
        "done\n"
        "line=pyspark\n"
        "printf -v arguments ' <%s>' \"$@\"\n"
        'printf \'%s\\n\' "${line}${arguments}" >> "$COMMAND_LOG"\n',
    )
    _write_executable(fake_bin / "tail", "#!/usr/bin/env bash\nexit 0\n")
    environment = os.environ.copy()
    environment.update(
        {
            "COMMAND_LOG": str(command_log),
            "HOME": str(tmp_path),
            "JUPYTER_PORT": "8888",
            "MONGODB_PORT": "27017",
            "MONGO_INITDB_ROOT_PASSWORD": "p@ss:/ word",
            "MONGO_INITDB_ROOT_USERNAME": "root user",
            "MONGO_SERVER_PROBE_STATE": str(tmp_path / "mongo-server-probes"),
            "MONGO_USER_STATE": str(user_state),
            "MSPASS_DB_DIR": str(tmp_path / "db"),
            "MSPASS_DB_PATH": "scratch",
            "MSPASS_LOG_DIR": str(tmp_path / "logs"),
            "MSPASS_MONGO_AUTH": "true",
            "MSPASS_MONGO_KEYFILE": str(tmp_path / "mongo-keyfile"),
            "MSPASS_ROLE": role,
            "MSPASS_SCHEDULER": "dask",
            "MSPASS_SCHEDULER_ADDRESS": "scheduler",
            "MSPASS_SLEEP_TIME": "0",
            "MSPASS_WORK_DIR": str(tmp_path / "work"),
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "SPARK_PROPERTIES_CAPTURE": str(tmp_path / "spark.properties"),
        }
    )
    if extra_environment:
        environment.update(extra_environment)
    result = subprocess.run(
        ["bash", str(START_MSPASS)],
        capture_output=True,
        env=environment,
        text=True,
        timeout=10,
    )
    commands = command_log.read_text().splitlines() if command_log.exists() else []
    return result, commands, user_state


def test_standalone_start_enables_auth_and_initializes_root_user(tmp_path):
    result, commands, user_state = _run_start_script(tmp_path, "db")

    assert result.returncode == 0, result.stderr
    assert "p@ss:/ word" not in result.stdout
    assert "p@ss:/ word" not in result.stderr
    assert user_state.is_file()
    mongod_command = next(
        command for command in commands if command.startswith("mongod")
    )
    assert " <--auth>" in mongod_command
    assert sum("db.createUser" in command for command in commands) == 1
    authenticated = [command for command in commands if "connectionStatus" in command]
    assert len(authenticated) == 2
    assert all(
        "getSiblingDB" in command and ".auth(process.env" in command
        for command in authenticated
    )
    assert all(" <--password>" not in command for command in authenticated)
    assert all("p@ss:/ word" not in command for command in authenticated)


def test_standalone_restart_authenticates_without_recreating_root_user(tmp_path):
    result, commands, user_state = _run_start_script(tmp_path, "db", user_exists=True)

    assert result.returncode == 0, result.stderr
    assert user_state.is_file()
    assert all("db.createUser" not in command for command in commands)
    assert sum("connectionStatus" in command for command in commands) == 1


def test_standalone_restart_retries_authentication_after_server_becomes_ready(tmp_path):
    result, commands, user_state = _run_start_script(
        tmp_path,
        "db",
        {"MONGO_SERVER_FAILURES_BEFORE_READY": "2"},
        user_exists=True,
    )

    assert result.returncode == 0, result.stderr
    assert user_state.is_file()
    assert sum("connectionStatus" in command for command in commands) == 2
    assert all("db.createUser" not in command for command in commands)


def test_start_rejects_missing_auth_credentials_before_mongod(tmp_path):
    result, commands, _ = _run_start_script(
        tmp_path,
        "db",
        {"MONGO_INITDB_ROOT_PASSWORD": ""},
    )

    assert result.returncode != 0
    assert "MONGO_INITDB_ROOT_PASSWORD is required" in result.stderr
    assert not commands


@pytest.mark.parametrize("token", (None, "chosen-token"))
def test_frontend_passes_only_an_explicit_nonempty_jupyter_token(tmp_path, token):
    extra_environment = {"JUPYTER_TOKEN": token} if token is not None else {}
    result, commands, _ = _run_start_script(tmp_path, "frontend", extra_environment)

    assert result.returncode == 0, result.stderr
    jupyter_command = next(
        command for command in commands if command.startswith("jupyter")
    )
    if token is None:
        assert "NotebookApp.token" not in jupyter_command
    else:
        assert " <--NotebookApp.token=chosen-token>" in jupyter_command
    assert "NotebookApp.password" not in jupyter_command


def test_spark_frontend_keeps_encoded_mongo_credentials_out_of_arguments(tmp_path):
    result, commands, _ = _run_start_script(
        tmp_path,
        "frontend",
        {
            "MSPASS_DB_ADDRESS": "mspass-db",
            "MSPASS_SCHEDULER": "spark",
            "MSPASS_SCHEDULER_ADDRESS": "scheduler",
            "SPARK_MASTER_PORT": "7077",
        },
    )

    assert result.returncode == 0, result.stderr
    pyspark_command = next(
        command for command in commands if command.startswith("pyspark")
    )
    expected_uri = (
        "mongodb://root%20user:p%40ss%3A%2F%20word@mspass-db:27017/"
        "test.misc?authSource=admin"
    )
    properties_file = tmp_path / "spark.properties"
    assert properties_file.stat().st_mode & 0o777 == 0o600
    properties = properties_file.read_text()
    assert properties.count(expected_uri) == 2
    assert "spark.redaction.regex" in properties
    assert " <--properties-file>" in pyspark_command
    assert expected_uri not in pyspark_command
    assert "p@ss:/ word" not in pyspark_command
    assert "p@ss:/ word" not in result.stdout
    assert "p@ss:/ word" not in result.stderr


@pytest.mark.parametrize(
    ("role", "extra_environment"),
    (
        (
            "dbmanager",
            {
                "MSPASS_SHARD_LIST": "rs0/shard0:27017",
            },
        ),
        (
            "shard",
            {
                "MSPASS_SHARD_ID": "0",
            },
        ),
    ),
)
def test_sharded_roles_use_a_shared_keyfile_and_authenticated_clients(
    tmp_path, role, extra_environment
):
    result, commands, user_state = _run_start_script(tmp_path, role, extra_environment)

    assert result.returncode == 0, result.stderr
    assert user_state.is_file()
    server_commands = [
        command
        for command in commands
        if command.startswith("mongod <") or command.startswith("mongos <")
    ]
    assert server_commands
    assert all(" <--keyFile>" in command for command in server_commands)
    keyfile = tmp_path / "mongo-keyfile"
    assert keyfile.is_file()
    assert keyfile.stat().st_mode & 0o777 == 0o400
    assert len(keyfile.read_text().strip()) == 64
    authenticated = [command for command in commands if "getSiblingDB" in command]
    assert authenticated
    assert all(".auth(process.env" in command for command in authenticated)
    assert all(" <--password>" not in command for command in authenticated)
    if role == "dbmanager":
        assert any("hello: 1" in command for command in authenticated)


def test_client_uses_compose_mongo_credentials(monkeypatch):
    from mspasspy.client import Client
    from mspasspy.db.client import DBClient

    monkeypatch.setenv("MSPASS_DB_ADDRESS", "mspass-db")
    monkeypatch.setenv("MSPASS_HOME", str(REPOSITORY_ROOT))
    monkeypatch.setenv("MONGODB_PORT", "27017")
    monkeypatch.setenv("MONGO_INITDB_ROOT_USERNAME", "root user")
    monkeypatch.setenv("MONGO_INITDB_ROOT_PASSWORD", "p@ss:/ word")
    with (
        mock.patch.object(DBClient, "server_info", return_value={}),
        mock.patch("mspasspy.client.GlobalHistoryManager"),
    ):
        client = Client(scheduler="none")

    assert client._db_client._mspass_db_host == "mspass-db:27017"
    assert client._db_client._mspass_connection_kwargs == {
        "username": "root user",
        "password": "p@ss:/ word",
        "authSource": "admin",
    }


def test_client_uses_compose_credentials_with_an_explicit_database_host(monkeypatch):
    from mspasspy.client import Client
    from mspasspy.db.client import DBClient

    monkeypatch.setenv("MSPASS_HOME", str(REPOSITORY_ROOT))
    monkeypatch.setenv("MONGODB_PORT", "27017")
    monkeypatch.setenv("MONGO_INITDB_ROOT_USERNAME", "root user")
    monkeypatch.setenv("MONGO_INITDB_ROOT_PASSWORD", "p@ss:/ word")
    with (
        mock.patch.object(DBClient, "server_info", return_value={}),
        mock.patch("mspasspy.client.GlobalHistoryManager"),
    ):
        client = Client(database_host="mspass-db", scheduler="none")

    assert client._db_client._mspass_db_host == "mspass-db:27017"
    assert client._db_client._mspass_connection_kwargs == {
        "username": "root user",
        "password": "p@ss:/ word",
        "authSource": "admin",
    }


def test_explicit_database_uri_credentials_take_priority_over_compose_credentials(
    monkeypatch,
):
    from mspasspy.client import Client
    from mspasspy.db.client import DBClient

    monkeypatch.setenv("MSPASS_HOME", str(REPOSITORY_ROOT))
    monkeypatch.setenv("MONGO_INITDB_ROOT_USERNAME", "root user")
    monkeypatch.setenv("MONGO_INITDB_ROOT_PASSWORD", "p@ss:/ word")
    with (
        mock.patch.object(DBClient, "server_info", return_value={}),
        mock.patch("mspasspy.client.GlobalHistoryManager"),
    ):
        client = Client(
            database_host="mongodb://explicit:credential@mspass-db:27017",
            scheduler="none",
        )

    assert client._db_client._mspass_connection_kwargs == {}


def _real_compose_environment(tmp_path, token=None):
    environment = os.environ.copy()
    environment.update(
        {
            "MONGO_INITDB_ROOT_USERNAME": "mspass-compose-test",
            "MONGO_INITDB_ROOT_PASSWORD": "compose-test-p@ss:/ word",
            "PWD": str(tmp_path),
        }
    )
    if token is None:
        environment.pop("JUPYTER_TOKEN", None)
    else:
        environment["JUPYTER_TOKEN"] = token
    return environment


def _run_real_compose(compose_file, project, environment, *arguments, timeout=600):
    return subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(compose_file),
            *arguments,
        ],
        cwd=environment["PWD"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _start_real_compose(compose_file, project, environment):
    result = _run_real_compose(
        compose_file,
        project,
        environment,
        "up",
        "-d",
        "--wait",
        "--wait-timeout",
        "300",
    )
    assert result.returncode == 0, result.stderr


def _stop_real_compose(compose_file, project, environment):
    _run_real_compose(
        compose_file,
        project,
        environment,
        "down",
        "--remove-orphans",
        "--timeout",
        "30",
        timeout=120,
    )


def _real_compose_mongo_service(compose_file):
    if compose_file.name == "docker-compose_sharding.yaml":
        return "mspass-dbmanager"
    return "mspass-db"


def _run_authenticated_mongosh(compose_file, project, environment, expression):
    return _run_real_compose(
        compose_file,
        project,
        environment,
        "exec",
        "-T",
        _real_compose_mongo_service(compose_file),
        "mongosh",
        "--host",
        "127.0.0.1",
        "--port",
        "27017",
        "admin",
        "--quiet",
        "--eval",
        "if (!db.auth(process.env.MONGO_INITDB_ROOT_USERNAME, "
        "process.env.MONGO_INITDB_ROOT_PASSWORD)) { quit(1); }",
        "--eval",
        expression,
    )


def _assert_real_compose_authentication(compose_file, project, environment):
    mongo_service = _real_compose_mongo_service(compose_file)
    unauthenticated = _run_real_compose(
        compose_file,
        project,
        environment,
        "exec",
        "-T",
        mongo_service,
        "mongosh",
        "--host",
        "127.0.0.1",
        "--port",
        "27017",
        "admin",
        "--quiet",
        "--eval",
        "const result = db.adminCommand({listDatabases: 1}); quit(result.ok ? 0 : 13);",
    )
    assert unauthenticated.returncode != 0

    authenticated = _run_authenticated_mongosh(
        compose_file,
        project,
        environment,
        "const result = db.adminCommand({listDatabases: 1}); quit(result.ok ? 0 : 13);",
    )
    assert authenticated.returncode == 0, authenticated.stderr

    client = _run_real_compose(
        compose_file,
        project,
        environment,
        "exec",
        "-T",
        "mspass-frontend",
        "python",
        "-c",
        "from mspasspy.client import Client; "
        "client = Client(scheduler='none'); "
        "assert client.get_database_client().admin.command('ping')['ok'] == 1",
    )
    assert client.returncode == 0, client.stderr


def _real_compose_frontend_token(compose_file, project, environment):
    logs = _run_real_compose(
        compose_file,
        project,
        environment,
        "logs",
        "--no-color",
        "mspass-frontend",
    )
    assert logs.returncode == 0, logs.stderr
    tokens = re.findall(r"[?&]token=([A-Za-z0-9_-]+)", logs.stdout)
    assert tokens, logs.stdout
    return tokens[-1]


@pytest.mark.skipif(
    not RUN_REAL_COMPOSE_TESTS,
    reason="set MSPASS_RUN_COMPOSE_SECURITY_TESTS=1 to start real topologies",
)
@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_real_compose_authentication_restore_and_generated_tokens(
    tmp_path, compose_file
):
    project = "mspass794-" + uuid.uuid4().hex[:10]
    environment = _real_compose_environment(tmp_path)
    try:
        _start_real_compose(compose_file, project, environment)
        _assert_real_compose_authentication(compose_file, project, environment)
        first_token = _real_compose_frontend_token(compose_file, project, environment)
        stored = _run_authenticated_mongosh(
            compose_file,
            project,
            environment,
            'db.getSiblingDB("mspass_issue_794").restore.insertOne({_id: "sentinel"})',
        )
        assert stored.returncode == 0, stored.stderr

        _stop_real_compose(compose_file, project, environment)
        _start_real_compose(compose_file, project, environment)
        _assert_real_compose_authentication(compose_file, project, environment)
        second_token = _real_compose_frontend_token(compose_file, project, environment)
        restored = _run_authenticated_mongosh(
            compose_file,
            project,
            environment,
            'const value = db.getSiblingDB("mspass_issue_794").restore.findOne({_id: "sentinel"}); quit(value ? 0 : 14)',
        )
        assert restored.returncode == 0, restored.stderr

        assert second_token != first_token
    finally:
        _stop_real_compose(compose_file, project, environment)


@pytest.mark.skipif(
    not RUN_REAL_COMPOSE_TESTS,
    reason="set MSPASS_RUN_COMPOSE_SECURITY_TESTS=1 to start real topologies",
)
def test_real_compose_honors_explicit_jupyter_token(tmp_path):
    compose_file = COMPOSE_FILES[0]
    project = "mspass794-" + uuid.uuid4().hex[:10]
    environment = _real_compose_environment(tmp_path, "explicit-compose-token")
    try:
        _start_real_compose(compose_file, project, environment)
        assert (
            _real_compose_frontend_token(compose_file, project, environment)
            == "explicit-compose-token"
        )
    finally:
        _stop_real_compose(compose_file, project, environment)
