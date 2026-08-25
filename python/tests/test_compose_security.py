import json
import os
import shutil
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


def _compose_config(path, auth=False, username=None, password=None):
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker Compose is required to resolve the configuration")
    version = subprocess.run(
        [docker, "compose", "version"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if version.returncode != 0:
        pytest.skip("Docker Compose is not available in this environment")

    env = os.environ.copy()
    for key in (
        "MSPASS_MONGO_AUTH",
        "MONGO_INITDB_ROOT_USERNAME",
        "MONGO_INITDB_ROOT_PASSWORD",
    ):
        env.pop(key, None)
    if auth:
        env["MSPASS_MONGO_AUTH"] = "true"
    if username is not None:
        env["MONGO_INITDB_ROOT_USERNAME"] = username
    if password is not None:
        env["MONGO_INITDB_ROOT_PASSWORD"] = password
    return subprocess.run(
        [docker, "compose", "-f", str(path), "config", "--format", "json"],
        capture_output=True,
        env=env,
        text=True,
    )


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_compose_default_preserves_passwordless_mongodb(compose_file):
    result = _compose_config(compose_file)
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
        assert environment["MSPASS_MONGO_AUTH"] == "false"
        assert environment["MONGO_INITDB_ROOT_USERNAME"] == ""
        assert environment["MONGO_INITDB_ROOT_PASSWORD"] == ""
        healthcheck = " ".join(service["healthcheck"]["test"])
        assert "MSPASS_MONGO_AUTH" in healthcheck
        assert "db.auth(process.env.MONGO_INITDB_ROOT_USERNAME" in healthcheck

    frontend = config["services"]["mspass-frontend"]["environment"]
    assert frontend["MSPASS_MONGO_AUTH"] == "false"
    assert frontend["MSPASS_JUPYTER_PWD"] == "mspass"


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_compose_supports_explicit_mongodb_authentication(compose_file):
    result = _compose_config(
        compose_file,
        auth=True,
        username="root user",
        password="p@ss:/ word",
    )

    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)
    mongo_roles = ("db", "dbmanager", "shard")
    for service in config["services"].values():
        environment = service.get("environment", {})
        if (
            environment.get("MSPASS_ROLE") in mongo_roles
            or environment.get("MSPASS_ROLE") == "frontend"
        ):
            assert environment["MSPASS_MONGO_AUTH"] == "true"
            assert environment["MONGO_INITDB_ROOT_USERNAME"] == "root user"
            assert environment["MONGO_INITDB_ROOT_PASSWORD"] == "p@ss:/ word"


def test_start_script_uses_authentication_without_exposing_plaintext_password():
    script = START_MSPASS.read_text()

    assert "MONGO_SERVER_SECURITY_ARGS=(--auth)" in script
    assert "process.env.MONGO_INITDB_ROOT_PASSWORD" in script
    assert "${MONGO_INITDB_ROOT_PASSWORD}" not in script
    assert '"${MONGO_CLIENT_AUTH_ARGS[@]}"' in script
    assert '--password "$MONGO_INITDB_ROOT_PASSWORD"' not in script
    assert 'quote(os.environ["MONGO_INITDB_ROOT_PASSWORD"]' in script


def test_compose_guides_describe_default_and_opt_in_authentication():
    combined = "\n".join(guide.read_text() for guide in COMPOSE_GUIDES)
    assert "MSPASS_MONGO_AUTH" in combined
    assert "MONGO_INITDB_ROOT_USERNAME" in combined
    assert "MONGO_INITDB_ROOT_PASSWORD" in combined
    for guide in COMPOSE_GUIDES:
        text = guide.read_text()
        assert "``mspass``" in text


def _write_executable(path, content):
    path.write_text(content)
    path.chmod(0o755)


def _require_client_dependencies():
    pytest.importorskip("pymongo")
    pytest.importorskip("mspasspy.ccore")


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


def test_standalone_default_does_not_enable_authentication(tmp_path):
    result, commands, user_state = _run_start_script(
        tmp_path,
        "db",
        {"MSPASS_MONGO_AUTH": "false"},
    )

    assert result.returncode == 0, result.stderr
    assert not user_state.exists()
    mongod_command = next(
        command for command in commands if command.startswith("mongod")
    )
    assert "--auth" not in mongod_command
    assert "--keyFile" not in mongod_command
    assert all("db.createUser" not in command for command in commands)


@pytest.mark.parametrize("token", (None, "chosen-token"))
def test_frontend_preserves_jupyter_password_and_optional_token(tmp_path, token):
    extra_environment = {
        "MSPASS_JUPYTER_PWD": "mspass",
        "MSPASS_MONGO_AUTH": "false",
    }
    if token is not None:
        extra_environment["MSPASS_JUPYTER_TOKEN"] = token
    result, commands, _ = _run_start_script(tmp_path, "frontend", extra_environment)

    assert result.returncode == 0, result.stderr
    jupyter_command = next(
        command for command in commands if command.startswith("jupyter")
    )
    if token is None:
        assert "NotebookApp.token" not in jupyter_command
    else:
        assert " <--NotebookApp.token=chosen-token>" in jupyter_command
    assert "NotebookApp.password" in jupyter_command


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


def test_spark_frontend_preserves_passwordless_mongodb_arguments_by_default(tmp_path):
    result, commands, _ = _run_start_script(
        tmp_path,
        "frontend",
        {
            "MSPASS_DB_ADDRESS": "mspass-db",
            "MSPASS_MONGO_AUTH": "false",
            "MSPASS_SCHEDULER": "spark",
            "MSPASS_SCHEDULER_ADDRESS": "scheduler",
            "SPARK_MASTER_PORT": "7077",
        },
    )

    assert result.returncode == 0, result.stderr
    pyspark_command = next(
        command for command in commands if command.startswith("pyspark")
    )
    assert " <--properties-file>" not in pyspark_command
    assert (
        "spark.mongodb.input.uri=mongodb://mspass-db:27017/test.misc" in pyspark_command
    )
    assert (
        "spark.mongodb.output.uri=mongodb://mspass-db:27017/test.misc"
        in pyspark_command
    )


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
    _require_client_dependencies()
    from mspasspy.client import Client
    from mspasspy.db.client import DBClient

    monkeypatch.setenv("MSPASS_DB_ADDRESS", "mspass-db")
    monkeypatch.setenv("MSPASS_HOME", str(REPOSITORY_ROOT))
    monkeypatch.setenv("MONGODB_PORT", "27017")
    monkeypatch.setenv("MSPASS_MONGO_AUTH", "true")
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
    _require_client_dependencies()
    from mspasspy.client import Client
    from mspasspy.db.client import DBClient

    monkeypatch.setenv("MSPASS_HOME", str(REPOSITORY_ROOT))
    monkeypatch.setenv("MONGODB_PORT", "27017")
    monkeypatch.setenv("MSPASS_MONGO_AUTH", "true")
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
    _require_client_dependencies()
    from mspasspy.client import Client
    from mspasspy.db.client import DBClient

    monkeypatch.setenv("MSPASS_HOME", str(REPOSITORY_ROOT))
    monkeypatch.setenv("MSPASS_MONGO_AUTH", "true")
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


def test_client_ignores_compose_credentials_by_default(monkeypatch):
    _require_client_dependencies()
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
    assert client._db_client._mspass_connection_kwargs == {}


def _real_compose_environment(tmp_path):
    environment = os.environ.copy()
    environment.update(
        {
            "MSPASS_MONGO_AUTH": "true",
            "MONGO_INITDB_ROOT_USERNAME": "mspass-compose-test",
            "MONGO_INITDB_ROOT_PASSWORD": "compose-test-p@ss:/ word",
            "PWD": str(tmp_path),
        }
    )
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


@pytest.mark.skipif(
    not RUN_REAL_COMPOSE_TESTS,
    reason="set MSPASS_RUN_COMPOSE_SECURITY_TESTS=1 to start real topologies",
)
@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_real_compose_authentication_survives_restart(tmp_path, compose_file):
    project = "mspass794-" + uuid.uuid4().hex[:10]
    environment = _real_compose_environment(tmp_path)
    try:
        _start_real_compose(compose_file, project, environment)
        _assert_real_compose_authentication(compose_file, project, environment)
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
        restored = _run_authenticated_mongosh(
            compose_file,
            project,
            environment,
            'const value = db.getSiblingDB("mspass_issue_794").restore.findOne({_id: "sentinel"}); quit(value ? 0 : 14)',
        )
        assert restored.returncode == 0, restored.stderr
    finally:
        _stop_real_compose(compose_file, project, environment)
