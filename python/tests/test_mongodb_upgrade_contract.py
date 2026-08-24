import os
import re
import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(
    os.environ.get("MSPASS_TEST_SOURCE_ROOT", Path(__file__).resolve().parents[2])
).resolve()
DOCKERFILE = REPO_ROOT / "Dockerfile"
MIGRATION_SCRIPT = REPO_ROOT / "scripts" / "mongodb_upgrade_6_to_8.sh"
MIGRATION_DOC = (
    REPO_ROOT / "docs" / "source" / "getting_started" / "mongodb_6_to_8_migration.rst"
)
RUNTIME_TEST_SCRIPT = REPO_ROOT / "scripts" / "test_mongodb_8029_containers.sh"
RUNTIME_VERIFIER = REPO_ROOT / "scripts" / "verify_mongodb_runtime.py"


def test_all_ubuntu_2204_image_paths_pin_mongodb_8029():
    dockerfile = DOCKERFILE.read_text()
    package_suffixes = ("", "-server", "-shell", "-mongos", "-tools")

    assert dockerfile.count("MONGO_MAJOR=8.0") == 2
    assert dockerfile.count("MONGO_VERSION=8.0.29") == 2
    assert "MONGO_MAJOR=6.0" not in dockerfile
    assert "MONGO_VERSION=6.0.5" not in dockerfile
    for suffix in package_suffixes:
        assert dockerfile.count(f"${{MONGO_PACKAGE}}{suffix}=$MONGO_VERSION") == 1
        assert dockerfile.count(f"mongodb-org{suffix}=${{MONGO_VERSION}}") == 1
    assert dockerfile.count("server-${MONGO_MAJOR}.asc") == 2
    assert dockerfile.count("arch=$(dpkg --print-architecture)") == 2
    assert "jammy/${MONGO_PACKAGE%-unstable}/$MONGO_MAJOR" in dockerfile
    assert "jammy/mongodb-org/${MONGO_MAJOR}" in dockerfile


def test_migration_harness_has_one_fixed_order_and_stop_boundary():
    source = MIGRATION_SCRIPT.read_text()
    starts = re.findall(
        r'^start_stage "\$MONGO_([678])_IMAGE" ([678]\.0)$', source, re.M
    )
    assert starts == [
        ("6", "6.0"),
        ("7", "7.0"),
        ("7", "7.0"),
        ("8", "8.0"),
        ("8", "8.0"),
    ]
    assert source.index("set_fcv 6.0") < source.index('start_stage "$MONGO_7_IMAGE"')
    assert source.index("set_fcv 7.0") < source.index('start_stage "$MONGO_8_IMAGE"')
    assert source.index(
        "assert_fcv 6.0", source.index('start_stage "$MONGO_7_IMAGE"')
    ) < source.index("set_fcv 7.0")
    assert source.index(
        "assert_fcv 7.0", source.index('start_stage "$MONGO_8_IMAGE"')
    ) < source.index("set_fcv 8.0")
    assert source.index("fail_if_requested 6.0") < source.index(
        'start_stage "$MONGO_7_IMAGE"'
    )
    assert source.index("fail_if_requested 7.0") < source.index(
        'start_stage "$MONGO_8_IMAGE"'
    )
    assert "stop_stage\nfail_if_requested 6.0" in source
    assert "stop_stage\nfail_if_requested 7.0" in source
    assert 'if docker inspect "$container_name"' in source
    assert "stop_stage || true" in source
    assert "mongo:8.0.29" in source
    assert "confirm:true" in source
    assert 'if [[ "$target" == "6.0" ]]' in source
    assert re.findall(r"^assert_binary_version (\S+)$", source, re.M) == [
        "6.0.26",
        "7.0.29",
        "7.0.29",
        "8.0.29",
        "8.0.29",
    ]
    assert source.count('start_stage "$MONGO_7_IMAGE" 7.0') == 2
    assert source.count('start_stage "$MONGO_8_IMAGE" 8.0') == 2
    assert "trap cleanup EXIT" in source
    assert "trap 'exit 130' INT" in source
    assert "trap 'exit 143' TERM" in source


def test_migration_harness_stops_at_forced_boundaries_and_restarts(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$*" >> "$DOCKER_LOG"\n'
        'if [[ "$1" == inspect ]]; then exit 1; fi\n'
    )
    fake_docker.chmod(0o755)
    base_env = os.environ | {
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "DOCKER_LOG": str(docker_log),
    }

    def run_case(name, fail_after, expected_status, expected_images, marker):
        docker_log.unlink(missing_ok=True)
        root = tmp_path / name
        completed = subprocess.run(
            [str(MIGRATION_SCRIPT)],
            env=base_env
            | {
                "MSPASS_MONGO_UPGRADE_ROOT": str(root),
                "MSPASS_MONGO_UPGRADE_FAIL_AFTER": fail_after,
            },
            capture_output=True,
            text=True,
        )
        assert completed.returncode == expected_status
        run_lines = [
            line
            for line in docker_log.read_text().splitlines()
            if line.startswith("run ")
        ]
        images = [
            next(part for part in line.split() if part.startswith("mongo:"))
            for line in run_lines
        ]
        assert images == expected_images
        stage_events = [
            (
                "run"
                if line.startswith("run ")
                else "stop" if "shutdownServer()" in line else None
            )
            for line in docker_log.read_text().splitlines()
        ]
        assert [event for event in stage_events if event] == [
            event for _ in expected_images for event in ("run", "stop")
        ]
        assert (root / "last_completed_stage").read_text().strip() == marker

    run_case("fail6", "6.0", 70, ["mongo:6.0.26-jammy"], "6.0")
    run_case(
        "fail7",
        "7.0",
        70,
        ["mongo:6.0.26-jammy", "mongo:7.0.29-jammy", "mongo:7.0.29-jammy"],
        "7.0",
    )
    run_case(
        "complete",
        "",
        0,
        [
            "mongo:6.0.26-jammy",
            "mongo:7.0.29-jammy",
            "mongo:7.0.29-jammy",
            "mongo:8.0.29",
            "mongo:8.0.29",
        ],
        "8.0",
    )


def test_migration_harness_validates_before_using_docker(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        '#!/usr/bin/env bash\nprintf \'%s\\n\' "$*" >> "$DOCKER_LOG"\n'
    )
    fake_docker.chmod(0o755)
    env = os.environ | {
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "DOCKER_LOG": str(docker_log),
    }

    missing = subprocess.run(
        [str(MIGRATION_SCRIPT)], env=env, capture_output=True, text=True
    )
    assert missing.returncode == 2
    assert "must name a disposable directory" in missing.stderr
    assert not docker_log.exists()

    existing_root = tmp_path / "existing"
    existing_root.mkdir()
    existing = subprocess.run(
        [str(MIGRATION_SCRIPT)],
        env=env | {"MSPASS_MONGO_UPGRADE_ROOT": str(existing_root)},
        capture_output=True,
        text=True,
    )
    assert existing.returncode == 2
    assert "already exists" in existing.stderr
    assert not docker_log.exists()


def test_migration_documentation_names_every_required_gate():
    documentation = MIGRATION_DOC.read_text()
    for token in (
        "MongoDB 6.0.26 with FCV 6.0",
        "Start MongoDB 7.0.29 at FCV 6.0",
        "Start MongoDB 8.0.29 at FCV 7.0",
        "cleanly restart the same",
        "current stage has stopped cleanly",
        "last_completed_stage",
        "Do not point this rehearsal harness at production data",
    ):
        assert token in documentation


def test_runtime_container_contract_covers_standalone_sharded_and_mspass_paths():
    source = RUNTIME_TEST_SCRIPT.read_text()
    assert "MSPASS_MONGODB_TEST_IMAGE is required" in source
    assert "MSPASS_ROLE=db" in source
    assert "docker-compose_sharding.yaml" in source
    assert "up --detach --wait mspass-dbmanager" in source
    assert source.count("verify_mongodb_runtime.py") >= 4
    assert "mspass/mspass:latest" not in source
    assert "compose.override.yaml" in source
    assert 'docker image rm "$test_image"' in source
    assert 'docker volume rm "$standalone_volume"' in source
    assert source.count("mongodb_contract_") >= 6

    verifier = RUNTIME_VERIFIER.read_text()
    for token in (
        'server_version != "8.0.29"',
        "uuid.uuid4().hex",
        'create_index("value", unique=True)',
        'delete_one({"_id": "record"})',
        "gridfs.GridFS(database)",
        "database.save_data(",
        "database.read_data(",
        "HistoryLogger(database",
    ):
        assert token in verifier


def test_sharded_compose_fixture_remains_parseable():
    fixture = REPO_ROOT / "data" / "yaml" / "docker-compose_sharding.yaml"
    parsed = yaml.safe_load(fixture.read_text())
    assert {"mspass-dbmanager", "mspass-shard-0", "mspass-shard-1"} <= set(
        parsed["services"]
    )


def test_docker_workflow_runs_migration_and_built_image_integrations():
    workflow_path = REPO_ROOT / ".github" / "workflows" / "docker-publish.yml"
    workflow = yaml.safe_load(workflow_path.read_text())

    migration_steps = workflow["jobs"]["mongodb-migration"]["steps"]
    migration_commands = [step.get("run", "") for step in migration_steps]
    assert any(
        "pytest python/tests/test_mongodb_upgrade_contract.py" in command
        for command in migration_commands
    )
    assert any(
        "scripts/mongodb_upgrade_6_to_8.sh" in command
        and "MSPASS_MONGO_UPGRADE_FAIL_AFTER" in command
        and 'test "$status" -eq 70' in command
        and 'test "$status" -eq 0' in command
        for command in migration_commands
    )
    assert workflow["jobs"]["download-spark"]["needs"] == "mongodb-migration"

    runtime_steps = workflow["jobs"]["build-latest"]["steps"]
    build_step = next(
        step
        for step in runtime_steps
        if step["name"] == "Build Docker image (amd64 only)"
    )
    assert build_step["with"]["load"] is True
    assert "mspass/mspass:mongodb-contract" in build_step["with"]["tags"]
    integration_step = next(
        step
        for step in runtime_steps
        if step["name"] == "Test standalone and sharded MongoDB 8.0.29"
    )
    assert integration_step["run"] == "scripts/test_mongodb_8029_containers.sh"
    assert (
        integration_step["env"]["MSPASS_MONGODB_TEST_IMAGE"]
        == "mspass/mspass:mongodb-contract"
    )
