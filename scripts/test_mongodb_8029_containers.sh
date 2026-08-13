#!/usr/bin/env bash

set -Eeuo pipefail

MSPASS_MONGODB_TEST_IMAGE=${MSPASS_MONGODB_TEST_IMAGE:-}
if [[ -z "$MSPASS_MONGODB_TEST_IMAGE" ]]; then
    echo "MSPASS_MONGODB_TEST_IMAGE is required" >&2
    exit 2
fi

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
standalone_name="mspass-mongodb-8029-standalone-$$"
standalone_volume="${standalone_name}-home"
compose_project="mspass-mongodb-8029-sharded-$$"
fixture_root=$(mktemp -d)
compose_override="$fixture_root/compose.override.yaml"
test_image="mspass/mspass:mongodb-8029-contract-$$"

cleanup() {
    docker rm -f "$standalone_name" >/dev/null 2>&1 || true
    docker volume rm "$standalone_volume" >/dev/null 2>&1 || true
    if [[ -f "$compose_override" ]]; then
        docker compose \
            --project-name "$compose_project" \
            --file "$repository_root/data/yaml/docker-compose_sharding.yaml" \
            --file "$compose_override" \
            down --volumes --remove-orphans >/dev/null 2>&1 || true
    fi
    docker image rm "$test_image" >/dev/null 2>&1 || true
    rm -rf "$fixture_root" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_ping() {
    local container=$1
    local attempts=120
    while ((attempts > 0)); do
        if docker exec "$container" mongosh --quiet --eval \
            'quit(db.adminCommand({ping:1}).ok === 1 ? 0 : 1)' >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempts--))
    done
    echo "MongoDB did not become ready in $container" >&2
    return 1
}

docker volume create "$standalone_volume" >/dev/null
docker run --detach --rm \
    --name "$standalone_name" \
    --volume "$standalone_volume:/home" \
    --env MSPASS_ROLE=db \
    --env MSPASS_SLEEP_TIME=1 \
    "$MSPASS_MONGODB_TEST_IMAGE" >/dev/null
wait_for_ping "$standalone_name"
docker cp "$repository_root/scripts/verify_mongodb_runtime.py" \
    "$standalone_name:/tmp/verify_mongodb_runtime.py"
docker exec "$standalone_name" \
    python /tmp/verify_mongodb_runtime.py mongodb://127.0.0.1:27017
docker rm -f "$standalone_name" >/dev/null

# Give the image under test a process-unique tag and override only the three
# database services used by this contract.
docker tag "$MSPASS_MONGODB_TEST_IMAGE" "$test_image"
cat > "$compose_override" <<EOF
services:
  mspass-dbmanager:
    image: $test_image
    volumes:
      - mongodb_contract_dbmanager:/home
    environment:
      MSPASS_SLEEP_TIME: "1"
  mspass-shard-0:
    image: $test_image
    volumes:
      - mongodb_contract_shard0:/home
    environment:
      MSPASS_SLEEP_TIME: "1"
  mspass-shard-1:
    image: $test_image
    volumes:
      - mongodb_contract_shard1:/home
    environment:
      MSPASS_SLEEP_TIME: "1"
volumes:
  mongodb_contract_dbmanager:
  mongodb_contract_shard0:
  mongodb_contract_shard1:
EOF
(
    cd "$fixture_root"
    docker compose \
        --project-name "$compose_project" \
        --file "$repository_root/data/yaml/docker-compose_sharding.yaml" \
        --file "$compose_override" \
        up --detach --wait mspass-dbmanager
)
dbmanager_id=$(docker compose \
    --project-name "$compose_project" \
    --file "$repository_root/data/yaml/docker-compose_sharding.yaml" \
    --file "$compose_override" \
    ps --quiet mspass-dbmanager)
if [[ -z "$dbmanager_id" ]]; then
    echo "sharded MongoDB dbmanager did not start" >&2
    exit 1
fi
docker cp "$repository_root/scripts/verify_mongodb_runtime.py" \
    "$dbmanager_id:/tmp/verify_mongodb_runtime.py"
docker exec "$dbmanager_id" \
    python /tmp/verify_mongodb_runtime.py mongodb://127.0.0.1:27017
