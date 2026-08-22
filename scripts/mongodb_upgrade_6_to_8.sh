#!/usr/bin/env bash

set -Eeuo pipefail

MONGO_6_IMAGE=${MONGO_6_IMAGE:-mongo:6.0.26-jammy}
MONGO_7_IMAGE=${MONGO_7_IMAGE:-mongo:7.0.29-jammy}
MONGO_8_IMAGE=${MONGO_8_IMAGE:-mongo:8.0.29}
MSPASS_MONGO_UPGRADE_PORT=${MSPASS_MONGO_UPGRADE_PORT:-27029}
MSPASS_MONGO_UPGRADE_ROOT=${MSPASS_MONGO_UPGRADE_ROOT:-}
MSPASS_MONGO_UPGRADE_FAIL_AFTER=${MSPASS_MONGO_UPGRADE_FAIL_AFTER:-}

if [[ -z "$MSPASS_MONGO_UPGRADE_ROOT" ]]; then
    echo "MSPASS_MONGO_UPGRADE_ROOT must name a disposable directory" >&2
    exit 2
fi

case "$MSPASS_MONGO_UPGRADE_FAIL_AFTER" in
    ""|6.0|7.0) ;;
    *)
        echo "MSPASS_MONGO_UPGRADE_FAIL_AFTER must be empty, 6.0, or 7.0" >&2
        exit 2
        ;;
esac

case "$MSPASS_MONGO_UPGRADE_ROOT" in
    /|/home|/root|"$HOME")
        echo "refusing unsafe MSPASS_MONGO_UPGRADE_ROOT=$MSPASS_MONGO_UPGRADE_ROOT" >&2
        exit 2
        ;;
esac

if [[ -e "$MSPASS_MONGO_UPGRADE_ROOT" ]]; then
    echo "MSPASS_MONGO_UPGRADE_ROOT already exists; use a fresh disposable path" >&2
    exit 2
fi

command -v docker >/dev/null
mkdir -p "$MSPASS_MONGO_UPGRADE_ROOT/db"

container_name="mspass-mongodb-upgrade-$$"
current_stage=""

cleanup() {
    if docker inspect "$container_name" >/dev/null 2>&1; then
        stop_stage || true
    fi
    docker rm -f "$container_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_mongo() {
    local attempts=60
    while ((attempts > 0)); do
        if docker exec "$container_name" mongosh --quiet --eval \
            'quit(db.adminCommand({ping: 1}).ok === 1 ? 0 : 1)' >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempts--))
    done
    echo "MongoDB ${current_stage} did not become ready" >&2
    return 1
}

start_stage() {
    local image=$1
    current_stage=$2
    docker run --detach --rm \
        --name "$container_name" \
        --publish "127.0.0.1:${MSPASS_MONGO_UPGRADE_PORT}:27017" \
        --volume "$MSPASS_MONGO_UPGRADE_ROOT/db:/data/db" \
        "$image" --bind_ip_all >/dev/null
    wait_for_mongo
}

stop_stage() {
    docker exec "$container_name" mongosh --quiet --eval \
        'db.getSiblingDB("admin").shutdownServer()' >/dev/null 2>&1 || true
    for _ in {1..30}; do
        if ! docker inspect "$container_name" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "MongoDB ${current_stage} did not stop cleanly" >&2
    return 1
}

assert_binary_version() {
    local expected=$1
    docker exec "$container_name" mongosh --quiet --eval \
        "const v=db.version(); if (v !== '${expected}') throw new Error('expected MongoDB ${expected}, got '+v)"
}

assert_fcv() {
    local expected=$1
    docker exec "$container_name" mongosh --quiet --eval \
        "const v=db.adminCommand({getParameter:1,featureCompatibilityVersion:1}).featureCompatibilityVersion.version; if (v !== '${expected}') throw new Error('expected FCV ${expected}, got '+v)"
}

set_fcv() {
    local target=$1
    if [[ "$target" == "6.0" ]]; then
        docker exec "$container_name" mongosh --quiet --eval \
            "const r=db.adminCommand({setFeatureCompatibilityVersion:'${target}'}); if (r.ok !== 1) throw new Error(JSON.stringify(r))"
    else
        docker exec "$container_name" mongosh --quiet --eval \
            "const r=db.adminCommand({setFeatureCompatibilityVersion:'${target}',confirm:true}); if (r.ok !== 1) throw new Error(JSON.stringify(r))"
    fi
}

verify_read_write() {
    local stage=$1
    docker exec "$container_name" mongosh --quiet --eval \
        "const c=db.getSiblingDB('mspass_upgrade_fixture').state; c.updateOne({_id:'${stage}'},{\$set:{stage:'${stage}',verified:true}},{upsert:true}); if (c.countDocuments({verified:true}) !== $(case "$stage" in 6.0) echo 1;; 7.0) echo 2;; 8.0) echo 3;; esac)) throw new Error('read/write verification failed at ${stage}')"
}

fail_if_requested() {
    local stage=$1
    if [[ "$MSPASS_MONGO_UPGRADE_FAIL_AFTER" == "$stage" ]]; then
        printf '%s\n' "$stage" > "$MSPASS_MONGO_UPGRADE_ROOT/last_completed_stage"
        echo "forced failure after MongoDB ${stage}; no later binary or FCV was started" >&2
        exit 70
    fi
}

start_stage "$MONGO_6_IMAGE" 6.0
assert_binary_version 6.0.26
set_fcv 6.0
assert_fcv 6.0
verify_read_write 6.0
printf '%s\n' 6.0 > "$MSPASS_MONGO_UPGRADE_ROOT/last_completed_stage"
stop_stage
fail_if_requested 6.0

start_stage "$MONGO_7_IMAGE" 7.0
assert_binary_version 7.0.29
assert_fcv 6.0
set_fcv 7.0
stop_stage
start_stage "$MONGO_7_IMAGE" 7.0
assert_binary_version 7.0.29
assert_fcv 7.0
verify_read_write 7.0
printf '%s\n' 7.0 > "$MSPASS_MONGO_UPGRADE_ROOT/last_completed_stage"
stop_stage
fail_if_requested 7.0

start_stage "$MONGO_8_IMAGE" 8.0
assert_binary_version 8.0.29
assert_fcv 7.0
set_fcv 8.0
stop_stage
start_stage "$MONGO_8_IMAGE" 8.0
assert_binary_version 8.0.29
assert_fcv 8.0
verify_read_write 8.0
printf '%s\n' 8.0 > "$MSPASS_MONGO_UPGRADE_ROOT/last_completed_stage"
stop_stage

echo "MongoDB migration fixture completed: 6.0/FCV6 -> 7.0/FCV7 -> 8.0.29/FCV8"
