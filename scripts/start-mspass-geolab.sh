#!/bin/sh
set -eu

export NB_HOME="${NB_HOME:-/home/jovyan}"
export HOME="${HOME:-$NB_HOME}"
export MSPASS_WORK_DIR="${MSPASS_WORK_DIR:-$NB_HOME}"
export MSPASS_WORKDIR="${MSPASS_WORKDIR:-$MSPASS_WORK_DIR}"
export MSPASS_DB_DIR="${MSPASS_DB_DIR:-$NB_HOME/db}"
export MSPASS_LOG_DIR="${MSPASS_LOG_DIR:-$NB_HOME/logs}"
export MSPASS_WORKER_DIR="${MSPASS_WORKER_DIR:-$NB_HOME/work}"
export MONGO_DATA_DIR="${MONGO_DATA_DIR:-$MSPASS_DB_DIR/data}"
export MONGO_LOG="${MONGO_LOG:-$MSPASS_LOG_DIR/mongo_log}"

export MONGODB_PORT="${MONGODB_PORT:-27017}"
export MSPASS_ENABLE_LOCAL_DASK="${MSPASS_ENABLE_LOCAL_DASK:-true}"
export MSPASS_SCHEDULER="${MSPASS_SCHEDULER:-dask}"
export MSPASS_SCHEDULER_ADDRESS="${MSPASS_SCHEDULER_ADDRESS:-127.0.0.1}"
export MSPASS_DB_ADDRESS="${MSPASS_DB_ADDRESS:-127.0.0.1}"
export DASK_SCHEDULER_PORT="${DASK_SCHEDULER_PORT:-8786}"
export MSPASS_STARTUP_TIMEOUT_SECONDS="${MSPASS_STARTUP_TIMEOUT_SECONDS:-120}"
export MSPASS_STARTUP_POLL_SECONDS="${MSPASS_STARTUP_POLL_SECONDS:-2}"

# GeoLab currently provides up to 4 CPUs.  Use multiple single-threaded
# worker processes by default to avoid Python GIL contention.
export MSPASS_DASK_WORKER_COUNT="${MSPASS_DASK_WORKER_COUNT:-4}"
export MSPASS_DASK_WORKER_THREADS="${MSPASS_DASK_WORKER_THREADS:-1}"
export MSPASS_DASK_WORKER_MEMORY_LIMIT="${MSPASS_DASK_WORKER_MEMORY_LIMIT:-0}"

LOCAL_DASK_ENABLED=false
case "${MSPASS_ENABLE_LOCAL_DASK}" in
    true|TRUE|True|1|yes|YES|Yes)
        LOCAL_DASK_ENABLED=true
        ;;
    false|FALSE|False|0|no|NO|No)
        if [ "${MSPASS_SCHEDULER}" = "dask" ] && \
            [ "${MSPASS_SCHEDULER_ADDRESS}" = "127.0.0.1" ]; then
            export MSPASS_SCHEDULER=none
        fi
        ;;
esac

mkdir -p "$MONGO_DATA_DIR" "$MSPASS_LOG_DIR" "$MSPASS_WORKER_DIR"

if [ "${MSPASS_RESET_MONGO_DB:-false}" = "true" ]; then
    rm -rf "$MONGO_DATA_DIR"
    mkdir -p "$MONGO_DATA_DIR"
fi

MONGO_PID=""
DASK_SCHEDULER_PID=""
DASK_WORKER_PIDS=""
FRONTEND_PID=""

terminate_and_wait() {
    child_pid=$1
    if [ -z "$child_pid" ]; then
        return 0
    fi
    if kill -0 "$child_pid" 2>/dev/null; then
        kill "$child_pid" 2>/dev/null || true
    fi
    wait "$child_pid" 2>/dev/null || true
}

cleanup() {
    status=$?
    trap - INT TERM EXIT

    terminate_and_wait "$FRONTEND_PID"

    for worker_pid in $DASK_WORKER_PIDS; do
        terminate_and_wait "$worker_pid"
    done

    terminate_and_wait "$DASK_SCHEDULER_PID"
    terminate_and_wait "$MONGO_PID"

    exit "$status"
}

trap 'exit 130' INT
trap 'exit 143' TERM
trap cleanup EXIT

child_exited() {
    child_state="$(ps -p "$1" -o stat= 2>/dev/null || true)"
    case "$child_state" in
        ""|Z*) return 0 ;;
        *) return 1 ;;
    esac
}

owned_child_exited() {
    if [ -n "$MONGO_PID" ] && child_exited "$MONGO_PID"; then
        echo "Fatal: mongod exited during startup." >&2
        echo "Last MongoDB log lines:" >&2
        tail -200 "$MONGO_LOG" >&2 || true
        return 0
    fi
    if [ -n "$DASK_SCHEDULER_PID" ] && child_exited "$DASK_SCHEDULER_PID"; then
        echo "Fatal: Dask scheduler exited during startup." >&2
        echo "Last Dask scheduler log lines:" >&2
        tail -200 "$MSPASS_LOG_DIR/dask-scheduler.log" >&2 || true
        return 0
    fi
    return 1
}

build_dask_endpoint() {
    scheduler_address=$1
    scheduler_port=$2
    case "$scheduler_address" in
        *://*) scheduler_endpoint=$scheduler_address ;;
        *) scheduler_endpoint="tcp://$scheduler_address" ;;
    esac
    scheduler_host_port=${scheduler_endpoint#*://}
    case "$scheduler_host_port" in
        \[*\]) scheduler_endpoint="${scheduler_endpoint}:$scheduler_port" ;;
        \[*\]:*|*:*) ;;
        *) scheduler_endpoint="${scheduler_endpoint}:$scheduler_port" ;;
    esac
    printf '%s' "$scheduler_endpoint"
}

mongo_is_ready() {
    mongosh --host "$MSPASS_DB_ADDRESS" --port "$MONGODB_PORT" --quiet \
        --eval 'db.adminCommand({ping: 1}).ok' >/dev/null 2>&1
}

dask_is_ready() {
    DASK_SCHEDULER_ENDPOINT=$1
    export DASK_SCHEDULER_ENDPOINT
    python -c '
import os
import distributed

client = distributed.Client(os.environ["DASK_SCHEDULER_ENDPOINT"], timeout="2s")
try:
    client.scheduler_info()
finally:
    client.close()
' >/dev/null 2>&1
}

startup_started="$(date +%s)"
startup_deadline=$((startup_started + MSPASS_STARTUP_TIMEOUT_SECONDS))

if [ "${MSPASS_SKIP_LOCAL_MONGO:-false}" != "true" ]; then
    if ! command -v mongod >/dev/null 2>&1; then
        echo "Fatal: mongod is not available in the GeoLab image." >&2
        exit 1
    fi

    mongod \
        --port "$MONGODB_PORT" \
        --dbpath "$MONGO_DATA_DIR" \
        --logpath "$MONGO_LOG" \
        --bind_ip_all &
    MONGO_PID=$!
fi

if [ "$LOCAL_DASK_ENABLED" = "true" ]; then
    export MSPASS_SCHEDULER=dask
    dask scheduler --host "$MSPASS_SCHEDULER_ADDRESS" \
        --port "$DASK_SCHEDULER_PORT" \
        > "$MSPASS_LOG_DIR/dask-scheduler.log" 2>&1 &
    DASK_SCHEDULER_PID=$!
fi

DASK_SCHEDULER_ENDPOINT="$(
    build_dask_endpoint "$MSPASS_SCHEDULER_ADDRESS" "$DASK_SCHEDULER_PORT"
)"
export DASK_SCHEDULER_ENDPOINT

while :; do
    mongo_ready=false
    dask_ready=false

    if mongo_is_ready; then
        mongo_ready=true
    fi
    if [ "$MSPASS_SCHEDULER" != "dask" ] || \
        dask_is_ready "$DASK_SCHEDULER_ENDPOINT"; then
        dask_ready=true
    fi

    if owned_child_exited; then
        exit 1
    fi
    if [ "$(date +%s)" -ge "$startup_deadline" ]; then
        echo "Fatal: GeoLab services did not become ready within ${MSPASS_STARTUP_TIMEOUT_SECONDS} seconds." >&2
        exit 1
    fi
    if [ "$mongo_ready" = "true" ] && [ "$dask_ready" = "true" ]; then
        break
    fi
    sleep "$MSPASS_STARTUP_POLL_SECONDS"
done

if [ "$LOCAL_DASK_ENABLED" = "true" ]; then
    worker_index=1
    while [ "$worker_index" -le "$MSPASS_DASK_WORKER_COUNT" ]; do
        worker_dir="$MSPASS_WORKER_DIR/worker-${worker_index}"
        mkdir -p "$worker_dir"

        dask worker \
            --nthreads "$MSPASS_DASK_WORKER_THREADS" \
            --memory-limit="$MSPASS_DASK_WORKER_MEMORY_LIMIT" \
            --local-directory "$worker_dir" \
            "$DASK_SCHEDULER_ENDPOINT" \
            > "$MSPASS_LOG_DIR/dask-worker-${worker_index}.log" 2>&1 &
        DASK_WORKER_PIDS="$DASK_WORKER_PIDS $!"
        worker_index=$((worker_index + 1))
    done
fi

cd "$MSPASS_WORKDIR" || {
    echo "Cannot change to MSPASS_WORKDIR: $MSPASS_WORKDIR" >&2
    exit 1
}

"$@" &
FRONTEND_PID=$!

set +e
wait "$FRONTEND_PID"
frontend_status=$?
set -e
FRONTEND_PID=""
exit "$frontend_status"
