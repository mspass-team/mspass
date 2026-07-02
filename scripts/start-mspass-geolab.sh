#!/bin/sh
set -eu

export HOME=/home/jovyan
export NB_HOME=/home/jovyan
export MSPASS_WORK_DIR=/home/jovyan
export MSPASS_WORKDIR=/home/jovyan
export MSPASS_LOG_DIR="${MSPASS_LOG_DIR:-/home/jovyan/logs}"
export MSPASS_WORKER_DIR="${MSPASS_WORKER_DIR:-/home/jovyan/work}"

export MONGODB_PORT="${MONGODB_PORT:-27017}"
export MSPASS_SCHEDULER="${MSPASS_SCHEDULER:-none}"
export DASK_SCHEDULER_PORT="${DASK_SCHEDULER_PORT:-8786}"

case "${MSPASS_PERSISTENT_MONGO:-false}:${MSPASS_DB_PATH:-}" in
    true:*|TRUE:*|True:*|1:*|yes:*|YES:*|Yes:*|*:home)
        export MSPASS_DB_DIR="${MSPASS_DB_DIR:-/home/jovyan/db}"
        ;;
    *)
        export MSPASS_MONGO_ROOT="${MSPASS_MONGO_ROOT:-/tmp/mspass-mongo}"
        export MSPASS_DB_DIR="${MSPASS_DB_DIR:-$MSPASS_MONGO_ROOT/db}"
        ;;
esac

export MONGO_DATA_DIR="${MONGO_DATA_DIR:-$MSPASS_DB_DIR/data}"
export MONGO_LOG="${MONGO_LOG:-$MSPASS_LOG_DIR/mongo_log}"

mkdir -p "$MONGO_DATA_DIR" "$MSPASS_LOG_DIR" "$MSPASS_WORKER_DIR"

if [ "${MSPASS_RESET_MONGO_DB:-false}" = "true" ]; then
    rm -rf "$MONGO_DATA_DIR"
    mkdir -p "$MONGO_DATA_DIR"
fi

# Health checks use localhost, but Dask Gateway workers need a pod-routable
# MongoDB address when the MongoDBWorker plugin serializes the DB connection.
if [ -z "${MSPASS_DB_ADDRESS:-}" ]; then
    db_address="$(hostname -i 2>/dev/null | awk '{print $1}')"
    if [ -z "$db_address" ]; then
        db_address="$(hostname)"
    fi
    export MSPASS_DB_ADDRESS="$db_address"
fi

MONGO_PID=""
DASK_SCHEDULER_PID=""
DASK_WORKER_PID=""
FRONTEND_PID=""

cleanup() {
    status=$?
    trap - INT TERM EXIT

    if [ -n "$FRONTEND_PID" ] && kill -0 "$FRONTEND_PID" 2>/dev/null; then
        kill "$FRONTEND_PID" 2>/dev/null || true
        wait "$FRONTEND_PID" 2>/dev/null || true
    fi

    if [ -n "$DASK_WORKER_PID" ] && kill -0 "$DASK_WORKER_PID" 2>/dev/null; then
        kill "$DASK_WORKER_PID" 2>/dev/null || true
        wait "$DASK_WORKER_PID" 2>/dev/null || true
    fi

    if [ -n "$DASK_SCHEDULER_PID" ] && kill -0 "$DASK_SCHEDULER_PID" 2>/dev/null; then
        kill "$DASK_SCHEDULER_PID" 2>/dev/null || true
        wait "$DASK_SCHEDULER_PID" 2>/dev/null || true
    fi

    if [ -n "$MONGO_PID" ] && kill -0 "$MONGO_PID" 2>/dev/null; then
        mongosh --host 127.0.0.1 --port "$MONGODB_PORT" --quiet \
            --eval 'db.getSiblingDB("admin").shutdownServer({force: true})' \
            >/dev/null 2>&1 || true
        wait "$MONGO_PID" 2>/dev/null || true
    fi

    exit "$status"
}

trap cleanup INT TERM EXIT

mongo_process_exited() {
    mongo_state="$(ps -p "$MONGO_PID" -o stat= 2>/dev/null || true)"
    case "$mongo_state" in
        ""|Z*) return 0 ;;
        *) return 1 ;;
    esac
}

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

    mongo_ready=false
    timeout="${MSPASS_MONGO_STARTUP_TIMEOUT:-30}"
    i=0
    while [ "$i" -lt "$timeout" ]; do
        if mongosh --host 127.0.0.1 --port "$MONGODB_PORT" --quiet \
            --eval 'db.adminCommand({ping: 1}).ok' >/dev/null 2>&1; then
            mongo_ready=true
            break
        fi

        if mongo_process_exited; then
            echo "Fatal: mongod exited during startup." >&2
            echo "Last MongoDB log lines:" >&2
            tail -200 "$MONGO_LOG" >&2 || true
            exit 1
        fi

        i=$((i + 1))
        sleep 1
    done

    if [ "$mongo_ready" != "true" ]; then
        echo "Fatal: mongod did not become ready before timeout." >&2
        echo "Last MongoDB log lines:" >&2
        tail -200 "$MONGO_LOG" >&2 || true
        exit 1
    fi
fi

case "${MSPASS_ENABLE_LOCAL_DASK:-false}" in
    true|TRUE|True|1|yes|YES|Yes)
        export MSPASS_SCHEDULER=dask
        export MSPASS_SCHEDULER_ADDRESS="${MSPASS_SCHEDULER_ADDRESS:-127.0.0.1}"
        dask scheduler --port "$DASK_SCHEDULER_PORT" \
            > "$MSPASS_LOG_DIR/dask-scheduler.log" 2>&1 &
        DASK_SCHEDULER_PID=$!
        sleep 5
        dask worker \
            --memory-limit="${MSPASS_DASK_WORKER_MEMORY_LIMIT:-0}" \
            --local-directory "$MSPASS_WORKER_DIR" \
            "tcp://${MSPASS_SCHEDULER_ADDRESS}:${DASK_SCHEDULER_PORT}" \
            > "$MSPASS_LOG_DIR/dask-worker.log" 2>&1 &
        DASK_WORKER_PID=$!
        ;;
esac

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
