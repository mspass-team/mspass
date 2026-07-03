#!/bin/sh
set -eu

export HOME=/home/jovyan
export NB_HOME=/home/jovyan
export MSPASS_WORK_DIR=/home/jovyan
export MSPASS_WORKDIR=/home/jovyan
export MSPASS_DB_DIR="${MSPASS_DB_DIR:-/home/jovyan/db}"
export MSPASS_LOG_DIR="${MSPASS_LOG_DIR:-/home/jovyan/logs}"
export MSPASS_WORKER_DIR="${MSPASS_WORKER_DIR:-/home/jovyan/work}"
export MONGO_DATA_DIR="${MONGO_DATA_DIR:-$MSPASS_DB_DIR/data}"
export MONGO_LOG="${MONGO_LOG:-$MSPASS_LOG_DIR/mongo_log}"

export MONGODB_PORT="${MONGODB_PORT:-27017}"
export MSPASS_ENABLE_LOCAL_DASK="${MSPASS_ENABLE_LOCAL_DASK:-true}"
export MSPASS_SCHEDULER="${MSPASS_SCHEDULER:-dask}"
export MSPASS_SCHEDULER_ADDRESS="${MSPASS_SCHEDULER_ADDRESS:-127.0.0.1}"
export MSPASS_DB_ADDRESS="${MSPASS_DB_ADDRESS:-127.0.0.1}"
export DASK_SCHEDULER_PORT="${DASK_SCHEDULER_PORT:-8786}"

# GeoLab currently provides up to 4 CPUs.  Use multiple single-threaded
# worker processes by default to avoid Python GIL contention.
export MSPASS_DASK_WORKER_COUNT="${MSPASS_DASK_WORKER_COUNT:-4}"
export MSPASS_DASK_WORKER_THREADS="${MSPASS_DASK_WORKER_THREADS:-1}"
export MSPASS_DASK_WORKER_MEMORY_LIMIT="${MSPASS_DASK_WORKER_MEMORY_LIMIT:-0}"

case "${MSPASS_ENABLE_LOCAL_DASK}" in
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

cleanup() {
    status=$?
    trap - INT TERM EXIT

    if [ -n "$FRONTEND_PID" ] && kill -0 "$FRONTEND_PID" 2>/dev/null; then
        kill "$FRONTEND_PID" 2>/dev/null || true
        wait "$FRONTEND_PID" 2>/dev/null || true
    fi

    for worker_pid in $DASK_WORKER_PIDS; do
        if kill -0 "$worker_pid" 2>/dev/null; then
            kill "$worker_pid" 2>/dev/null || true
            wait "$worker_pid" 2>/dev/null || true
        fi
    done

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
        dask scheduler --host 127.0.0.1 --port "$DASK_SCHEDULER_PORT" \
            > "$MSPASS_LOG_DIR/dask-scheduler.log" 2>&1 &
        DASK_SCHEDULER_PID=$!

        sleep 5

        worker_index=1
        while [ "$worker_index" -le "$MSPASS_DASK_WORKER_COUNT" ]; do
            worker_dir="$MSPASS_WORKER_DIR/worker-${worker_index}"
            mkdir -p "$worker_dir"

            dask worker \
                --nthreads "$MSPASS_DASK_WORKER_THREADS" \
                --memory-limit="$MSPASS_DASK_WORKER_MEMORY_LIMIT" \
                --local-directory "$worker_dir" \
                "tcp://127.0.0.1:${DASK_SCHEDULER_PORT}" \
                > "$MSPASS_LOG_DIR/dask-worker-${worker_index}.log" 2>&1 &
            DASK_WORKER_PIDS="$DASK_WORKER_PIDS $!"
            worker_index=$((worker_index + 1))
        done
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
