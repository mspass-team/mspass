#!/bin/sh
set -eu

export HOME=/home/jovyan
export NB_HOME=/home/jovyan
export MSPASS_WORK_DIR=/home/jovyan
export MSPASS_WORKDIR=/home/jovyan
export MSPASS_DB_DIR=/home/jovyan/db
export MSPASS_LOG_DIR=/home/jovyan/logs
export MSPASS_WORKER_DIR=/home/jovyan/work

export MONGODB_PORT="${MONGODB_PORT:-27017}"
export MSPASS_DB_ADDRESS="${MSPASS_DB_ADDRESS:-127.0.0.1}"
export MSPASS_SCHEDULER="${MSPASS_SCHEDULER:-none}"
export DASK_SCHEDULER_PORT="${DASK_SCHEDULER_PORT:-8786}"

mkdir -p "$MSPASS_DB_DIR/data" "$MSPASS_LOG_DIR" "$MSPASS_WORKER_DIR"

if command -v mongod >/dev/null 2>&1; then
    mongod \
        --port "$MONGODB_PORT" \
        --dbpath "$MSPASS_DB_DIR/data" \
        --logpath "$MSPASS_LOG_DIR/mongo_log" \
        --bind_ip_all &
else
    echo "Warning: mongod is not available; MsPASS local database startup skipped." >&2
fi

case "${MSPASS_ENABLE_LOCAL_DASK:-false}" in
    true|TRUE|True|1|yes|YES|Yes)
        export MSPASS_SCHEDULER=dask
        export MSPASS_SCHEDULER_ADDRESS="${MSPASS_SCHEDULER_ADDRESS:-127.0.0.1}"
        dask scheduler --port "$DASK_SCHEDULER_PORT" \
            > "$MSPASS_LOG_DIR/dask-scheduler.log" 2>&1 &
        sleep 5
        dask worker \
            --memory-limit="${MSPASS_DASK_WORKER_MEMORY_LIMIT:-0}" \
            --local-directory "$MSPASS_WORKER_DIR" \
            "tcp://${MSPASS_SCHEDULER_ADDRESS}:${DASK_SCHEDULER_PORT}" \
            > "$MSPASS_LOG_DIR/dask-worker.log" 2>&1 &
        ;;
esac

cd "$MSPASS_WORKDIR" || {
    echo "Cannot change to MSPASS_WORKDIR: $MSPASS_WORKDIR" >&2
    exit 1
}

exec "$@"
