#!/bin/sh
set -eu

for arg in "$@"; do
    command_name=${arg##*/}
    if [ "$command_name" = "jupyterhub-singleuser" ]; then
        exec /usr/sbin/start-mspass-geolab.sh "$@"
    fi
done

exec "$@"
