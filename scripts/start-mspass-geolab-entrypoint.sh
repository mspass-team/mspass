#!/bin/sh
set -eu

command_name=${1:-}
command_name=${command_name##*/}
case "$command_name" in
    jupyterhub-singleuser)
        exec /usr/sbin/start-mspass-geolab.sh "$@"
        ;;
    jupyter)
        if [ "${2:-}" = "lab" ]; then
            exec /usr/sbin/start-mspass-geolab.sh "$@"
        fi
        ;;
    python|python[0-9]*)
        wrapped_command_name=${2:-}
        wrapped_command_name=${wrapped_command_name##*/}
        if [ "$wrapped_command_name" = "jupyterhub-singleuser" ]; then
            exec /usr/sbin/start-mspass-geolab.sh "$@"
        fi
        ;;
esac

exec "$@"
