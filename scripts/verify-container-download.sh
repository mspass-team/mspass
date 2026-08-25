#!/bin/sh
set -eu

if [ "$#" -ne 3 ]; then
    echo "usage: $0 sha256|sha512 DIGEST FILE" >&2
    exit 64
fi

case "$1" in
    sha256) checksum_command=sha256sum ;;
    sha512) checksum_command=sha512sum ;;
    *)
        echo "unsupported checksum algorithm: $1" >&2
        exit 64
        ;;
esac

printf '%s  %s\n' "$2" "$3" | "$checksum_command" -c -
