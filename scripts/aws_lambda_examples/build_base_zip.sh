#!/usr/bin/env bash
set -euo pipefail

example_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
output=${1:-"${example_dir}/base.zip"}
export_dir=$(mktemp -d)
trap 'rm -rf -- "${export_dir}"' EXIT

docker buildx build \
    --platform linux/amd64 \
    --file "${example_dir}/Dockerfile.bundle" \
    --output "type=local,dest=${export_dir}" \
    "${example_dir}"

install -m 0644 "${export_dir}/base.zip" "${output}"
