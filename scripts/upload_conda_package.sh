#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 PACKAGE_OR_DIRECTORY PYTHON_VERSION ANACONDA_COMMAND" >&2
  exit 2
fi

package_source=$1
python_version=$2
anaconda_command=$3
: "${ANACONDA_API_TOKEN:?ANACONDA_API_TOKEN is required}"

packages=()
if [[ -f "$package_source" ]]; then
  if [[ "$package_source" == *.conda || "$package_source" == *.tar.bz2 ]]; then
    packages=("$package_source")
  else
    echo "package source is not a conda package: $package_source" >&2
    exit 1
  fi
elif [[ -d "$package_source" ]]; then
  while IFS= read -r -d '' package; do
    packages+=("$package")
  done < <(
    find "$package_source" -type f \
      \( -name '*.conda' -o -name '*.tar.bz2' \) -print0
  )
else
  echo "package source does not exist: $package_source" >&2
  exit 1
fi

if [[ ${#packages[@]} -ne 1 ]]; then
  echo "expected exactly one conda package, found ${#packages[@]}" >&2
  exit 1
fi

labels=(--label "py${python_version}")
if [[ "$python_version" == "3.13" ]]; then
  labels=(--label main "${labels[@]}")
fi

"$anaconda_command" upload "${packages[0]}" \
  "${labels[@]}" --force
