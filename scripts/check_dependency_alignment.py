#!/usr/bin/env python3
"""Check dependency alignment between pyproject.toml and meta.yaml.

This script enforces that runtime Python dependencies declared in [project]
are represented in conda recipe requirements.run, after applying a small set
of pip-to-conda mappings.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

try:
    import tomllib  # type: ignore[import-not-found]  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise SystemExit(
            "tomllib is unavailable. Use Python >= 3.11 or install tomli."
        ) from exc


def normalize_name(requirement: str) -> str:
    req = requirement.strip()
    req = req.split(";", 1)[0].strip()
    req = re.split(r"[<>=!~]", req, maxsplit=1)[0].strip()
    req = req.split("[", 1)[0].strip()
    return req.lower().replace("_", "-")


def map_pip_requirement(raw_requirement: str) -> set[str]:
    req = raw_requirement.strip()
    req_no_marker = req.split(";", 1)[0].strip()
    if req_no_marker.lower().startswith("dask[complete]"):
        return {"dask", "distributed", "cloudpickle"}
    return {normalize_name(req)}


def parse_conda_section(meta_text: str, section_name: str) -> list[str]:
    lines = meta_text.splitlines()
    in_requirements = False
    in_section = False
    deps: list[str] = []

    for line in lines:
        if not in_requirements:
            if re.match(r"^requirements:\s*$", line):
                in_requirements = True
            continue

        if in_section:
            if re.match(r"^\s{4}-\s+", line):
                dep = re.sub(r"^\s{4}-\s+", "", line).strip()
                deps.append(dep)
                continue

            # Leaving the section when indentation drops back to top-level recipe key
            if re.match(r"^\S", line) or re.match(r"^\s{2}\w", line):
                break
            continue

        if re.match(rf"^\s{{2}}{re.escape(section_name)}:\s*$", line):
            in_section = True

    return deps


def extract_setuptools_minimum(requirements: list[str]) -> int | None:
    for req in requirements:
        name = normalize_name(req)
        if name != "setuptools":
            continue
        match = re.search(r">=\s*(\d+)", req)
        if match:
            return int(match.group(1))
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pyproject", default="pyproject.toml")
    parser.add_argument("--meta", default="meta.yaml")
    args = parser.parse_args()

    pyproject_path = Path(args.pyproject)
    meta_path = Path(args.meta)

    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    meta_text = meta_path.read_text(encoding="utf-8")

    pip_runtime = pyproject.get("project", {}).get("dependencies", [])
    if not isinstance(pip_runtime, list):
        print("ERROR: [project].dependencies must be a list", file=sys.stderr)
        return 2

    expected_conda_runtime: set[str] = set()
    for req in pip_runtime:
        expected_conda_runtime |= map_pip_requirement(req)

    conda_run_raw = parse_conda_section(meta_text, "run")
    conda_run = {normalize_name(dep) for dep in conda_run_raw}

    missing_runtime = sorted(expected_conda_runtime - conda_run)

    # Optional guard: ensure conda host setuptools floor is not below build-system floor.
    build_requires = pyproject.get("build-system", {}).get("requires", [])
    if not isinstance(build_requires, list):
        print("ERROR: [build-system].requires must be a list", file=sys.stderr)
        return 2

    host_raw = parse_conda_section(meta_text, "host")
    pip_setuptools_min = extract_setuptools_minimum(build_requires)
    conda_setuptools_min = extract_setuptools_minimum(host_raw)

    has_error = False
    if missing_runtime:
        has_error = True
        print("Dependency alignment check failed.")
        print("Missing conda run dependencies for pip runtime dependencies:")
        for dep in missing_runtime:
            print(f"  - {dep}")

    if pip_setuptools_min is not None and conda_setuptools_min is not None:
        if conda_setuptools_min < pip_setuptools_min:
            has_error = True
            print("Setuptools alignment check failed.")
            print(
                "Conda host setuptools minimum is lower than build-system minimum:"
            )
            print(f"  - build-system requires setuptools>={pip_setuptools_min}")
            print(f"  - conda host uses setuptools>={conda_setuptools_min}")

    if has_error:
        return 1

    print("Dependency alignment check passed.")
    print(f"Checked {len(pip_runtime)} pip runtime dependencies against conda run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
