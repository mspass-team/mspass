#!/usr/bin/env python3
"""Sync meta.yaml and conda_build.sh from pyproject.toml.

pyproject.toml is the single source of truth for all Python runtime
dependencies.  dependency_map.toml defines:
  - pip→conda name/expansion mappings
    - packages available only via pip (excluded from the conda run: section)
  - conda-only runtime extras (C libraries, etc.)

Usage:
  python scripts/sync_dependencies.py           # same as --check
  python scripts/sync_dependencies.py --update  # update meta.yaml + conda_build.sh in-place
  python scripts/sync_dependencies.py --check   # exit 1 if files are out of sync (used in CI)
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

try:
    import tomllib  # type: ignore[import-not-found]  # Python 3.11+
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "tomllib unavailable. Use Python >= 3.11 or: pip install tomli"
        ) from exc

_BEGIN_MARKER = "# BEGIN_PIP_ONLY_DEPS"
_END_MARKER = "# END_PIP_ONLY_DEPS"


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_pip_dep(raw: str) -> tuple[str, str]:
    """Split a pip requirement into (name_with_extras, version_spec).

    Environment markers (after ';') are stripped.
    Examples:
      "dask[complete]>=2025.9.1"  -> ("dask[complete]", ">=2025.9.1")
      "numpy>=2.2.6"              -> ("numpy", ">=2.2.6")
      "earthscope-sdk"            -> ("earthscope-sdk", "")
    """
    bare = raw.strip().split(";", 1)[0].strip()
    # Name: letters/digits/underscores/hyphens/dots, optional [extras]
    m = re.match(r"^([A-Za-z0-9_.-]+(?:\[[A-Za-z0-9_,\s]+\])?)(.*)?$", bare)
    if not m:
        return bare, ""
    return m.group(1).strip(), (m.group(2) or "").strip()


def _normalize(name: str) -> str:
    """Normalize to conda-style package name (lowercase, underscores→hyphens, no extras)."""
    return name.split("[", 1)[0].lower().replace("_", "-")


# ---------------------------------------------------------------------------
# Conversion logic
# ---------------------------------------------------------------------------

def _to_conda_deps(
    pip_deps: list[str],
    conda_name_map: dict[str, list[str]],
    pip_only: set[str],
) -> list[str]:
    """Convert pyproject runtime deps to conda dep strings, applying the mapping."""
    result: list[str] = []
    for raw in pip_deps:
        name_ext, ver = _parse_pip_dep(raw)
        base = _normalize(name_ext)
        if base in pip_only:
            continue
        # Try full key (with extras) first, then base name
        key = name_ext.lower().replace("_", "-")
        expansion = conda_name_map.get(key) or conda_name_map.get(base)
        if expansion:
            for i, cname in enumerate(expansion):
                result.append(f"{cname}{ver}" if (i == 0 and ver) else cname)
        else:
            result.append(f"{base}{ver}" if ver else base)
    return result


def _pip_only_install_list(pip_deps: list[str], pip_only: set[str]) -> list[str]:
    """Return the pip install spec for each pip-only dep (name + version if given)."""
    out: list[str] = []
    for raw in pip_deps:
        name_ext, ver = _parse_pip_dep(raw)
        if _normalize(name_ext) in pip_only:
            out.append(f"{name_ext}{ver}" if ver else name_ext)
    return out


# ---------------------------------------------------------------------------
# File generation
# ---------------------------------------------------------------------------

def _make_run_section(conda_deps: list[str], conda_extras: list[str]) -> str:
    """Build the meta.yaml '  run:' block text."""
    items = ["    - python {{ PYTHON_VERSION }}"]
    items += [f"    - {d}" for d in conda_deps]
    items += [f"    - {d}" for d in conda_extras]
    return "  run:\n" + "\n".join(items) + "\n"


def _update_meta_run(meta_text: str, conda_deps: list[str], conda_extras: list[str]) -> str:
    new_block = _make_run_section(conda_deps, conda_extras)
    updated, n = re.subn(r"  run:\n(?:    [^\n]*\n)+", new_block, meta_text)
    if n == 0:
        raise ValueError("Could not locate 'run:' section in meta.yaml")
    return updated


def _update_conda_build_sh(text: str, pip_only_deps: list[str]) -> str:
    """Insert generated pip-only installs and enforce --no-deps for the package."""
    text = re.sub(
        r"(\$\{PYTHON\}\s+-m\s+pip\s+install\s+\.)(?!\s+--no-deps)(\s+-vv)",
        r"\1 --no-deps\2",
        text,
    )

    if pip_only_deps:
        dep_lines = "\n".join(
            f'PIP_NO_INDEX=False PIP_NO_DEPENDENCIES=True ${{PYTHON}} -m pip install --no-deps "{d}"'
            for d in pip_only_deps
        )
        new_section = f"{_BEGIN_MARKER}\n{dep_lines}\n{_END_MARKER}"
    else:
        new_section = None

    if _BEGIN_MARKER in text:
        pattern = rf"{re.escape(_BEGIN_MARKER)}.*?{re.escape(_END_MARKER)}"
        if new_section:
            return re.sub(pattern, new_section, text, flags=re.DOTALL)
        else:
            # Remove the section and any blank line that preceded it
            return re.sub(r"\n+" + pattern, "", text, flags=re.DOTALL)
    elif new_section:
        return text.rstrip("\n") + "\n\n" + new_section + "\n"
    return text


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--update", action="store_true",
        help="Overwrite meta.yaml and conda_build.sh with generated content",
    )
    mode.add_argument(
        "--check", action="store_true",
        help="Exit 1 if files are out of sync with pyproject.toml (default)",
    )
    parser.add_argument("--pyproject", default="pyproject.toml")
    parser.add_argument("--meta", default="meta.yaml")
    parser.add_argument("--conda-build-script", default="scripts/conda_build.sh")
    parser.add_argument("--map", default="scripts/dependency_map.toml")
    args = parser.parse_args()

    pyproject = tomllib.loads(Path(args.pyproject).read_text(encoding="utf-8"))
    dep_map = tomllib.loads(Path(args.map).read_text(encoding="utf-8"))

    pip_runtime: list[str] = pyproject.get("project", {}).get("dependencies", [])

    pip_only: set[str] = {
        _normalize(n) for n in dep_map.get("pip_only", {}).get("packages", [])
    }
    conda_name_map: dict[str, list[str]] = {
        k.lower().replace("_", "-"): v
        for k, v in dep_map.get("conda_name_map", {}).items()
    }
    conda_extras: list[str] = dep_map.get("conda_runtime_extras", {}).get("packages", [])

    conda_deps = _to_conda_deps(pip_runtime, conda_name_map, pip_only)
    pip_only_installs = _pip_only_install_list(pip_runtime, pip_only)

    meta_text = Path(args.meta).read_text(encoding="utf-8")
    build_text = Path(args.conda_build_script).read_text(encoding="utf-8")

    expected_meta = _update_meta_run(meta_text, conda_deps, conda_extras)
    expected_build = _update_conda_build_sh(build_text, pip_only_installs)

    if args.update:
        Path(args.meta).write_text(expected_meta, encoding="utf-8")
        Path(args.conda_build_script).write_text(expected_build, encoding="utf-8")
        print(
            f"Updated {args.meta}: "
            f"{len(conda_deps)} conda runtime deps, {len(conda_extras)} extras"
        )
        print(
            f"Updated {args.conda_build_script}: "
            f"{len(pip_only_installs)} pip-only dep(s): {pip_only_installs}"
        )
        return 0

    # --check (default when neither flag given)
    errors: list[str] = []
    if meta_text != expected_meta:
        errors.append(args.meta)
    if build_text != expected_build:
        errors.append(args.conda_build_script)

    if errors:
        print("ERROR: the following files are out of sync with pyproject.toml:")
        for e in errors:
            print(f"  {e}")
        print(
            "\nFix by running:\n"
            "  python scripts/sync_dependencies.py --update"
        )
        return 1

    print(
        f"OK: {args.meta} and {args.conda_build_script} "
        "are in sync with pyproject.toml."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
