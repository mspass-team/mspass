#!/usr/bin/env python3
"""Prepare the GitHub Pages deployment tree for versioned MsPASS docs."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path


VERSION_DIR_RE = re.compile(r"^v\d+(?:\.\d+)*(?:[A-Za-z0-9._+-]*)?$")


def _copy_tree(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)


def _version_sort_key(version: str) -> tuple[tuple[int, ...], str]:
    numeric_parts = []
    for part in version.lstrip("v").split("."):
        match = re.match(r"(\d+)", part)
        numeric_parts.append(int(match.group(1)) if match else 0)
    return tuple(numeric_parts), version


def _deployed_versions() -> list[str]:
    """Return version directories that already exist on the GitHub Pages branch."""
    result = subprocess.run(
        ["git", "ls-tree", "-d", "--name-only", "origin/gh-pages"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []

    versions = {
        name.strip()
        for name in result.stdout.splitlines()
        if VERSION_DIR_RE.match(name.strip())
    }
    return sorted(versions, key=_version_sort_key, reverse=True)


def _with_trailing_slash(url: str) -> str:
    return url.rstrip("/") + "/"


def _switcher_entries(site_url: str, current_version: str) -> list[dict[str, object]]:
    site_url = site_url.rstrip("/")
    versions = sorted(set(_deployed_versions()), key=_version_sort_key, reverse=True)
    if current_version.startswith("v") and current_version not in versions:
        versions.append(current_version)
        versions = sorted(set(versions), key=_version_sort_key, reverse=True)

    entries: list[dict[str, object]] = [
        {
            "name": "latest",
            "version": "latest",
            "url": _with_trailing_slash(site_url),
        }
    ]
    for index, tag in enumerate(versions):
        entry: dict[str, object] = {
            "name": tag,
            "version": tag,
            "url": _with_trailing_slash(f"{site_url}/{tag}"),
        }
        if index == 0:
            entry["preferred"] = True
        entries.append(entry)
    return entries


def prepare_docs(html_dir: Path, site_dir: Path, site_url: str, version_match: str) -> None:
    if not html_dir.is_dir():
        raise FileNotFoundError(f"HTML build directory does not exist: {html_dir}")

    if site_dir.exists():
        shutil.rmtree(site_dir)
    site_dir.mkdir(parents=True)
    (site_dir / ".nojekyll").touch()

    if version_match == "latest":
        for item in html_dir.iterdir():
            destination = site_dir / item.name
            if item.is_dir():
                shutil.copytree(item, destination)
            else:
                shutil.copy2(item, destination)
        _copy_tree(html_dir, site_dir / "latest")
    else:
        _copy_tree(html_dir, site_dir / version_match)

    switcher = _switcher_entries(site_url, version_match)
    (site_dir / "switcher.json").write_text(
        json.dumps(switcher, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--html-dir", required=True, type=Path)
    parser.add_argument("--site-dir", required=True, type=Path)
    parser.add_argument("--site-url", required=True)
    parser.add_argument("--version-match", required=True)
    args = parser.parse_args()

    prepare_docs(
        html_dir=args.html_dir,
        site_dir=args.site_dir,
        site_url=args.site_url,
        version_match=args.version_match,
    )


if __name__ == "__main__":
    main()
