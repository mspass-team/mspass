#!/usr/bin/env python3
"""Prepare the GitHub Pages deployment tree for versioned MsPASS docs."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path


def _copy_tree(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)


def _git_tags() -> list[str]:
    result = subprocess.run(
        ["git", "tag", "--list", "v[0-9]*", "--sort=-v:refname"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [tag.strip() for tag in result.stdout.splitlines() if tag.strip()]


def _with_trailing_slash(url: str) -> str:
    return url.rstrip("/") + "/"


def _switcher_entries(site_url: str, current_version: str) -> list[dict[str, object]]:
    site_url = site_url.rstrip("/")
    tags = _git_tags()
    if current_version.startswith("v") and current_version not in tags:
        tags.insert(0, current_version)

    entries: list[dict[str, object]] = [
        {
            "name": "latest",
            "version": "latest",
            "url": _with_trailing_slash(site_url),
        }
    ]
    for index, tag in enumerate(tags):
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
