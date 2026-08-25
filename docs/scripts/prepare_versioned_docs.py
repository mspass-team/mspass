#!/usr/bin/env python3
"""Prepare the GitHub Pages deployment tree for versioned MsPASS docs."""

from __future__ import annotations

import argparse
import io
import json
import re
import shutil
import subprocess
import tarfile
from pathlib import Path

VERSION_DIR_RE = re.compile(r"^v\d+(?:\.\d+)*(?:[A-Za-z0-9._+-]*)?$")
SITE_CONTROL_FILES = {"CNAME"}


def _copy_tree(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)


def _fetch_deployed_site() -> bool:
    """Fetch the current Pages branch, returning false before its first deploy."""
    remote_branch = subprocess.run(
        [
            "git",
            "ls-remote",
            "--exit-code",
            "--refs",
            "origin",
            "refs/heads/gh-pages",
        ],
        capture_output=True,
    )
    if remote_branch.returncode == 2:
        subprocess.run(
            ["git", "update-ref", "-d", "refs/remotes/origin/gh-pages"],
            check=True,
            capture_output=True,
        )
        return False
    remote_branch.check_returncode()
    subprocess.run(
        [
            "git",
            "fetch",
            "--force",
            "--depth=1",
            "origin",
            "gh-pages:refs/remotes/origin/gh-pages",
        ],
        check=True,
        capture_output=True,
    )
    return True


def _restore_deployed_site(site_dir: Path) -> None:
    """Copy the current GitHub Pages tree into ``site_dir`` when it exists."""
    deployed_site_exists = _fetch_deployed_site()
    site_dir.mkdir(parents=True)
    if not deployed_site_exists:
        return

    tree = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", "origin/gh-pages"],
        check=True,
        capture_output=True,
    )
    if not tree.stdout:
        return

    result = subprocess.run(
        ["git", "archive", "--format=tar", "origin/gh-pages"],
        check=True,
        capture_output=True,
    )
    if not result.stdout.strip(b"\0"):
        return

    root = site_dir.resolve()
    with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:") as archive:
        for member in archive.getmembers():
            destination = (site_dir / member.name).resolve()
            if destination != root and root not in destination.parents:
                raise ValueError(f"Unsafe path in gh-pages archive: {member.name}")
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                destination.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError(f"Cannot read gh-pages file: {member.name}")
                with destination.open("wb") as output:
                    shutil.copyfileobj(source, output)
            else:
                raise ValueError(
                    f"Unsupported entry in gh-pages archive: {member.name}"
                )


def _clear_latest(site_dir: Path) -> None:
    """Remove the deployed latest alias while preserving version directories."""
    for item in site_dir.iterdir():
        if item.is_dir() and VERSION_DIR_RE.match(item.name):
            continue
        if item.name in SITE_CONTROL_FILES:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


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


def prepare_docs(
    html_dir: Path, site_dir: Path, site_url: str, version_match: str
) -> None:
    if not html_dir.is_dir():
        raise FileNotFoundError(f"HTML build directory does not exist: {html_dir}")

    if site_dir.exists():
        shutil.rmtree(site_dir)
    _restore_deployed_site(site_dir)

    if version_match == "latest":
        _clear_latest(site_dir)
    (site_dir / ".nojekyll").touch()

    if version_match == "latest":
        for item in html_dir.iterdir():
            destination = site_dir / item.name
            if item.name in SITE_CONTROL_FILES and destination.exists():
                continue
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
