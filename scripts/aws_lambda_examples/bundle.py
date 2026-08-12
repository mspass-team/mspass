#!/usr/bin/env python3
"""Build and verify the Python 3.13 Lambda deployment archive."""

import argparse
import importlib
import os
from pathlib import Path
import shutil
import sys
import tempfile
import zipfile

sys.dont_write_bytecode = True
os.environ.setdefault("MPLCONFIGDIR", "/tmp/mspass-lambda-matplotlib")

LAMBDA_UNZIPPED_LIMIT = 250 * 1024 * 1024
BUNDLED_IMPORTS = (
    "certifi",
    "charset_normalizer",
    "contourpy",
    "cycler",
    "dateutil",
    "decorator",
    "fontTools",
    "greenlet",
    "idna",
    "kiwisolver",
    "lxml",
    "matplotlib",
    "numpy",
    "obspy",
    "packaging",
    "PIL",
    "pyparsing",
    "requests",
    "scipy",
    "setuptools",
    "six",
    "sqlalchemy",
    "typing_extensions",
    "urllib3",
    "aws_lambda_func_def",
    "process",
)
FORBIDDEN_ARCHIVE_MARKERS = ("python3.7", "python-37", "py3.7", "cpython-37")


def _within(path, root):
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def prune_asset(asset_root):
    """Remove wheel content that cannot be used at Lambda runtime."""
    for directory in sorted(asset_root.rglob("*"), reverse=True):
        if directory.is_dir() and directory.name in {"test", "tests", "__pycache__"}:
            shutil.rmtree(directory)
    bin_directory = asset_root / "bin"
    if bin_directory.exists():
        shutil.rmtree(bin_directory)
    for path in asset_root.rglob("*"):
        if path.is_file() and path.suffix in {".a", ".c", ".cpp", ".h", ".pyi"}:
            path.unlink()


def verify_imports(asset_root):
    """Import every bundled top-level module and exercise ObsPy native I/O."""
    if sys.version_info[:2] != (3, 13):
        raise RuntimeError("the Lambda bundle must be verified with Python 3.13")

    asset_root = asset_root.resolve()
    importlib.invalidate_caches()
    for module_name in BUNDLED_IMPORTS:
        module = importlib.import_module(module_name)
        module_path = Path(module.__file__).resolve()
        if not _within(module_path, asset_root):
            raise RuntimeError(
                f"{module_name} loaded from {module_path}, outside {asset_root}"
            )

    import numpy as np
    import obspy

    trace = obspy.Trace(data=np.arange(32, dtype=np.int32))
    with tempfile.TemporaryDirectory() as directory:
        waveform_path = Path(directory) / "native.mseed"
        trace.write(waveform_path, format="MSEED")
        restored = obspy.read(waveform_path, format="MSEED")
    if not np.array_equal(restored[0].data, trace.data):
        raise RuntimeError("ObsPy native MiniSEED round trip changed sample data")

    native_files = [
        path
        for path in asset_root.rglob("*")
        if path.is_file() and (path.name.endswith(".so") or ".so." in path.name)
    ]
    if not native_files:
        raise RuntimeError("bundle contains no native libraries")
    for path in native_files:
        with path.open("rb") as stream:
            elf_header = stream.read(20)
        if (
            elf_header[:6] != b"\x7fELF\x02\x01"
            or int.from_bytes(elf_header[18:20], "little") != 62
        ):
            raise RuntimeError(f"bundle native library is not x86_64 ELF: {path}")


def verify_archive(archive_path):
    """Validate ABI tags, content, and Lambda's uncompressed size limit."""
    with zipfile.ZipFile(archive_path) as archive:
        members = archive.infolist()

    names = [member.filename for member in members]
    lowered_names = [name.lower() for name in names]
    for name, lowered in zip(names, lowered_names):
        if name.endswith(".pyc") or any(
            marker in lowered for marker in FORBIDDEN_ARCHIVE_MARKERS
        ):
            raise RuntimeError(f"obsolete Python artifact in bundle: {name}")
        if ".cpython-" in lowered and ".cpython-313-" not in lowered:
            raise RuntimeError(f"non-Python-3.13 extension in bundle: {name}")

    required_files = {"process.py", "aws_lambda_func_def.py"}
    if not required_files.issubset(names):
        raise RuntimeError("bundle is missing the Lambda handler sources")
    for package in ("numpy", "obspy", "scipy"):
        if not any(
            name.startswith(f"{package}/") and name.endswith(".so") for name in names
        ):
            raise RuntimeError(f"bundle has no native extension for {package}")

    uncompressed_size = sum(member.file_size for member in members)
    if uncompressed_size > LAMBDA_UNZIPPED_LIMIT:
        raise RuntimeError(
            f"bundle expands to {uncompressed_size} bytes; "
            f"Lambda allows {LAMBDA_UNZIPPED_LIMIT}"
        )


def write_archive(asset_root, archive_path):
    """Write a deterministic archive from the prepared asset directory."""
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for path in sorted(asset_root.rglob("*")):
            if not path.is_file():
                continue
            relative_path = path.relative_to(asset_root).as_posix()
            info = zipfile.ZipInfo(relative_path, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            mode = 0o755 if ".so" in path.name else 0o644
            info.external_attr = mode << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes(), compresslevel=9)


def build(asset_root, archive_path):
    prune_asset(asset_root)
    verify_imports(asset_root)
    write_archive(asset_root, archive_path)
    verify_archive(archive_path)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("asset_root", type=Path)
    build_parser.add_argument("archive", type=Path)

    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("asset_root", type=Path)
    verify_parser.add_argument("archive", type=Path)

    arguments = parser.parse_args()
    if arguments.command == "build":
        build(arguments.asset_root, arguments.archive)
    else:
        verify_imports(arguments.asset_root)
        verify_archive(arguments.archive)


if __name__ == "__main__":
    main()
