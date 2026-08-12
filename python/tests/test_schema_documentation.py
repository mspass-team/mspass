import csv
import importlib.util
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FILES = (
    REPO_ROOT / "data" / "yaml" / "mspass.yaml",
    REPO_ROOT / "data" / "yaml" / "mspass_fdsn.yaml",
    REPO_ROOT / "data" / "yaml" / "mspass_s3.yaml",
)
EXPECTED_CONCEPTS = {
    "DEPMIN": "Minimum amplitude of a signal",
    "DEPMAX": "Maximum amplitude of a signal",
}


def test_depmin_depmax_schema_descriptions():
    for schema_file in SCHEMA_FILES:
        with schema_file.open(encoding="utf-8") as stream:
            schema = yaml.safe_load(stream)
        attributes = schema["Metadata"]["Other"]["schema"]

        assert {
            key: attributes[key]["concept"] for key in EXPECTED_CONCEPTS
        } == EXPECTED_CONCEPTS


def test_generated_depmin_depmax_descriptions(tmp_path, monkeypatch):
    generator_file = (
        REPO_ROOT / "docs" / "source" / "mspass_schema" / "build_metadata_tbls.py"
    )
    spec = importlib.util.spec_from_file_location(
        "mspass_schema_table_generator", generator_file
    )
    generator = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, generator)
    spec.loader.exec_module(generator)
    monkeypatch.setattr(generator, "SCRIPT_DIR", tmp_path)

    generator.main()

    with (tmp_path / "all.csv").open(encoding="utf-8", newline="") as stream:
        rows = {row["key"]: row for row in csv.DictReader(stream)}
    assert {key: rows[key]["concept"] for key in EXPECTED_CONCEPTS} == EXPECTED_CONCEPTS
