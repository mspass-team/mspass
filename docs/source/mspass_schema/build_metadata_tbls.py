"""Generate the CSV tables included by the schema documentation.

The table contents are derived from ``data/yaml/mspass.yaml``.  The small
``build_metadata_tbls.pf`` file only defines documentation-oriented subsets of
that schema.  Every subset key is validated against the YAML schema so stale
tables fail during the Sphinx build instead of rendering ``undefined`` rows.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(os.environ.get("MSPASS_HOME", SCRIPT_DIR.parents[2])).resolve()
SCHEMA_FILE = REPO_ROOT / "data" / "yaml" / "mspass.yaml"
GROUP_FILE = SCRIPT_DIR / "build_metadata_tbls.pf"

HEADER = ("Key", "Type", "Mutable", "Concept")
TYPE_NAMES = {
    "int": "int",
    "integer": "int",
    "double": "double",
    "float": "double",
    "str": "string",
    "string": "string",
    "bool": "boolean",
    "boolean": "boolean",
    "dict": "dict",
    "list": "list",
    "objectid": "ObjectId",
    "bytes": "bytes",
    "byte": "bytes",
    "object": "bytes",
}


@dataclass
class AttributeDefinition:
    """Normalized row data for a single metadata key."""

    key: str
    type_name: str
    mutable: bool
    concept: str = ""
    aliases: set[str] = field(default_factory=set)

    def row(self) -> tuple[str, str, str, str]:
        return (
            self.key,
            self.type_name,
            "Yes" if self.mutable else "No",
            self.concept,
        )


def _load_schema() -> dict:
    with SCHEMA_FILE.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _canonical_type(type_name: str | None) -> str:
    if type_name is None:
        raise ValueError("Schema attribute is missing a type")
    return TYPE_NAMES.get(str(type_name).strip().casefold(), str(type_name))


def _as_alias_set(aliases: object, key: str) -> set[str]:
    if aliases is None:
        return set()
    if isinstance(aliases, str):
        aliases = [aliases]
    return {alias for alias in aliases if alias != key}


def _default_collection_names(database_schema: dict) -> dict[str, str]:
    defaults = {}
    for collection, definition in database_schema.items():
        if "default" in definition:
            defaults[definition["default"]] = collection
    return defaults


def _compile_database_definitions(raw_schema: dict) -> dict[str, dict[str, dict]]:
    """Resolve database references in the same way ``DatabaseSchema`` does."""

    database_schema = raw_schema["Database"]
    compiled = {
        collection: dict(definition["schema"])
        for collection, definition in database_schema.items()
    }

    for collection, attributes in compiled.items():
        for key, attribute in list(attributes.items()):
            if "reference" not in attribute:
                continue
            referenced_key = "_id" if key == f"{attribute['reference']}_id" else key
            reference_collection = database_schema[attribute["reference"]]
            while "base" in reference_collection:
                reference_collection = database_schema[reference_collection["base"]]
                if referenced_key in reference_collection["schema"]:
                    break
            foreign_attribute = reference_collection["schema"][referenced_key]
            attributes[key] = {**foreign_attribute, **attribute}

    return compiled


def _metadata_catalog(raw_schema: dict) -> dict[str, AttributeDefinition]:
    """Return a deterministic catalog of metadata attributes keyed by name."""

    database_definitions = _compile_database_definitions(raw_schema)
    defaults = _default_collection_names(raw_schema["Database"])
    catalog: dict[str, AttributeDefinition] = {}

    for metadata_definition in raw_schema["Metadata"].values():
        for key, raw_attribute in metadata_definition["schema"].items():
            attribute = dict(raw_attribute)
            if "collection" in attribute:
                collection = attribute["collection"]
                lookup_key = key
                if key.startswith(f"{collection}_"):
                    lookup_key = key.replace(f"{collection}_", "", 1)
                collection = defaults.get(collection, collection)
                foreign_attribute = database_definitions[collection][lookup_key]
                attribute = {**foreign_attribute, **attribute}

            aliases = _as_alias_set(attribute.get("aliases"), key)
            if key not in catalog:
                catalog[key] = AttributeDefinition(
                    key=key,
                    type_name=_canonical_type(attribute.get("type")),
                    mutable=not attribute.get("readonly", True),
                    concept=attribute.get("concept", ""),
                    aliases=aliases,
                )
            else:
                catalog[key].aliases.update(aliases)

    return catalog


def _parse_group_tables(group_file: Path) -> dict[str, list[str]]:
    """Parse the small AntelopePf-style ``&Tbl`` file used for table groups."""

    groups: dict[str, list[str]] = {}
    current_group: str | None = None

    with group_file.open(encoding="utf-8") as stream:
        for line_number, raw_line in enumerate(stream, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if current_group is None:
                if not line.endswith("&Tbl{"):
                    raise ValueError(
                        f"{group_file}:{line_number}: expected '<name> &Tbl{{'"
                    )
                current_group = line.split()[0]
                if current_group in groups:
                    raise ValueError(
                        f"{group_file}:{line_number}: duplicate table {current_group}"
                    )
                groups[current_group] = []
            elif line == "}":
                current_group = None
            else:
                groups[current_group].append(line)

    if current_group is not None:
        raise ValueError(f"{group_file}: table {current_group} is missing '}}'")
    return groups


def _write_csv(path: Path, rows: Iterable[tuple[str, ...]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerows(rows)


def _write_attribute_table(
    filename: str, keys: Iterable[str], catalog: dict[str, AttributeDefinition]
) -> None:
    missing = [key for key in keys if key not in catalog]
    if missing:
        raise ValueError(
            f"{GROUP_FILE}: table {filename} contains undefined schema keys: "
            + ", ".join(missing)
        )

    rows = [HEADER]
    rows.extend(catalog[key].row() for key in keys)
    _write_csv(SCRIPT_DIR / filename, rows)


def main() -> None:
    raw_schema = _load_schema()
    catalog = _metadata_catalog(raw_schema)

    all_keys = sorted(catalog)
    _write_attribute_table("all.csv", all_keys, catalog)

    alias_rows = [("Unique Key", "Valid Aliases")]
    for key in all_keys:
        aliases = sorted(catalog[key].aliases)
        if aliases:
            alias_rows.append((key, " : ".join(aliases)))
    _write_csv(SCRIPT_DIR / "aliases.csv", alias_rows)

    for tag, keys in _parse_group_tables(GROUP_FILE).items():
        _write_attribute_table(f"{tag}.csv", keys, catalog)


if __name__ == "__main__":
    main()
