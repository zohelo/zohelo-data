"""What the warehouse is declared to build: the shared lists of warehouse.yaml plus each source's own lines.

A source declares its Gold tables (`gold:`) and the row ceilings of its full-rebuild models
(`full_rebuild:`) in its own `ingestion/sources/<source>.yaml`, so a new source adds no line to
warehouse.yaml (docs/source-registration.md). warehouse.yaml keeps what belongs to no single source: the
conformed dimensions, facts, seeds and KPI metrics. Every reader of the two lists goes through this module
(warehouse/build.py, ops/repo_check.py, ops/tracker.py, ops/retire_tables.py, ops/platform_inventory.py).
Reads YAML only: no R2, no dbt.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
PLATFORM = ROOT / "warehouse" / "warehouse.yaml"
SOURCES = ROOT / "ingestion" / "sources"


def platform() -> dict[str, Any]:
    return yaml.safe_load(PLATFORM.read_text()) or {}


def sources() -> dict[str, dict[str, Any]]:
    """source_system → its declaration, in file-name order."""
    return {path.stem: yaml.safe_load(path.read_text()) or {} for path in sorted(SOURCES.glob("*.yaml"))}


def gold() -> list[str]:
    """Every Gold table the dbt project builds, plus gold.coverage: warehouse.yaml `gold`, then each source's
    `gold:` list; a table listed in both places counts once."""
    names = list(platform().get("gold") or [])
    for config in sources().values():
        names += [name for name in config.get("gold") or [] if name not in names]
    return names


def full_rebuild() -> dict[str, dict[str, Any]]:
    """model → {rows: ceiling[, exception]} of every full-rebuild dbt model (owner policy #822): warehouse.yaml
    `full_rebuild`, then each source's `full_rebuild:`. A model declared twice keeps the warehouse.yaml entry;
    ops/repo_check.py fails on it (`full_rebuild_twice`)."""
    ceilings = dict(platform().get("full_rebuild") or {})
    for config in sources().values():
        for model, entry in (config.get("full_rebuild") or {}).items():
            ceilings.setdefault(model, entry)
    return ceilings


def full_rebuild_twice() -> list[str]:
    """Models whose ceiling is declared in more than one file, as `model (file, file)`."""
    seen: dict[str, list[str]] = {model: ["warehouse.yaml"] for model in platform().get("full_rebuild") or {}}
    for source, config in sources().items():
        for model in config.get("full_rebuild") or {}:
            seen.setdefault(model, []).append(f"{source}.yaml")
    return sorted(f"{model} ({', '.join(files)})" for model, files in seen.items() if len(files) > 1)
