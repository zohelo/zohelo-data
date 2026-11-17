"""The source register: ingestion/register.yaml and the per-provider files ingestion/register/*.yaml.

Epic data/ingestion/register-restructure splits the register per provider. First step: every reader loads
both, and new entries go to ingestion/register/<provider>.yaml (family K demo sources: demo.yaml). The
entries of register.yaml move later. Every file is a mapping with `sources:` holding entries with the same
fields (docs/sources.md); an id appears once across all files (ops/repo_check.py).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
MAIN = "ingestion/register.yaml"
FOLDER = "ingestion/register"


def _read(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else None


def paths(root: Path = ROOT) -> list[Path]:
    """register.yaml first, then the folder's files in name order."""
    return [root / MAIN, *sorted((root / FOLDER).glob("*.yaml"))]


def by_file(root: Path = ROOT, read: Callable[[Path], Any] = _read) -> list[tuple[str, list[dict]]]:
    """(repository-relative file, its entries) for every register file that has entries."""
    found = []
    for path in paths(root):
        data = read(path)
        items = data.get("sources") if isinstance(data, dict) else None
        if items:
            found.append((path.relative_to(root).as_posix(), list(items)))
    return found


def entries(root: Path = ROOT, read: Callable[[Path], Any] = _read) -> list[dict]:
    """Every register entry across the files, in file order."""
    return [entry for _, items in by_file(root, read) for entry in items]
