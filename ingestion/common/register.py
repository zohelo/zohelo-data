"""The source register: one file per entry, ``ingestion/register/<id lower>.yaml`` (docs/sources.md, D42).

Each file is one entry (a mapping with ``id``, ``status``, ``family``, ``licence`` and the other research fields);
its name is the entry's id in lower case (``PLS-001`` -> ``pls-001.yaml``). Two pull requests that review or
build different sources therefore never edit the same register file. Readers take the entries from ``entries()``,
in file-name order. Reads files only.
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
FOLDER = Path("ingestion") / "register"  # relative to the repository root


def path(ident: str, root: Path = ROOT) -> Path:
    """The file of register entry `ident` (it need not exist yet)."""
    return root / FOLDER / f"{ident.lower()}.yaml"


def files(root: Path = ROOT) -> list[Path]:
    """The register files, in file-name order."""
    return sorted((root / FOLDER).glob("*.yaml"))


def entries(root: Path = ROOT) -> list[dict]:
    """Every register entry, in file-name order."""
    return [yaml.safe_load(file.read_text(encoding="utf-8")) for file in files(root)]
