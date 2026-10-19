#!/usr/bin/env python3
"""Expire Iceberg snapshots the catalog's own maintenance leaves behind (policy #822, #820).

The R2 Data Catalog setting (ops/catalog_maintenance) keeps 1 snapshot and expires older
ones after 1 hour, but on 19 Oct six tables without new commits since ~14 Oct still held
2–143 snapshots up to 126 h old: the catalog does not expire snapshots of idle tables.
This script applies the same policy to every Bronze / Silver / Gold table: snapshots
older than MAX_AGE that are neither the current snapshot nor a branch/tag head.

Without --apply it only lists them (annotation per run, tables with most snapshots
first). With --apply it removes them from each table's metadata, one commit per table;
data files they leave unreferenced are removed by `ops/orphan_files --delete`.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
from typing import Any

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

LAYERS = ("bronze", "silver", "gold")
MAX_AGE = timedelta(hours=1)  # policy #822: the grace for queries running during a commit


def expirable(table, cutoff_ms: int) -> list[int]:
    """Snapshot ids older than the cutoff, except the current snapshot and branch/tag heads."""
    protected = {ref.snapshot_id for ref in table.metadata.refs.values()}
    current = table.metadata.current_snapshot_id
    return sorted(
        s.snapshot_id
        for s in table.metadata.snapshots
        if s.timestamp_ms < cutoff_ms and s.snapshot_id != current and s.snapshot_id not in protected
    )


def fit(payload: dict[str, Any], rows: list[tuple[str, list]], limit: int = 3800) -> dict[str, Any]:
    """The payload with as many per-table rows as fit in one annotation (list last)."""
    payload["tables"] = {}
    for name, row in rows:
        if len(json.dumps({**payload, "tables": {**payload["tables"], name: row}})) > limit:
            break
        payload["tables"][name] = row
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="expire the listed snapshots")
    args = parser.parse_args(argv)

    cat = catalog()
    cutoff_ms = int((datetime.now(UTC) - MAX_AGE).timestamp() * 1000)
    rows, errors, expired = [], {}, 0
    for layer in LAYERS:
        for identifier in cat.list_tables(layer):
            name = f"{layer}.{identifier[-1]}"
            table = cat.load_table(identifier)
            ids = expirable(table, cutoff_ms)
            if not ids:
                continue
            before = len(table.metadata.snapshots)
            after = before
            if args.apply:
                try:
                    table.maintenance.expire_snapshots().by_ids(ids).commit()
                    after = len(cat.load_table(identifier).metadata.snapshots)
                    expired += before - after
                except Exception as exc:  # report every table, then fail the run
                    errors[name] = repr(exc)[:200]
            rows.append((name, [before, len(ids), after]))
    rows.sort(key=lambda row: (-row[1][1], row[0]))
    payload = {
        "mode": "apply" if args.apply else "dry-run",
        "max_age_hours": MAX_AGE.total_seconds() / 3600,
        "format": "table: [snapshots before, expirable, snapshots after]",
        "tables_with_expirable": len(rows),
        "expirable": sum(row[1][1] for row in rows),
        "expired": expired,
        "errors": errors,
    }
    annotate("error" if errors else "notice", "expire snapshots", fit(payload, rows))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
