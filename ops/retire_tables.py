#!/usr/bin/env python3
"""Retire catalog tables that today's code does not write (owner decision #835, 16 Oct 2026).

The candidates are exactly the tables the tracker (ops/tracker.py) classifies as legacy,
leftover or old-code copy: tables of retired code, dbt temp/backup tables, tables no source
declares, and the pass-through copies of paused sources. Tables written by today's code
("current") are never candidates. Landing and Archive files are not touched.

Without --apply the run only lists the candidates (count, rows, GB) as annotations. With
--apply it drops them, but only when --expect equals the number of candidates found now, so a
list that changed since the owner's approval is refused. Tables are purged (the catalog may delete
their files); files left behind are removed by `ops/orphan_files --delete --max-share <x>`.

After dropping, every now-empty namespace starting with `zz_dev_` (scratch from trials) is dropped
too; the list run reports which would go. Args: --apply, --expect <n>.
"""

from __future__ import annotations

import argparse
from typing import Any

import yaml

from ingestion.common.bronze_files import retry
from ingestion.common.r2 import catalog, sources
from ingestion.common.report import annotate, step_summary
from ops.platform_inventory import SCRATCH, tables
from ops.tracker import ROOT, classify

RETIRED = ("legacy", "leftover", "copy")


def scratch_namespaces(cat) -> list[str]:
    """Top-level namespaces starting with zz_dev_, sorted."""
    listed = retry(lambda: cat.list_namespaces(), "list namespaces")
    return sorted(ns[0] for ns in listed if ns and ns[0].startswith(SCRATCH))


def drop_empty_scratch(cat, dropping: set[str]) -> list[str]:
    """Drop zz_dev_ namespaces that hold no table other than `dropping` (all of them in a dry run)."""
    dropped = []
    for namespace in scratch_namespaces(cat):
        left = [
            t
            for t in retry(lambda ns=namespace: cat.list_tables(ns), f"list {namespace}")
            if ".".join(t) not in dropping
        ]
        if not left:
            dropped.append(namespace)
    return dropped


def candidates(found: dict[str, dict[str, Any]]) -> list[str]:
    """Catalog tables classified legacy, leftover or old-code copy, sorted."""
    registry = sources()
    platform = yaml.safe_load((ROOT / "warehouse" / "warehouse.yaml").read_text()) or {}
    shared_gold = list(platform.get("gold") or [])
    return sorted(n for n in found if classify(n, registry, shared_gold)[0] in RETIRED)


def report(title: str, names: list[str], found: dict[str, dict[str, Any]]) -> None:
    """Totals in one annotation, the names in as few further annotations as fit."""
    annotate(
        "notice",
        title,
        {
            "tables": len(names),
            "rows": sum(found[n]["rows"] for n in names),
            "gb": round(sum(found[n]["bytes"] for n in names) / 1e9, 2),
        },
    )
    chunk: list[str] = []
    for name in names:
        if len(", ".join([*chunk, name])) > 3600:
            annotate("notice", f"{title}: tables", {"names": chunk})
            chunk = []
        chunk.append(name)
    if chunk:
        annotate("notice", f"{title}: tables", {"names": chunk})
    lines = [f"## {title}", "", "| table | rows | GB |", "|---|---:|---:|"]
    lines += [f"| `{n}` | {found[n]['rows']:,} | {found[n]['bytes'] / 1e9:.2f} |" for n in names]
    step_summary("\n".join(lines) + "\n")


def main(argv=None) -> int:
    """List the candidates; with --apply and a matching --expect, drop them."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expect", type=int, default=None)
    args = parser.parse_args(argv)
    cat = catalog()
    found = tables(cat)
    names = candidates(found)
    report("retire candidates", names, found)
    if not args.apply:
        annotate("notice", "scratch namespaces to drop", {"namespaces": drop_empty_scratch(cat, set(names))})
        return 0
    if args.expect != len(names):
        annotate("error", "retire refused", {"expected": args.expect, "found": len(names)})
        return 1
    dropped, refused = [], 0
    for name in names:
        layer, table = name.split(".", 1)
        try:  # purge asks the catalog to delete the data files too; not every catalog honours it
            retry(lambda layer=layer, table=table: cat.purge_table((layer, table)), f"purge {name}")
        except Exception as exc:
            if "NoSuchTable" in type(exc).__name__:  # a retried purge whose first attempt succeeded
                dropped.append(name)
                continue
            if not refused:  # one warning; annotations per step are limited
                annotate("warning", "purge refused, dropping instead", {"table": name, "error": str(exc)[:300]})
            refused += 1
            retry(lambda layer=layer, table=table: cat.drop_table((layer, table)), f"drop {name}")
        dropped.append(name)
    report("retired", dropped, found)
    scratch = []
    for namespace in drop_empty_scratch(cat, set()):
        retry(lambda namespace=namespace: cat.drop_namespace(namespace), f"drop namespace {namespace}")
        scratch.append(namespace)
    annotate("notice", "scratch namespaces dropped", {"namespaces": scratch})
    annotate("notice", "retire mode", {"purge_requested": len(dropped) - refused, "dropped_only": refused})
    return 0


def run(argv=None) -> int:
    """main() with failures published as an error annotation (job logs are not readable by AI sessions)."""
    try:
        return main(argv)
    except Exception as exc:
        annotate("error", "retire failed", {"error": type(exc).__name__, "detail": str(exc)[:3000]})
        raise


if __name__ == "__main__":
    raise SystemExit(run())
