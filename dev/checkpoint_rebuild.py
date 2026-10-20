"""Checkpoint rebuild of Eurostat and GUS BDL (design #875, owner approval 20 Oct 16:37).

Approved one-off production change, run from this branch in steps:

  build eurostat|gus_bdl   the source's loader into the new tables: Bronze bronze.eurostat_observations /
                           bronze.bdl_observations, Silver silver.<table>_new (beside the old tables,
                           which keep serving). Resumable: a rerun continues where the last stopped.
  compare                  completeness of the new tables (metadata) and, per entity, new Bronze rows
                           against the old Bronze table; the owner gets these numbers before the swap.
  swap                     drop the old tables (Eurostat Bronze WITHOUT purge: the new table reuses its
                           files) and rename silver.<table>_new to its final name.
"""

from __future__ import annotations

import argparse
import contextlib
import json

from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

NEW = {"eurostat": ("silver", "eurostat_observations_new"), "gus_bdl": ("silver", "bdl_observations_new")}
OLD_BRONZE = {"eurostat": ("bronze", "eurostat_full_observations"), "gus_bdl": ("bronze", "bdl_subgroup_observations")}
NEW_BRONZE = {"eurostat": ("bronze", "eurostat_observations"), "gus_bdl": ("bronze", "bdl_observations")}
ENTITY = {"eurostat": "dataset_id", "gus_bdl": "subgroup_id"}
# Old tables replaced by the checkpoint: (table, purge). Eurostat Bronze files are reused.
RETIRE = {
    "eurostat": [
        (("bronze", "eurostat_full_observations"), False),
        (("silver", "eurostat_observations"), True),
        (("silver", "eurostat_dataset_versions"), True),
        (("silver", "eurostat_file_index"), True),
        (("silver", "eurostat_loaded_files"), True),
    ],
    "gus_bdl": [
        (("bronze", "bdl_subgroup_observations"), True),
        (("silver", "bdl_observations"), True),
    ],
}


def catalog_compaction_off() -> dict:
    """Catalog-wide compaction off before any build (#875): it must not rewrite the old Eurostat
    Bronze files the new table re-registers. Same setting as ops/catalog_maintenance --apply."""
    import requests

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{req('CLOUDFLARE_ACCOUNT_ID')}/r2-catalog/"
        f"{req('R2_LAKEHOUSE_BUCKET')}/maintenance-configs"
    )
    headers = {"Authorization": f"Bearer {req('R2_DATA_CATALOG_TOKEN')}"}
    body = {
        "compaction": {"state": "disabled"},
        "snapshot_expiration": {"state": "enabled", "min_snapshots_to_keep": 1, "max_snapshot_age": "1h"},
    }
    response = requests.post(url, headers=headers, json=body, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"catalog maintenance: {response.status_code} {response.text[:300]}")
    return response.json().get("result") or {}


def build(source: str, minutes: float, workers: int) -> int:
    annotate("notice", "catalog compaction off", catalog_compaction_off())
    et.configure(OLD_BRONZE[source])  # the old table too, until it is dropped
    if source == "eurostat":
        from ingestion.to_bronze.eurostat import entities
    else:
        from ingestion.to_bronze.gus_bdl import entities
    args = ["--silver-table", ".".join(NEW[source]), "--minutes", str(minutes), "--workers", str(workers)]
    return entities.main([*args, "--approved", "875"])


def check(source: str) -> dict:
    """Completeness of the new tables against Landing (metadata only)."""
    cat, client = catalog(), s3()
    if source == "eurostat":
        from ingestion.to_bronze.eurostat import entities as m

        p = m.plan(cat, client, m.model_digest(), None, NEW[source], m.BRONZE, m.REJECTED, create=False)
    else:
        from ingestion.to_bronze.gus_bdl import entities as m

        p = m.plan(cat, client, {"bronze": m.BRONZE, "silver": NEW[source], "rejected": m.REJECTED}, None, create=False)
    return et.completeness(p["expected"], p["bronze"], p["silver"], p["expected_silver"])


def compare(source: str) -> int:
    cat, client = catalog(), s3()
    if source == "eurostat":
        from ingestion.to_bronze.eurostat import entities as m

        p = m.plan(cat, client, m.model_digest(), None, NEW[source], m.BRONZE, m.REJECTED, create=False)
    else:
        from ingestion.to_bronze.gus_bdl import entities as m

        p = m.plan(cat, client, {"bronze": m.BRONZE, "silver": NEW[source], "rejected": m.REJECTED}, None, create=False)
    new_b, new_s = p["bronze"], p["silver"]
    report = et.completeness(p["expected"], new_b, new_s, p["expected_silver"])
    report["rejected"] = sorted(p["rejected"])[:30]
    old = et.state(cat.load_table(OLD_BRONZE[source]), ENTITY[source])
    both = sorted(set(old) & set(new_b))
    differ = [e for e in both if old[e]["rows"] != new_b[e]["rows"]]
    report["old_bronze_entities"] = len(old)
    report["old_bronze_rows"] = sum(v["rows"] for v in old.values())
    report["new_bronze_gb"] = round(sum(v["bytes"] for v in new_b.values()) / 1e9, 2)
    report["new_silver_gb"] = round(sum(v["bytes"] for v in new_s.values()) / 1e9, 2)
    report["entities_rows_differ_from_old"] = len(differ)
    report["differ_sample"] = [
        {
            "entity": e,
            "old": old[e]["rows"],
            "new": new_b[e]["rows"],
            "same_file": bool(old[e]["recipes"] & new_b[e]["recipes"]),
        }
        for e in differ[:10]
    ]
    report["only_in_old"] = sorted(set(old) - set(new_b))[:20]
    annotate("notice", f"checkpoint compare {source}", report)
    print(json.dumps(report, default=str))
    return 0


def swap(source: str) -> int:
    """Refuses unless the new Silver table exists and is complete enough; never runs twice."""
    cat = catalog()
    if not cat.table_exists(NEW[source]):
        annotate("error", f"checkpoint swap {source}", {"refused": f"{'.'.join(NEW[source])} does not exist"})
        return 1
    new_rows = et.state(cat.load_table(NEW[source]), ENTITY[source])
    report = check(source)
    if report["bronze_pct"] < 99 or report["silver_pct"] < 99 or report["entities_with_several_files"]:
        annotate("error", f"checkpoint swap {source}", {"refused": "new tables below 99% complete", **report})
        return 1
    done = []
    for table_id, purge in RETIRE[source]:
        with contextlib.suppress(NoSuchTableError):
            if purge:
                cat.purge_table(table_id)
            else:
                cat.drop_table(table_id)
            done.append({"table": ".".join(table_id), "purge": purge})
    final = (NEW[source][0], NEW[source][1].removesuffix("_new"))
    cat.rename_table(NEW[source], final)
    et.configure(final)
    out = {"retired": done, "renamed": f"{'.'.join(NEW[source])} -> {'.'.join(final)}", "entities": len(new_rows)}
    annotate("notice", f"checkpoint swap {source}", out)
    print(json.dumps(out))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("step", choices=("build", "compare", "swap"))
    parser.add_argument("source", choices=("eurostat", "gus_bdl"))
    parser.add_argument("--minutes", type=float, default=320)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.step == "build":
        return build(args.source, args.minutes, args.workers)
    if args.step == "compare":
        return compare(args.source)
    return swap(args.source)


if __name__ == "__main__":
    raise SystemExit(main())
