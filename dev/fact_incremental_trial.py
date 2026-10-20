"""Trial of the incremental gold.fact_observation (#870) in the scratch namespace. Removed
before merge. Stages: full build (with wdi_observations, timed), no-change incremental,
repair after all WDI fact rows were deleted and stale rows injected; trial tables dropped."""

import time

import pyarrow as pa

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse.build import build_once

NS = "zz_dbt_trial"
EXCLUDE = ["--exclude", "test_name:relationships"]  # dimensions are not built in the trial


def counts(cat):
    t = cat.load_table((NS, "fact_observation"))
    s = t.current_snapshot().summary.additional_properties if t.current_snapshot() else {}
    stale = t.scan(row_filter="raw_sha256 == 'trial-stale'", selected_fields=("raw_sha256",)).to_arrow().num_rows
    keys = ("total-records", "added-records", "deleted-records", "total-data-files")
    return {**{k: s.get(k) for k in keys}, "stale_rows": stale}


def stage(name, cat, selectors, variables):
    began = time.monotonic()
    code, _ = build_once([*selectors, *EXCLUDE], "trial", variables)
    out = {"stage": name, "exit": code, "minutes": round((time.monotonic() - began) / 60, 1), **counts(cat)}
    annotate("notice" if code == 0 else "error", f"trial {name}", out)
    return code


def main():
    cat = catalog()
    try:
        began = time.monotonic()
        code, _ = build_once(["wdi_observations", *EXCLUDE], "trial", {})
        annotate(
            "notice",
            "trial wdi_observations full build",
            {"exit": code, "minutes": round((time.monotonic() - began) / 60, 1)},
        )
        if code:
            return 1
        if stage("full", cat, ["fact_observation"], {"full_refresh_models": ["fact_observation"]}):
            return 1
        if stage("incremental_nochange", cat, ["fact_observation"], {}):
            return 1
        t = cat.load_table((NS, "fact_observation"))
        stale = t.scan(limit=5).to_arrow()
        stale = stale.set_column(
            stale.schema.get_field_index("raw_sha256"),
            "raw_sha256",
            pa.array(["trial-stale"] * len(stale), stale.schema.field("raw_sha256").type),
        )
        t.delete(delete_filter="source_system == 'world_bank_wdi'")
        cat.load_table((NS, "fact_observation")).append(stale)
        annotate(
            "notice", "trial perturbed", {"deleted": "all WDI rows", "stale_rows_added": len(stale), **counts(cat)}
        )
        if stage("incremental_repair", cat, ["fact_observation"], {}):
            return 1
        return 0
    finally:
        for name in ("fact_observation", "wdi_observations"):
            try:
                cat.purge_table((NS, name))
            except Exception as exc:
                annotate("warning", "trial cleanup", {"table": name, "error": str(exc)[:300]})


if __name__ == "__main__":
    raise SystemExit(main())
