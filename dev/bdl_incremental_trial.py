"""Trial of the incremental silver.bdl_observations (#822 step 2) in the scratch namespace.
Removed before merge. Stages: full build, no-change incremental, repair after a simulated
missing subgroup and stale rows; then the trial tables are dropped."""

import time

import pyarrow as pa
import pyarrow.compute as pc

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse.build import build_once

NS = "zz_dbt_trial"


def counts(cat):
    t = cat.load_table((NS, "bdl_observations"))
    s = t.current_snapshot().summary.additional_properties if t.current_snapshot() else {}
    return {
        k: s.get(k) for k in ("total-records", "total-position-deletes", "total-equality-deletes", "total-data-files")
    }


def stage(name, cat, selectors, variables):
    began = time.monotonic()
    code, _ = build_once(selectors, "trial", variables)
    out = {"stage": name, "exit": code, "minutes": round((time.monotonic() - began) / 60, 1), **counts(cat)}
    annotate("notice" if code == 0 else "error", f"trial {name}", out)
    return code


def main():
    cat = catalog()
    try:
        if stage("full", cat, ["bdl_variables", "bdl_observations"], {"full_refresh_models": ["bdl_observations"]}):
            return 1
        if stage("incremental_nochange", cat, ["bdl_observations"], {}):
            return 1
        t = cat.load_table((NS, "bdl_observations"))
        sample = t.scan(selected_fields=("subgroup_id",), limit=200000).to_arrow()
        groups = pc.unique(sample["subgroup_id"]).to_pylist()
        gone, other = groups[0], groups[-1]
        t.delete(delete_filter=f"subgroup_id == '{gone}'")
        stale = (
            cat.load_table((NS, "bdl_observations")).scan(row_filter=f"subgroup_id == '{other}'", limit=5).to_arrow()
        )
        stale = stale.set_column(
            stale.schema.get_field_index("variables_key"),
            "variables_key",
            pa.array(["trial-stale"] * len(stale), stale.schema.field("variables_key").type),
        )
        t = cat.load_table((NS, "bdl_observations"))
        t.append(stale)
        annotate(
            "notice",
            "trial perturbed",
            {"deleted_subgroup": gone, "stale_rows_in": other, "stale_rows": len(stale), **counts(cat)},
        )
        if stage("incremental_repair", cat, ["bdl_observations"], {}):
            return 1
        return 0
    finally:
        for name in ("bdl_observations", "bdl_variables"):
            try:
                cat.purge_table((NS, name))
            except Exception as exc:
                annotate("warning", "trial cleanup", {"table": name, "error": str(exc)[:300]})


if __name__ == "__main__":
    raise SystemExit(main())
