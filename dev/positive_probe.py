"""Read-only probe for #2807, one Dev run. Writes nothing to R2.

Compiles warehouse/tests/metrics_value_range.sql (with the exclusive lower bound of this branch) on the prod
target and runs its SELECT with `dbt show` (a query, no materialization):
1. the rows that fail only because of the new rule (value <= 0 for a positive KPI), grouped by KPI x source x
   measure, with the first examples;
2. every other failing row of the test, grouped the same way (failures that exist on main too).
"""

from __future__ import annotations

import json
import os
import subprocess

from ingestion.common.report import annotate
from warehouse.build import PROJECT, TOPICS

POSITIVE = ("population", "population_projection", "gdp_current_prices", "gdp_per_capita", "fx_rate")


def _dbt(args: list[str]) -> subprocess.CompletedProcess:
    base = ["dbt", "--quiet", *args[:1], "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT), "--target", "prod"]
    return subprocess.run(
        [*base, *args[1:], "--vars", json.dumps({"topics": TOPICS})],
        cwd=PROJECT,
        env={**os.environ},
        text=True,
        capture_output=True,
    )


def _show(title: str, sql: str, limit: int = 500) -> list[dict] | None:
    shown = _dbt(["show", "--inline", sql, "--limit", str(limit), "--output", "json"])
    out = shown.stdout.strip()
    try:
        return json.loads(out[out.index("{") :])["show"]
    except (ValueError, KeyError):
        annotate("error", f"{title}: show failed", {"rc": shown.returncode, "out": (out + shown.stderr)[-1500:]})
        return None


def main() -> int:
    # The positive KPIs are read straight from their metrics tables (some metrics models of other KPIs are not
    # built on production yet, so the compiled test as a whole cannot run there).
    union = " union all ".join(
        f"select concept_key, source_system, measure_key, geography_key, date_key, value from r2.gold.metrics_{c}"
        for c in POSITIVE
    )
    grouped = (
        "select concept_key, source_system, measure_key, count(*) filter (where value <= 0) as n, "
        "count(*) filter (where value = 0) as zeros, count(*) as rows_total, min(value) as min_value, "
        "min(date_key) filter (where value <= 0) as first_date, max(date_key) filter (where value <= 0) as last_date, "
        "string_agg(distinct cast(geography_key as varchar), ' ') filter (where value <= 0) as geo "
        f"from ({union}) as t group by all order by n desc, 1, 2, 3"
    )
    rows = _show("1 values <= 0", grouped)
    if rows is None:
        return 1
    bad = [r for r in rows if r["n"]]
    for r in bad:
        r["geo"] = (r.get("geo") or "")[:200]
    annotate(
        "warning" if bad else "notice",
        "1 values <= 0 of positive KPIs on production Gold",
        {"groups": len(rows), "rows": sum(r["rows_total"] for r in rows), "bad_groups": len(bad),
         "bad_rows": sum(r["n"] for r in bad), "bad": bad[:15]},
    )
    annotate("notice", "1a groups read", [[r["concept_key"], r["source_system"], r["measure_key"], r["rows_total"]]
                                           for r in rows][:60])
    if bad:
        examples = _show(
            "1b examples",
            f"select * from ({union}) as t where value <= 0 order by 1, 2, 3, 5 limit 40",
            40,
        )
        if examples is not None:
            annotate("notice", "1b examples", examples[:40])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
