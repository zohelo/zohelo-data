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
    """Run the compiled test of this branch with its metrics CTE limited to the positive KPIs' tables."""
    import re

    done = _dbt(["compile", "--select", "metrics_value_range"])
    files = list((PROJECT / "target" / "compiled").rglob("metrics_value_range.sql"))
    if done.returncode or not files:
        annotate("error", "compile failed", {"rc": done.returncode, "out": (done.stdout + done.stderr)[-1500:]})
        return 1
    sql = files[0].read_text()
    union = " union all ".join(
        f"select concept_key, source_system, measure_key, geography_key, date_key, value from r2.gold.metrics_{c}"
        for c in POSITIVE
    )
    limited, count = re.subn(r"metrics as \(.*?\n\)\n", f"metrics as ({union})\n", sql, count=1, flags=re.S)
    if count != 1:
        annotate("error", "metrics CTE not found", sql[-1500:])
        return 1
    rows = _show("test on positive KPIs", limited)
    if rows is None:
        return 1
    annotate(
        "warning" if rows else "notice",
        "compiled metrics_value_range (this branch), metrics CTE = the 5 positive KPIs' tables",
        {"failing_rows": len(rows), "first": rows[:20], "where": sql[sql.rindex("where") :][:900]},
    )
    total = _show("rows read", f"select count(*) as n from ({union}) as t", 1)
    annotate("notice", "rows read", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
