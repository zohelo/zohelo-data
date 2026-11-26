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
    done = _dbt(["compile", "--select", "metrics_value_range"])
    files = list((PROJECT / "target" / "compiled").rglob("metrics_value_range.sql"))
    if done.returncode or not files:
        annotate("error", "compile failed", {"rc": done.returncode, "out": (done.stdout + done.stderr)[-1500:]})
        return 1
    sql = files[0].read_text()
    positive = ", ".join(f"'{c}'" for c in POSITIVE)
    new_rule = (
        f"(t.value <= 0 and t.concept_key in ({positive})) and not "
        "(t.value < t.value_min or t.value > t.value_max or t.value_min is null)"
    )
    grouped = (
        "select t.concept_key, t.source_system, t.measure_key, count(*) as n, min(t.value) as min_value, "
        "max(t.value) as max_value, min(t.date_key) as first_date, max(t.date_key) as last_date, "
        "count(distinct t.geography_key) as areas, string_agg(distinct cast(t.geography_key as varchar), ' ') as geo "
        f"from ({sql}) as t where {{cond}} group by all order by n desc"
    )
    rows = _show("1 new rule", grouped.format(cond=new_rule))
    if rows is not None:
        for r in rows:
            r["geo"] = (r.get("geo") or "")[:200]
        annotate(
            "warning" if rows else "notice",
            "1 values <= 0 of positive KPIs on production Gold (fail only by the new rule)",
            {"groups": len(rows), "rows": sum(r["n"] for r in rows), "first": rows[:12]},
        )
        examples = _show(
            "1b examples",
            f"select t.concept_key, t.source_system, t.measure_key, t.geography_key, t.date_key, t.value from ({sql}) "
            f"as t where {new_rule} order by 1, 2, 3, 5 limit 40",
            40,
        )
        if examples is not None:
            annotate("notice", "1b examples", examples[:40])
    other = _show("2 other failures", grouped.format(cond="not (" + new_rule + ")"))
    if other is not None:
        for r in other:
            r["geo"] = (r.get("geo") or "")[:120]
        annotate(
            "warning" if other else "notice",
            "2 other failing rows of metrics_value_range (exist on main too)",
            {"groups": len(other), "rows": sum(r["n"] for r in other), "first": other[:10]},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
