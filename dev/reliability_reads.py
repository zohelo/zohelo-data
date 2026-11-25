"""Read-only checks of the reliability lane (#2873, orchestrator order 25 Nov 23:40), one Dev run. Writes nothing to R2.

1. metrics_value_range on production Gold (13,692 rows in build run 45708433034): the test compiled with the prod
   target from main, its rows grouped by concept x source x measure x bound x kind (below / above / no bound) with
   count, min, max, areas and dates; then, for each failing key, the distribution of ALL its metrics rows (to tell a
   stray row from a wrong bound or a unit error).
2. krs_entity rows with a null name (45 in that build): their krs_number, register and legal form in Silver.

Full results go to the job log (one JSON line each, prefixed RESULT); annotations are compact.
"""

from __future__ import annotations

import json
import os
import re
import subprocess

from ingestion.common.report import annotate
from warehouse.build import PROJECT, TOPICS


def _dbt(args: list[str]) -> subprocess.CompletedProcess:
    base = ["dbt", "--quiet", *args[:1], "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT), "--target", "prod"]
    return subprocess.run(
        [*base, *args[1:], "--vars", json.dumps({"topics": TOPICS})],
        cwd=PROJECT,
        env={**os.environ},
        text=True,
        capture_output=True,
    )


MISSING: list[str] = []


def _show(sql: str, label: str, limit: int = 2000) -> list[dict] | None:
    """Runs the query; a metrics model not yet built on production is read as empty."""
    for _ in range(40):
        for name in MISSING:
            sql = sql.replace(f'"gold"."{name}"', '(select * from "r2"."gold"."metrics_population" where false) as _e')
            sql = sql.replace('"r2".(select', "(select")
        shown = _dbt(["show", "--inline", sql, "--limit", str(limit), "--output", "json"])
        out = shown.stdout.strip()
        try:
            return json.loads(out[out.index("{") :])["show"]
        except (ValueError, KeyError):
            text = out + shown.stderr
            gone = re.search(r"Table with name (\w+) does not exist", text)
            if gone and gone.group(1) not in MISSING:
                MISSING.append(gone.group(1))
                continue
            annotate("error", f"{label}: show failed", {"rc": shown.returncode, "missing": MISSING, "out": text[-1500:]})
            return None
    return None


def _emit(label: str, rows) -> None:
    print("RESULT " + label + " " + json.dumps(rows, default=str, separators=(",", ":")), flush=True)


def value_range() -> None:
    done = _dbt(["compile", "--select", "metrics_value_range"])
    files = list((PROJECT / "target" / "compiled").rglob("metrics_value_range.sql"))
    if done.returncode or not files:
        annotate("error", "1 compile failed", {"rc": done.returncode, "out": (done.stdout + done.stderr)[-1500:]})
        return
    sql = files[0].read_text()
    grouped = (
        "select concept_key, source_system, measure_key, value_min, value_max,"
        " case when value_min is null then 'nobound' when value < value_min then 'below' else 'above' end as kind,"
        " count(*) as n, min(value) as lo, max(value) as hi, count(distinct geography_key) as areas,"
        " min(date_key) as d0, max(date_key) as d1, arg_max(geography_key, value) as geo_hi,"
        " arg_min(geography_key, value) as geo_lo"
        f" from ({sql}) as t group by all order by n desc"
    )
    rows = _show(grouped, "1 grouped")
    if rows is None:
        return
    _emit("value_range_groups", rows)
    total = sum(int(r["n"]) for r in rows)
    by_src: dict[str, int] = {}
    for r in rows:
        key = f"{r['concept_key']}|{r['source_system']}|{r['kind']}"
        by_src[key] = by_src.get(key, 0) + int(r["n"])
    annotate("warning" if total else "notice", "1 metrics_value_range on prod: rows by concept|source|kind",
             {"rows": total, "groups": len(rows), "by": by_src, "not built on prod": MISSING})
    compact = [
        [r["concept_key"], r["source_system"], r["measure_key"], r["kind"], r["value_min"], r["value_max"], r["n"],
         r["lo"], r["hi"], r["geo_hi"]]
        for r in rows
    ]
    for i in range(0, min(len(compact), 120), 30):
        annotate("notice", f"1a groups {i}-{i + 29}", compact[i : i + 30])

    head, _, _ = sql.rpartition("\nwhere m.value")
    if not head:
        annotate("error", "1b: compiled test has no final where", sql[-400:])
        return
    keys = sorted({(r["concept_key"], r["source_system"], r["measure_key"]) for r in rows})
    stats_all: list[dict] = []
    for i in range(0, len(keys), 40):
        cond = " or ".join(
            "(concept_key = '{}' and source_system = '{}' and measure_key = '{}')".format(*(k.replace("'", "''") for k in key))
            for key in keys[i : i + 40]
        )
        agg = (
            "select concept_key, source_system, measure_key, count(*) as n, min(value) as lo,"
            " quantile_cont(value, 0.01) as p01, median(value) as p50, quantile_cont(value, 0.99) as p99,"
            " max(value) as hi, count(distinct geography_key) as areas, min(date_key) as d0, max(date_key) as d1,"
            " arg_max(geography_key || '@' || date_key, value) as at_hi,"
            " max(case when geography_key in ('PL', 'ISO3166:PL', 'TERC:00') then value end) as pl_hi,"
            " max(value_min) as bmin, max(value_max) as bmax"
            f" from ({head}) as t where {cond} group by 1, 2, 3 order by 1, 2, 3"
        )
        stats = _show(agg, f"1b stats {i}")
        if stats is not None:
            stats_all.extend(stats)
    _emit("value_range_key_stats", stats_all)
    compact = [
        [s["concept_key"], s["source_system"], s["measure_key"], s["n"], s["lo"], s["p50"], s["p99"], s["hi"],
         s["at_hi"], s["bmin"], s["bmax"]]
        for s in stats_all
    ]
    for i in range(0, min(len(compact), 90), 30):
        annotate("notice", f"1b all rows of failing keys {i}-{i + 29}", compact[i : i + 30])


def krs() -> None:
    rows = _show(
        "select e.krs_number, s.register, s.legal_form, s.requested_on, s.source_version, s.has_current_extract"
        " from {{ ref('krs_entity') }} as e left join {{ ref('ms_krs_entities') }} as s using (krs_number)"
        " where e.name is null order by 1",
        "2 krs",
        limit=200,
    )
    if rows is None:
        return
    _emit("krs_null_name", rows)
    annotate("warning" if rows else "notice", "2 krs_entity rows with a null name on prod",
             {"rows": len(rows), "requested_on": sorted({str(r["requested_on"]) for r in rows}),
              "legal_form": sorted({str(r["legal_form"]) for r in rows}), "first": [r["krs_number"] for r in rows[:10]]})


def units() -> None:
    """Why world_bank_wdi:NY.GDP.MKTP.CD is not converted from US$ to million US$ (D33) in the metrics."""
    queries = {
        "3a unit_unmapped": "select * from ({{ poc_measure_units() }}) as u where source_unit_key is null or"
        " target_base_unit_key is null",
        "3b gdp units": "select * from ({{ poc_measure_units('gdp_current_prices') }}) as u",
        "3c wdi dim_measure": "select measure_key, unit_key, measure_name from {{ ref('poc_dim_measure') }}"
        " where measure_key like 'world_bank_wdi:%' order by 1",
        "3d wdi indicators": "select indicator_code, unit_of_measure, indicator_name from {{ ref('wdi_indicators') }}"
        " where indicator_code in (select replace(measure_key, 'world_bank_wdi:', '') from {{ ref('poc_dim_measure') }}"
        " where measure_key like 'world_bank_wdi:%') order by 1",
        "3e wdi unit codes in build_wdi": "select unit_code, count(*) as n, count(distinct measure_key) as measures,"
        " min(source_version) as v0, max(source_version) as v1 from {{ ref('poc_build_wdi') }} group by 1 order by 2 desc",
    }
    for label, sql in queries.items():
        rows = _show(sql, label, limit=500)
        if rows is None:
            continue
        _emit(label.replace(" ", "_"), rows)
        annotate("notice", label, rows[:40])


def main() -> int:
    for step in (units,):
        try:
            step()
        except Exception as exc:
            annotate("error", f"{step.__name__} crashed", f"{type(exc).__name__}: {str(exc)[:1500]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
