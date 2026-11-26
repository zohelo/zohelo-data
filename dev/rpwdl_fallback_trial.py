"""Dev trial of the rpwdl Silver fallback after review (#3139, PR #3234), on the branch's final code; no downloads.

The production Bronze tables bronze.rpwdl_* are read (read-only) and copied into a local DuckDB file playing "r2"
(target prod; production catalog attached read-only as "lake" for the other parents, as in rpwdl_years_trial).
A: Bronze exactly as production holds it now (only entities without a year, landed before #3139): the state the first
   platform build after the merge meets. Silver must not be empty, every attached test must pass, Gold one year.
B: the same rows plus copies relabelled _2026 and _2027 (the year entities after the first runs on the new code):
   Silver must read only the year entities (rows = 2 x production), Gold two years, the newest provisional.
dbt build of the five Silver models, the code lists, poc_map_geography and poc_build_rpwdl with every attached test,
twice per scenario. Nothing is written to R2. Counts only. Deleted before merge."""

from __future__ import annotations

import shutil
import sys
import tempfile
import traceback
from pathlib import Path

import duckdb

from dev.rpwdl_years_trial import SELECT, SILVER, dbt, note, setup_dbt
from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.to_bronze.rpwdl import load as bronze


def scenario(name: str, arrow_tables: dict, failures: list[str]) -> dict:
    work = Path(tempfile.mkdtemp(prefix=f"rpwdl-{name}-"))
    try:
        aliases, _, legacy = setup_dbt(work, arrow_tables)
        runs = []
        for attempt in (1, 2):
            code, results = dbt(work, "build", "--select", *SELECT)
            bad = {k: v for k, v in results.items() if v[0] not in ("success", "pass")}
            runs.append({"exit": code, "nodes": len(results), "tests": sum(1 for k in results if k.startswith("test.")),
                         "not_passing": bad})
            if code or bad:
                failures.append(f"{name} dbt build {attempt}: exit {code}, {bad}")
        local = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
        rel = {m: f'{s}."{a}"' for m, (s, a) in aliases.items()}
        silver = {m: local.sql(f"select snapshot_year, count(*), count(*) filter (is_active) from {rel[m]} "
                               "group by 1 order by 1").fetchall() for m in SILVER}
        b = rel["poc_build_rpwdl"]
        gold = {
            "rows_by_period_status": local.sql(f"select period_label, observation_status_key, count(*) from {b} "
                                               "group by all order by all").fetchall(),
            "dup_series_key_x_date_key": local.sql(
                f"select count(*) from (select 1 from {b} group by measure_key || '|' || geography_key || '|' || "
                "breakdown_json, date_key having count(*) > 1)").fetchall()[0][0],
            "unmatched_X": local.sql(f"select count(*) from {b} where geography_key = '_X'").fetchall()[0][0],
            "spot_PL": local.sql(f"select measure_key, period_label, value from {b} where geography_key = 'PL' "
                                 "and is_total order by all").fetchall(),
        }
        local.close()
        if gold["dup_series_key_x_date_key"] or gold["unmatched_X"]:
            failures.append(f"{name} gold {gold}")
        return {"runs": runs, "legacy_rows_added_by_setup": legacy, "silver_by_year(n, active)": silver, **gold}
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main(argv) -> int:
    failures: list[str] = []
    try:
        cat = bf.retry(catalog, "catalog")
        prod = {t: cat.load_table(("bronze", t)).scan().to_arrow() for t in bronze.TABLES}
        con = duckdb.connect()
        entities = {}
        for t, a in prod.items():
            con.register(t, a)
            entities[t] = con.sql(f"select entity, count(*) from {t} group by 1 order by 1").fetchall()
        note("production bronze (read-only)", {"rows": {t: a.num_rows for t, a in prod.items()},
                                               "entities": {t: len(e) for t, e in entities.items()},
                                               "entities_with_year": {t: sum(1 for e, _ in v if e[-4:].isdigit())
                                                                      for t, v in entities.items()}})
        a = scenario("A", prod, failures)
        if any(not rows for rows in a["silver_by_year(n, active)"].values()):
            failures.append("A: a Silver model is empty")
        if [r[0] for r in a["rows_by_period_status"]] != ["2026"] or a["rows_by_period_status"][0][1] != "provisional":
            failures.append(f"A: gold periods {a['rows_by_period_status']}")
        note("A: production bronze, no year entities", a)

        relabelled = {}
        for t in prod:
            relabelled[t] = con.sql(
                f"select * from {t} union all "
                f"select * replace (entity || '_2026' as entity) from {t} union all "
                f"select * replace (entity || '_2027' as entity) from {t}"
            ).to_arrow_table()
        b = scenario("B", relabelled, failures)
        for m, rows in b["silver_by_year(n, active)"].items():
            a_rows = a["silver_by_year(n, active)"][m]
            if [r[0] for r in rows] != [2026, 2027] or any(tuple(r[1:]) != tuple(a_rows[0][1:]) for r in rows):
                failures.append(f"B: silver {m} {rows} vs A {a_rows}")
        if [tuple(r[:2]) for r in b["rows_by_period_status"]] != [("2026", "ok"), ("2027", "provisional")]:
            failures.append(f"B: gold periods {b['rows_by_period_status']}")
        note("B: year entities next to the old ones", b)
    except Exception as exc:  # noqa: BLE001
        failures.append(f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
    note("result", {"failures": failures})
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
