"""Dev trial (scratch only, deleted before merge): dbt build of the Gold folder and seeds with the OECD,
PRG/TERYT and GLEIF/MF blocks (#2738, #2739, #2740) against real Silver, written to zz_dev_srcgold
(dbt var gold_schema), then checks as annotations and the scratch tables dropped."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time

from dev.src_gold_probe import connect, show
from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
import sys
NS = "zz_dev_srcgold_" + (sys.argv[1] if len(sys.argv) > 1 else "x")
NEW = "('world_bank_ids',)"


def build() -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "warehouse")}
    started = time.monotonic()
    run = subprocess.run(
        ["dbt", "build", "--target", "prod", "--select", "path:models/gold", "path:seeds",
         "--exclude", "path:models/gold/topics", "fact_grain", "--threads", "2", "--vars", f"{{gold_schema: {NS}}}"],
        cwd=ROOT / "warehouse", env=env, capture_output=True, text=True,
    )
    for wait in (90, 240, 480):  # catalog HTTP 429: retry the failed and skipped nodes (warehouse.yaml waits)
        if run.returncode == 0 or "429" not in run.stdout:
            break
        annotate("notice", "trial retry", {"wait_s": wait})
        time.sleep(wait)
        run = subprocess.run(["dbt", "retry", "--threads", "1"], cwd=ROOT / "warehouse", env=env,
                             capture_output=True, text=True)
    results = json.loads((ROOT / "warehouse" / "target" / "run_results.json").read_text())["results"]
    bad = [(r["unique_id"].split(".")[-1][:60], r["status"], (r.get("message") or "")[:200])
           for r in results if r["status"] not in ("success", "pass")]
    slow = sorted(((r["unique_id"].split(".")[-1][:40], round(r["execution_time"])) for r in results),
                  key=lambda x: -x[1])
    annotate("notice" if run.returncode == 0 else "warning", "trial build",
             {"exit": run.returncode, "minutes": round((time.monotonic() - started) / 60, 1), "nodes": len(results),
              "not_ok": bad[:25], "slowest": slow[:10]})
    if run.returncode:
        errs = [ln[:300] for ln in run.stdout.splitlines() if "rror" in ln][:30]
        text = json.dumps(errs, ensure_ascii=False)
        for i in range(0, len(text), 3500):
            annotate("warning", f"trial errors [{i // 3500 + 1}]", text[i:i + 3500])


def checks() -> None:
    con = connect()
    g = f"r2.{NS}"
    show(con, "build_check source systems", f"""select string_agg(distinct source_system, ',' order by source_system),
        count(*) filter (where row_count = 0), list(source_system || ':' || block_id) filter (where row_count = 0)
        from {g}.build_check""")
    show(con, "geo checks", f"""select (select count(*) from {g}.build_prg where geography_key = 'PL') prg_country_rows,
        (select count(*) from {g}.build_prg) prg_rows,
        (select count(*) from {g}.geo_unit where area_includes_sea) sea_rows""")
    show(con, "build_check new", f"""select source_system, block_id, row_count, series, measures,
        rows_geography_unmatched, unmatched_sample, rows_total_series, first_date, last_date, frequencies, statuses,
        note from {g}.build_check where source_system in {NEW}""")
    show(con, "map_geography new", f"""select source_system, count(*), count(*) filter (where not matched),
        list(source_code) filter (where not matched)[1:40] from {g}.map_geography
        where source_system in {NEW} group by 1""")
    show(con, "grain_check new", f"select * from {g}.grain_check where source_system in {NEW}")
    show(con, "fact grain by source (series_key x date_key)", f"""select source_system, count(*) from (
        select series_key, date_key, any_value(source_system) as source_system from (
            select series_key, date_key, source_system from {g}.fact_annual union all
            select series_key, date_key, source_system from {g}.fact_quarterly union all
            select series_key, date_key, source_system from {g}.fact_monthly union all
            select series_key, date_key, source_system from {g}.fact_daily)
        group by all having count(*) > 1) group by 1""")
    show(con, "grain_check all", f"select source_system, count(*) from {g}.grain_check group by 1")
    show(con, "ids metrics", f"""select measure_key, geography_level, count(*), count(distinct geography_key), min(date_key),
        max(date_key), arg_max(value, date_key) filter (where geography_key = 'UA') ukr_latest,
        arg_max(value, date_key) filter (where geography_key = 'TR') tur_latest from (
        select * from {g}.metrics_external_debt_stocks union all select * from {g}.metrics_external_debt_gni
        union all select * from {g}.metrics_external_debt_service union all select * from {g}.metrics_external_debt_service_gni
        union all select * from {g}.metrics_external_debt_service_exports) group by 1, 2 order by 1, 2""", 30)
    show(con, "ids build rows", f"select count(*), count(distinct series_key), max(date_key) from (select *, measure_key || geography_key as series_key from {g}.build_ids)")


def drop() -> None:
    cat = bf.retry(catalog, "catalog")
    rows, failed = {}, {}
    for ident in cat.list_tables(NS):
        try:
            snap = cat.load_table(ident).current_snapshot()
            rows[ident[-1]] = int((snap.summary or {}).get("total-records", -1)) if snap else 0
        except Exception:  # noqa: BLE001 - report only
            rows[ident[-1]] = -1
    text = json.dumps(rows)
    for i in range(0, len(text), 3500):
        annotate("notice", f"trial rows [{i // 3500 + 1}]", text[i:i + 3500])
    for ident in cat.list_tables(NS):
        for attempt in range(6):
            try:
                cat.drop_table(ident)
                failed.pop(ident[-1], None)
                break
            except Exception as exc:  # noqa: BLE001 - rate limits: back off, report what is left
                failed[ident[-1]] = str(exc)[:100]
                time.sleep(10 * (attempt + 1))
        time.sleep(1)
    for attempt in range(6):
        try:
            cat.drop_namespace(NS)
            failed.pop("namespace", None)
            break
        except Exception as exc:  # noqa: BLE001
            failed["namespace"] = str(exc)[:100]
            time.sleep(10 * (attempt + 1))
    annotate("notice" if not failed else "warning", "trial dropped", {"tables": len(rows), "failed": failed})


if __name__ == "__main__":
    try:
        build()
        checks()
    finally:
        drop()
