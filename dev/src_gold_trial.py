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
NS = "zz_dev_srcgold"
NEW = "('oecd', 'gleif', 'mf_biala_lista', 'gugik_prg', 'gus_teryt')"


def build() -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "warehouse")}
    started = time.monotonic()
    run = subprocess.run(
        ["dbt", "build", "--target", "prod", "--select", "path:models/gold", "path:seeds",
         "--exclude", "path:models/gold/topics", "--threads", "2", "--vars", f"{{gold_schema: {NS}}}"],
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
    show(con, "build_check new", f"""select source_system, block_id, row_count, series, measures,
        rows_geography_unmatched, unmatched_sample, rows_total_series, first_date, last_date, frequencies, statuses,
        note from {g}.build_check where source_system in {NEW}""")
    show(con, "map_geography new", f"""select source_system, count(*), count(*) filter (where not matched),
        list(source_code) filter (where not matched)[1:40] from {g}.map_geography
        where source_system in {NEW} group by 1""")
    show(con, "grain_check new", f"select * from {g}.grain_check where source_system in {NEW}")
    show(con, "grain_check all", f"select source_system, count(*) from {g}.grain_check group by 1")
    show(con, "dim_measure new", f"""select source_system, block_id, status, count(*), sum(series), sum(points),
        min(first_date), max(last_date) from {g}.dim_measure where source_system in {NEW}
        group by all order by 1, 2""")
    show(con, "oecd aggregates", f"select geography_key, name_en from {g}.dim_geography where geography_key like 'OECD:%'")
    show(con, "dim_geography teryt", f"""select level_name, teryt_unit_type, gmina_type_code, count(*),
        count(iso_3166_2), min(teryt_state_date) from {g}.dim_geography where terc_code is not null
        group by all order by 1, 2""")
    show(con, "geo_neighbour", f"""select level_name, contiguity, count(*), min(shared_border_km),
        max(shared_border_km), count(distinct geography_key) from {g}.geo_neighbour group by all order by 1, 2""")
    show(con, "geo_unit", f"""select level_name, count(*), sum(area_km2), min(neighbours), max(neighbours),
        min(centroid_lon), max(centroid_lon), min(centroid_lat), max(centroid_lat) from {g}.geo_unit group by 1""")
    show(con, "geo_unit sample", f"""select * from {g}.geo_unit where level_name in ('country', 'wojewodztwo')
        order by 1""", 20)
    for k in ("business_confidence_oecd", "consumer_confidence_oecd", "cli_oecd", "lei_issued",
              "vat_whitelist_entries", "area"):
        show(con, f"metrics {k}", f"""select source_system, measure_key, geography_level, count(*) n,
            count(distinct geography_key) areas, min(date_key), max(date_key),
            arg_max(value, date_key) filter (where geography_key = 'PL') pl_latest,
            arg_max(value, date_key) filter (where geography_key = 'DE') de_latest, any_value(unit)
            from {g}.metrics_{k} group by all order by 1, 3""", 30)
    show(con, "gleif PL breakdown", f"""select f.measure_key, s.breakdown_json, f.value from {g}.fact_daily as f
        inner join {g}.dim_series as s on f.series_key = s.series_key
        where f.source_system = 'gleif' and f.geography_key = 'PL' and f.measure_key = 'gleif:lei2:entities:_T'
        order by f.value desc""", 25)
    show(con, "gleif voiv", f"""select f.geography_key, f.value from {g}.fact_daily as f
        inner join {g}.dim_series as s on f.series_key = s.series_key
        where f.measure_key = 'gleif:lei2:entities:_T' and s.is_total and f.geography_key like 'TERC:%'
        order by 1""", 20)
    show(con, "gleif per measure", f"""select measure_key, count(*), sum(value) filter (where is_total) from
        {g}.build_gleif where geography_key not like 'TERC:%' group by 1 order by 1""")
    show(con, "mf rows", f"""select measure_key, series_key, date_key, value from {g}.fact_daily
        where source_system = 'mf_biala_lista'""")
    show(con, "coverage blocks", f"""select source, block_id, status, silver_rows from r2.gold.coverage
        where source in {NEW} order by 1, 2""", 30)


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
