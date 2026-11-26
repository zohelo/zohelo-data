"""Dev trial of #3119 (GIOŚ yearly archive of verified values) on the final code; test branch only, never merged.

Landing: every archive file (26 years and the station workbook) into the Landing prefix zz_dev_3119/01_landing,
twice (the second run must write nothing); load.candidates must not take an archive entity for a daily pass.
Bronze and Silver: archive_load.run into the namespaces zz_dev_3119_b (Bronze, lake prefix zz_dev_3119_b/) and
zz_dev_3119_s (Silver, lake prefix zz_dev_3119_s/), twice (the second run must write nothing); profiles of both
layers. dbt (real dbt, target prod) in this runner's checkout only: the two archive tables are read from scratch
sources, the Silver model gios_air_archive_stations and every Gold model built land in zz_dev_3119_s (var
gold_schema); the provisional parts (silver.gios_air_hourly, gios_air_stations, gios_air_sensors) and the other
sources' Silver tables the geography map reads are read from production, never written; a guard refuses a selected
node outside zz_dev_. Built twice. Checks: tests, Gold rows per block, duplicates by series_key × date_key,
verified apart from provisional, spot values. Everything written is removed in `finally`.
"""

from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import time
import traceback

import duckdb
import yaml

from ingestion.common import report as report_module

LAND = "zz_dev_3119/01_landing"
NS_B, NS_S = "zz_dev_3119_b", "zz_dev_3119_s"
ROOT = Path(__file__).resolve().parents[1]
WH = ROOT / "warehouse"
COLLECTED: dict[str, list] = {}
OURS = ("gios_air_archive_stations", "poc_gios_air_verified", "poc_build_gios_air_quality", "poc_map_geography")
ANNOTATE = report_module.annotate


def collect(level: str, title: str, data) -> None:
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:6000]}", flush=True)


report_module.annotate = collect


def note(title: str, data, level: str = "notice") -> None:
    print(f"NOTE {title}: {json.dumps(data, default=str, ensure_ascii=False)}", flush=True)
    ANNOTATE(level, title, data)


def q(con, sql: str, limit: int = 80) -> list:
    rows = con.execute(sql).fetchall()[:limit]
    return [[v if isinstance(v, int | float) or v is None else str(v) for v in r] for r in rows]


def landing_objects(client, bucket) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def snapshots(cat) -> dict:
    out = {}
    for ns in (NS_B, NS_S):
        try:
            for t in cat.list_tables(ns):
                snap = cat.load_table(t).current_snapshot()
                out[".".join(t)] = snap.snapshot_id if snap else None
        except Exception:
            pass
    return out


def patch_checkout() -> None:
    """This runner's checkout only: the archive tables in scratch sources, the Silver model in scratch."""
    yml = WH / "models/bronze/gios_air_quality.yml"
    spec = yaml.safe_load(yml.read_text())
    moved = {}
    for source in spec["sources"]:
        keep = []
        for table in source["tables"]:
            (moved.setdefault(source["name"], []) if table["name"].startswith("gios_air_archive") else keep).append(table)
        source["tables"] = keep
    spec["sources"] += [
        {"name": "trial_b", "database": "r2", "schema": NS_B, "tables": moved["gios_air_quality"]},
        {"name": "trial_s", "database": "r2", "schema": NS_S, "tables": moved["gios_air_quality_silver"]},
    ]
    yml.write_text(yaml.safe_dump(spec, sort_keys=False, allow_unicode=True))
    swaps = {
        "models/silver/gios_air_quality/gios_air_archive_stations.sql": [("source('gios_air_quality',", "source('trial_b',")],
        "models/gold/poc_gios_air_verified.sql": [("source('gios_air_quality_silver',", "source('trial_s',")],
        "tests/gios_air_archive_values_complete.sql": [("source('gios_air_quality_silver',", "source('trial_s',")],
    }
    for rel, pairs in swaps.items():
        path = WH / rel
        text = path.read_text()
        for old, new in pairs:
            assert old in text, (rel, old)
            text = text.replace(old, new)
        path.write_text(text)
    project = yaml.safe_load((WH / "dbt_project.yml").read_text())
    silver = project["models"]["zohelo"]["silver"]
    silver.setdefault("gios_air_quality", {})["+schema"] = NS_S
    (WH / "dbt_project.yml").write_text(yaml.safe_dump(project, sort_keys=False, allow_unicode=True))


def dbt(args: list[str], variables: dict) -> subprocess.CompletedProcess:
    env = {**os.environ, "DBT_PROFILES_DIR": str(WH), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    return subprocess.run(
        ["dbt", *args, "--target", "prod", "--vars", json.dumps(variables)], cwd=WH, env=env, capture_output=True,
        text=True,
    )


def selection(variables: dict) -> tuple[list[str], list[str]]:
    run = dbt(["parse", "--no-partial-parse"], variables)
    if run.returncode:
        raise RuntimeError("dbt parse: " + run.stdout[-1500:])
    nodes = json.loads((WH / "target/manifest.json").read_text())["nodes"]
    by_name = {n["name"]: uid for uid, n in nodes.items() if n["resource_type"] in ("model", "seed")}
    blocks = sorted(n["name"] for n in nodes.values() if "gold_block" in n["tags"] and n["name"] != "poc_build_gios_air_quality")

    def inside(uid: str) -> bool:
        n = nodes[uid]
        if n["resource_type"] == "seed" or n["name"] in OURS:
            return True
        if not n["original_file_path"].startswith("models/gold/"):
            return False
        return "gold_block" not in n["tags"]

    todo, chosen = [by_name[t] for t in OURS], set()
    while todo:
        uid = todo.pop()
        if uid in chosen:
            continue
        chosen.add(uid)
        todo += [p for p in nodes[uid]["depends_on"]["nodes"] if p in nodes and inside(p)]
    outside = [nodes[u]["name"] + "@" + nodes[u]["schema"] for u in chosen if not nodes[u]["schema"].startswith("zz_dev_")]
    if outside:
        raise RuntimeError(f"guard: selected nodes outside zz_dev_: {outside}")
    return sorted(nodes[u]["name"] for u in chosen), blocks


def build(names: list[str], variables: dict) -> dict:
    started = time.monotonic()
    args = ["build", "--select", *names, "source:trial_b", "source:trial_s", "--indirect-selection", "cautious", "--threads", "2"]
    run = dbt(args, variables)
    for wait in (90, 240):
        if run.returncode == 0 or "429" not in run.stdout:
            break
        time.sleep(wait)
        run = dbt(["retry", "--threads", "1"], variables)
    results = json.loads((WH / "target/run_results.json").read_text())["results"]
    kinds = Counter((r["unique_id"].split(".")[0], r["status"]) for r in results)
    ours = [
        f"{r['unique_id'].split('.')[2][:60]}:{r['status']}:{r.get('failures')}"
        for r in results
        if r["unique_id"].startswith("test.") and ("gios" in r["unique_id"] or "trial_" in r["unique_id"])
    ]
    bad = [(r["unique_id"].split(".")[2][:60], r["status"], (r.get("message") or "")[:200]) for r in results if r["status"] not in ("success", "pass")]
    out = {"exit": run.returncode, "minutes": round((time.monotonic() - started) / 60, 1), "nodes": len(results),
           "by_kind_status": {f"{k}:{s}": n for (k, s), n in sorted(kinds.items())}, "not_ok": bad[:10], "gios_tests": ours}
    if run.returncode and not bad:
        out["stdout_tail"] = [ln[:300] for ln in run.stdout.splitlines() if "rror" in ln][:10]
    return out


def trial() -> int:
    from ingestion.common import bronze_files as bf
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.gios_air_quality import archive_load, load
    from ingestion.to_landing.gios_air_quality import archive, receipts

    client, cat = s3(), bf.retry(catalog, "catalog")
    landing, lake = req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")

    # Landing twice
    res_l: dict = {"runs": []}
    states = []
    for _ in (1, 2):
        t0 = time.monotonic()
        held = receipts(client, landing, LAND, archive.is_archive)
        summary, _ = archive.run(client, landing, LAND, held, time.monotonic() + 150 * 60)
        res_l["runs"].append({k: summary.get(k) for k in ("listed", "current", "landed", "unchanged", "left_for_next_run", "bytes")}
                             | {"minutes": round((time.monotonic() - t0) / 60, 1)})
        states.append(landing_objects(client, landing))
    data = [(k, s) for k, s in states[0] if not k.endswith(".receipt.json")]
    res_l.update({"files": len(data), "bytes": sum(s for _, s in data), "second_run_unchanged": states[0] == states[1],
                  "sizes_mb": {k.split("/")[-2]: round(s / 1e6, 1) for k, s in data}})
    res_l["runs"][0]["landed"] = len(res_l["runs"][0]["landed"] or [])
    res_l["pass_candidates_from_archive"] = len(load.candidates(client, landing, LAND, cat, NS_B))
    note("gios archive trial landing", res_l)

    # Bronze and Silver twice
    res_b: dict = {"runs": []}
    snaps = []
    for _ in (1, 2):
        out = archive_load.run(client, cat, landing, lake, LAND, NS_B, NS_S, 200)
        res_b["runs"].append({k: (len(v) if k == "loaded" else v) for k, v in out.items() if k != "skipped_members"}
                             | {"skipped_members": out["skipped_members"]})
        snaps.append(snapshots(cat))
    res_b["second_run_unchanged"] = snaps[0] == snaps[1] and res_b["runs"][1]["loaded"] == 0
    held = archive_load.newest_receipts(client, landing, LAND)
    loaded = set()
    for t in ((NS_B, archive_load.VALUES), (NS_B, archive_load.STATIONS)):
        loaded |= bf.loaded_hashes(cat.load_table(t))
    rejected = bf.rejected_hashes(cat, (NS_B, archive_load.REJECTED))
    res_b["landing_files_in_bronze_or_rejected"] = [len(held), sum(r["sha256"] in loaded | rejected for r in held.values())]
    note("gios archive trial bronze silver", res_b, "notice" if not res_b["runs"][0]["rejected"] else "warning")

    con = duckdb.connect()
    con.register("bv", cat.load_table((NS_B, archive_load.VALUES)).scan().to_arrow())
    con.register("bs", cat.load_table((NS_B, archive_load.STATIONS)).scan().to_arrow())
    prof_b = {
        "rows_files_entities": q(con, "select count(*), count(distinct raw_sha256), count(distinct entity) from bv")[0],
        "rows_per_year": q(con, "select entity, count(*) from bv group by 1 order by 1"),
        "members": q(con, "select count(distinct member) from bv")[0][0],
        "non_numeric_values": q(con, "select value, count(*) from bv where try_cast(value as double) is null group by 1 order by 2 desc limit 10"),
        "null_station_code": q(con, "select count(*) from bv where station_code is null")[0][0],
        "header_variants": q(con, "select coalesce(averaging_time, '<null>'), count(*) from bv group by 1 order by 2 desc limit 12"),
        "station_rows": q(con, "select count(*), count(distinct station_code), count(distinct voivodeship) from bs")[0],
    }
    note("gios archive trial bronze profile", prof_b)
    con.register("sv", cat.load_table((NS_S, archive_load.VALUES)).scan(
        selected_fields=("entity", "archive_year", "member", "station_code", "indicator", "averaging_time", "indicator_published",
                         "unit", "measured_at", "measured_on", "value", "validation_status", "raw_sha256")).to_arrow())
    prof_s = {
        "rows": q(con, "select count(*), count(distinct entity), min(measured_on), max(measured_on) from sv")[0],
        "dup_keys": q(con, "select count(*) from (select 1 from sv group by station_code, indicator, averaging_time, measured_at having count(*) > 1)")[0][0],
        "null_keys": q(con, "select count(*) filter (where station_code is null), count(*) filter (where indicator is null), count(*) filter (where measured_at is null), count(*) filter (where raw_sha256 is null) from sv")[0],
        "averaging_status": q(con, "select averaging_time, validation_status, count(*) from sv group by 1, 2"),
        "indicator_from_member_name": q(con, "select count(*) from sv where indicator_published is null")[0][0],
        "units": q(con, "select coalesce(unit, '<none>'), count(*) from sv group by 1 order by 2 desc"),
        "five_hourly_per_year": q(con, "select archive_year, count(distinct station_code), count(*) from sv where averaging_time = '1g' and indicator in ('PM10', 'PM2.5', 'NO2', 'O3', 'SO2') group by 1 order by 1"),
        "zeros_negatives_five": q(con, "select count(*) filter (where value = 0), count(*) filter (where value < 0), min(value), max(value) from sv where indicator in ('PM10', 'PM2.5', 'NO2', 'O3', 'SO2') and averaging_time = '1g'")[0],
        "codes_unplaced_five_hourly": q(con, "with m as (select station_code as code from bs union select trim(unnest(string_split(regexp_replace(old_station_codes, '[;,\\s]+', ',', 'g'), ','))) from bs where old_station_codes is not null) select count(distinct station_code), list(distinct station_code)[1:10] from sv where averaging_time = '1g' and indicator in ('PM10', 'PM2.5', 'NO2', 'O3', 'SO2') and station_code not in (select code from m)")[0],
        "spot_station_pm10_2025_mean": q(con, "select station_code, round(avg(value), 1), count(*) from sv where archive_year = 2025 and indicator = 'PM10' and averaging_time = '1g' and station_code in ('MpKrakAlKras', 'DsWrocAlWisn', 'MzWarAlNiepo') group by 1"),
    }
    note("gios archive trial silver profile", prof_s)
    con.unregister("sv")

    # dbt
    patch_checkout()
    variables = {"gold_schema": NS_S, "topics": yaml.safe_load((WH / "warehouse.yaml").read_text()).get("topics")}
    names, blocks = selection(variables)
    variables["waiting_blocks"] = blocks
    res_d: dict = {"selected_nodes": len(names), "build1": build(names, variables)}
    res_d["build2"] = build(names, variables)
    note("gios archive trial dbt build", res_d, "notice" if res_d["build1"]["exit"] == 0 else "error")

    for name, alias in (("build_gios_air_quality", "b"), ("gios_air_verified", "v"), ("gios_air_archive_stations", "st")):
        con.register(alias, cat.load_table((NS_S, name)).scan().to_arrow())
    gold = {
        "rows_per_block": q(con, "select block_id, frequency, observation_status_key, count(*), count(distinct measure_key), min(date_key), max(date_key) from b group by all order by 1"),
        "dup_series_date": q(con, "select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key from b group by all having count(*) > 1)")[0][0],
        "measure_keys_shared_by_blocks": q(con, "select count(*) from (select measure_key from b group by 1 having count(distinct block_id) > 1)")[0][0],
        "geography_unmatched": q(con, "select block_id, geography_source_code, count(*) from b where geography_key = '_X' group by all"),
        "areas_verified_monthly": q(con, "select count(distinct geography_key) from b where block_id = 'air_verified_monthly'")[0][0],
        "value_range": q(con, "select block_id, min(value), max(value) from b group by 1"),
        "spot_pl_monthly": q(con, "select measure_codes, period_label, value from b where block_id = 'air_verified_monthly' and geography_key = 'PL' and period_label in ('2006-01', '2015-01', '2025-01', '2025-07') order by 1, 2"),
        "spot_pl_daily": q(con, "select measure_codes, period_label, value from b where block_id = 'air_verified_daily' and period_label in ('2025-01-01', '2025-11-15', '2010-01-27') order by 1, 2"),
        "spot_voiv_monthly_pm10_2025_01": q(con, "select geography_key, value from b where measure_key = 'gios_air_quality:air_verified_monthly:ug_m3:PM10' and period_label = '2025-01' order by 2 desc limit 17"),
        "stations_per_pl_day_min": q(con, "select indicator, min(stations), max(stations) from v where block_id = 'air_verified_daily' group by 1"),
        "first_month_per_indicator_pl": q(con, "select indicator, min(date_key) from v where block_id = 'air_verified_monthly' and area = 'POLSKA' group by 1 order by 1"),
    }
    note("gios archive trial gold", gold)
    return 0 if res_d["build1"]["exit"] == 0 and res_d["build2"]["exit"] == 0 else 1


def cleanup() -> None:
    from ingestion.common import bronze_files as bf
    from ingestion.common.r2 import catalog, req, s3

    removed: dict = {"tables": 0, "objects": 0, "failed": {}}
    cat = bf.retry(catalog, "catalog")
    for ns in (NS_S, NS_B):
        try:
            tables = list(cat.list_tables(ns))
        except Exception:
            tables = []
        for table in tables:
            for attempt in range(6):
                try:
                    try:
                        cat.purge_table(table)
                    except Exception:
                        cat.drop_table(table)
                    removed["tables"] += 1
                    removed["failed"].pop(".".join(table), None)
                    break
                except Exception as exc:
                    removed["failed"][".".join(table)] = str(exc)[:80]
                    time.sleep(10 * (attempt + 1))
        try:
            cat.drop_namespace(ns)
        except Exception as exc:
            removed["failed"][ns] = str(exc)[:80]
    client = s3()
    left = 0
    for bucket_var, prefixes in (("STORAGE_LANDING_BUCKET_NAME", ["zz_dev_3119/"]), ("STORAGE_LAKEHOUSE_BUCKET_NAME", [f"{NS_B}/", f"{NS_S}/"])):
        bucket = req(bucket_var)
        for prefix in prefixes:
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
                    removed["objects"] += len(keys)
            left += sum(len(p.get("Contents", [])) for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix))
    removed["objects_left"] = left
    removed["namespaces_left"] = [ns for ns in (NS_B, NS_S) if (ns,) in cat.list_namespaces()]
    note("gios archive trial scratch removed", removed, "notice" if not removed["failed"] and not left else "warning")


def main() -> int:
    status = 1
    try:
        status = trial()
    except Exception as exc:
        note("gios archive trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-2000:]}, "error")
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
