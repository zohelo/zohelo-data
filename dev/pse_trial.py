"""Dev trial for #2870 (PSE Reports API v2; test branch only, never on main).

Mode `probe` (read-only, about 14 requests): shapes of both reports. Mode `trial` (default): Landing -> Bronze ->
Silver -> Gold into scratch, each step twice. Landing: the full history 2024-06-14 .. yesterday (Warsaw) into the
Landing prefix zz_dev_pse/ (pass 1, about 1,770 requests, one started a second at most); pass 2 plans only what is
missing plus the newest 7 days, so it re-requests 14 report-days and must land nothing new. Bronze: both tables into
the Iceberg namespace zz_dev_pse and lake prefix zz_dev_pse/ (twice: the second appends nothing). Silver and the Gold
build stage are the dbt SQL rendered with Jinja and run in local DuckDB over the scratch Bronze tables, each built
twice. Mode `local` runs the model part over a small synthetic Bronze (no network). Everything written is removed in
`finally`; never writes a production namespace or prefix.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
import re
import sys
import time

import duckdb
from jinja2 import Environment
import requests

from ingestion.common import report as report_module
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_pse"
API = "https://api.raporty.pse.pl/api"
MACROS = "".join(p.read_text() for p in sorted(Path("warehouse/macros").glob("*.sql")))
COLLECTED: dict[str, list] = {}


def collect(level: str, title: str, data) -> None:
    """Annotations of the activities, gathered into the trial's own few annotations (10 per step at most)."""
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


ANNOTATE = report_module.annotate
report_module.annotate = collect


def note(title: str, data, level: str = "notice") -> None:
    ANNOTATE(level, title, data)


def q(con, sql: str, limit: int = 60) -> list:
    return [[v if isinstance(v, int | float) or v is None else str(v) for v in r] for r in con.execute(sql).fetchall()][
        :limit
    ]


# --- probe ---------------------------------------------------------------------------------------------------


def get(entity: str, day: str) -> tuple[requests.Response, float]:
    time.sleep(1.1)
    started = time.monotonic()
    r = requests.get(
        f"{API}/{entity}",
        params={"$filter": f"business_date eq '{day}'"},
        headers={"User-Agent": bulk_file.USER_AGENT, "Accept-Encoding": "identity"},
        timeout=(30, 120),
    )
    return r, round(time.monotonic() - started, 2)


def profile(entity: str, day: str) -> dict:
    r, secs = get(entity, day)
    out: dict = {"http": r.status_code, "secs": secs, "bytes": len(r.content)}
    out["sha"] = sha256(r.content).hexdigest()[:12]
    rows = r.json().get("value") or []
    out["rows"] = len(rows)
    if rows:
        keys = sorted({k for row in rows for k in row})
        out["keys"] = keys
        out["types"] = {k: dict(Counter(type(row.get(k)).__name__ for row in rows)) for k in keys}
    return out


def probe() -> int:
    today = datetime.now(UTC).date()
    for entity in ("his-wlk-cal", "rce-pln"):
        note(f"pse probe {entity}", {d: profile(entity, d) for d in ("2024-06-14", str(today - timedelta(days=1)))})
    return 0


# --- models --------------------------------------------------------------------------------------------------


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    text = MACROS + Path(path).read_text()
    return (
        Environment(extensions=["jinja2.ext.do"])
        .from_string(text)
        .render(
            config=lambda **_k: "",
            ref=lambda name: refs.get(name, name),
            source=lambda _s, table: sources[table],
        )
    )


def render_activated() -> str:
    env = Environment(extensions=["jinja2.ext.do"])
    return env.from_string(MACROS + "{{ poc_activated_blocks() }}").render(ref=lambda n: n, config=lambda **_k: "")


VALUES = {
    "bronze_rce": ["rce_pln"],
    "bronze_his": [
        "demand", "wi", "pv", "jg", "jga", "jgm", "jgm1", "jgm2", "jgo", "jgw1", "jgw2", "jgz1", "jgz2", "jgz3",
        "jnwrb", "swm_p", "swm_np",
    ],
}  # fmt: skip


def bronze_profile(con, table: str) -> dict:
    out = {
        "rows_files_entities_days": q(
            con,
            f"select count(*), count(distinct raw_sha256), count(distinct entity), min(business_date), "
            f"max(business_date) from {table}",
        )[0],
        "rows_per_day": q(
            con, f"select n, count(*) from (select entity, count(*) n from {table} group by 1) group by 1"
        ),
        "dup_keys": q(
            con, f"select count(*) from (select entity, dtime_utc from {table} group by all having count(*) > 1)"
        )[0][0],
        "null_keys": q(con, f"select count(*) from {table} where business_date is null or dtime_utc is null")[0][0],
        "copies_per_entity": q(
            con,
            f"select n, count(*) from (select entity, count(distinct raw_sha256) n from {table} group by 1) group by 1",
        ),
    }
    nulls, odd = {}, {}
    for col in VALUES[table]:
        nulls[col] = q(con, f"select count(*) filter (where {col} is null) from {table}")[0][0]
        bad = q(
            con,
            f"select {col}, count(*) from {table} where {col} is not null and try_cast({col} as double) is null "
            f"group by 1 order by 2 desc limit 5",
        )
        if bad:
            odd[col] = bad
    out["nulls"] = {k: v for k, v in nulls.items() if v}
    out["non_numeric"] = odd
    out["dtime_markers"] = q(
        con, f"select count(*) filter (where dtime like '%a:%'), count(*) filter (where period like '%a%') from {table}"
    )[0]
    return out


def models(con, result: dict) -> None:
    src = {"pse_rce_pln": "bronze_rce", "pse_his_wlk_cal": "bronze_his"}
    silver = {}
    for name in ("pse_rce_pln", "pse_power_system", "pse_daily"):
        sql = render(f"warehouse/models/silver/pse/{name}.sql", {}, src)
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table {name} as {sql}")
            counts.append(con.execute(f"select count(*) from {name}").fetchone()[0])
        silver[name] = counts
    result["silver_rows_two_builds"] = silver
    tests = {
        "unique pse_rce_pln": "select business_date, interval_end_utc from pse_rce_pln group by all having count(*) > 1",
        "unique pse_power_system": "select business_date, interval_end_utc from pse_power_system group by all having count(*) > 1",
        "unique pse_daily": "select business_date, measure_code from pse_daily group by all having count(*) > 1",
        "not_null keys": "select 1 from pse_rce_pln where business_date is null or interval_end_utc is null or entity is null or raw_sha256 is null "
        "union all select 1 from pse_power_system where business_date is null or interval_end_utc is null or entity is null or raw_sha256 is null "
        "union all select 1 from pse_daily where business_date is null or report is null or measure_code is null or unit_code is null or value is null or interval_count is null or raw_sha256 is null",
        "accepted_values daily": "select 1 from pse_daily where report not in ('rce-pln', 'his-wlk-cal') or measure_code not in ('rce_pln', 'demand', 'wi', 'pv') "
        "or unit_code not in ('pln_per_mwh', 'mw') or interval_count not in (92, 96, 100)",
    }
    result["silver_tests_failing_rows"] = {
        k: con.execute(f"select count(*) from ({v})").fetchone()[0] for k, v in tests.items()
    }
    result["silver_range"] = q(
        con,
        "select 'rce' t, min(business_date), max(business_date), count(distinct business_date) from pse_rce_pln union all "
        "select 'his', min(business_date), max(business_date), count(distinct business_date) from pse_power_system",
    )
    expected = (
        "case when month(d) = 3 and dayofweek(d) = 0 and day(d) >= 25 then 92 "
        "when month(d) = 10 and dayofweek(d) = 0 and day(d) >= 25 then 100 else 96 end"
    )
    result["incomplete_days"] = q(
        con,
        f"""with c as (
            select 'rce_pln' m, business_date d, count(*) n, count(rce_pln) v from pse_rce_pln group by all
            union all select 'demand', business_date, count(*), count(demand) from pse_power_system group by all
            union all select 'wi', business_date, count(*), count(wi) from pse_power_system group by all
            union all select 'pv', business_date, count(*), count(pv) from pse_power_system group by all)
        select m, d, n, v, {expected} from c where n <> {expected} or v <> {expected} order by d, m""",
        20,
    )
    result["dst_days"] = q(
        con,
        "select business_date, measure_code, interval_count from pse_daily where interval_count <> 96 order by 1, 2",
        24,
    )
    result["daily_per_measure"] = q(
        con,
        "select measure_code, count(*), min(business_date), max(business_date), round(min(value), 2), "
        "round(avg(value), 2), round(max(value), 2) from pse_daily group by 1 order by 1",
    )
    result["negative_or_zero"] = q(
        con,
        "select measure_code, count(*) filter (where value < 0), count(*) filter (where value = 0) from pse_daily group by 1 order by 1",
    )
    result["interval_nulls_silver"] = q(
        con,
        "select count(*) filter (where rce_pln is null) from pse_rce_pln union all "
        "select count(*) filter (where demand is null or wi is null or pv is null) from pse_power_system",
    )
    gmap_sql = render("warehouse/models/gold/poc_map_geography.sql", {}, {})
    cte = re.search(r"\npse as \((.*?)\n\),\n", gmap_sql, re.S).group(1)
    con.execute(f"create or replace view gmap_all as {cte}")
    build_sql = render("warehouse/models/gold/poc_build_pse.sql", {"poc_map_geography": "gmap_all"}, {})
    con.execute(
        f"create or replace table activated as select * from ({render_activated()}) where source_system = 'pse'"
    )
    gold = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build_sql}")
        gold.append(con.execute("select count(*) from build").fetchone()[0])
    result["gold_rows_two_builds"] = gold
    result["gold_dup_series_date"] = con.execute(
        "select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json as sk, date_key "
        "from build group by all having count(*) > 1)"
    ).fetchone()[0]
    result["gold_measures"] = q(
        con,
        "select block_id, measure_key, frequency, geography_key, unit_code, count(*), min(date_key), max(date_key) "
        "from build group by all order by 2",
    )
    result["build_check"] = q(
        con,
        "select b.block_id, count(o.block_id), count(distinct o.measure_key || '|' || o.geography_key || '|' || "
        "o.breakdown_json), count(*) filter (where o.geography_key = '_X') from activated as b left join build as o "
        "on o.source_system = b.source_system and o.block_id = b.block_id group by all order by 1",
    )
    result["gold_null_or_nan"] = q(con, "select count(*) from build where value is null or isnan(value)")[0][0]
    result["gold_newest"] = q(
        con,
        "select measure_key, date_key, round(value, 3) from build "
        "qualify row_number() over (partition by measure_key order by date_key desc) = 1 order by 1",
    )
    result["gold_spot_2025_06_15"] = q(
        con, "select measure_key, round(value, 3) from build where date_key = date '2025-06-15' order by 1"
    )


# --- trial ---------------------------------------------------------------------------------------------------


def recompute(client, bucket: str, receipts: list[dict], day: str) -> dict:
    """Daily means computed in Python straight from the landed API answers of one day (independent of the SQL)."""
    from ingestion.common import bulk_bronze

    out = {}
    for r in receipts:
        if r["entity"].endswith(day.replace("-", "")):
            rows = json.loads(bulk_bronze.read_raw(client, bucket, "pse", r))["value"]
            for col in ("rce_pln", "demand", "wi", "pv"):
                vals = [row[col] for row in rows if col in row]
                if vals and all(v is not None for v in vals):
                    out[col] = [len(vals), round(sum(vals) / len(vals), 3)]
    return out


def trial() -> int:
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.pse import load as bronze
    from ingestion.to_landing import pse as landing

    bronze.annotate = collect
    result: dict = {}
    last = landing.yesterday()
    started = time.monotonic()
    result["landing_exit"] = [landing.extract(120, landing_root=SCRATCH, last=last) for _ in (1, 2)]
    result["landing_minutes"] = round((time.monotonic() - started) / 60, 1)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    receipts, data = landing.listing(client, bucket, SCRATCH)
    all_receipts = landing.read_receipts(client, bucket, [k for keys in receipts.values() for k in keys])
    result["landing_held"] = {
        "entities": len(receipts),
        "receipts": len(all_receipts),
        "data_files": len(data),
        "bytes": sum(r["size_bytes"] for r in all_receipts),
        "entities_with_two_copies": sorted(e for e, k in receipts.items() if len(k) > 1)[:10],
        "per_report": dict(Counter(e.rsplit("_", 1)[0] for e in receipts)),
        "first_last": [min(receipts), max(receipts)],
    }
    result["verify_exit"] = landing.verify(landing_root=SCRATCH)
    note(
        "pse trial Landing (scratch zz_dev_pse)",
        {**result, "extract": COLLECTED.get("pse landing extract"), "verify": COLLECTED.get("pse landing verify")},
    )
    t0 = time.monotonic()
    result_b: dict = {"bronze_exit": [bronze.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]}
    result_b["bronze_minutes"] = round((time.monotonic() - t0) / 60, 1)
    result_b["loads"] = COLLECTED.get("pse Bronze from Landing")
    cat = catalog()
    con = duckdb.connect()
    t0 = time.monotonic()
    for table, name in (("pse_rce_pln", "bronze_rce"), ("pse_his_wlk_cal", "bronze_his")):
        con.register(name, cat.load_table((SCRATCH, table)).scan().to_arrow())
    result_b["scan_minutes"] = round((time.monotonic() - t0) / 60, 1)
    try:
        rejected = cat.load_table((SCRATCH, "pse_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    result_b["rejected"] = [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:5]
    note("pse trial Bronze (scratch)", result_b)
    note("pse trial Bronze profile rce-pln", bronze_profile(con, "bronze_rce"))
    note("pse trial Bronze profile his-wlk-cal", bronze_profile(con, "bronze_his"))
    result_m: dict = {}
    models(con, result_m)
    newest_complete = q(con, "select max(business_date) from pse_daily")[0][0]
    result_m["python_recompute_" + str(newest_complete)] = recompute(client, bucket, all_receipts, str(newest_complete))
    result_m["gold_" + str(newest_complete)] = q(
        con, f"select measure_codes, round(value, 3) from build where date_key = date '{newest_complete}' order by 1"
    )
    report_models(result_m)
    return 0


def report_models(result: dict) -> None:
    silver_keys = [
        "silver_rows_two_builds", "silver_tests_failing_rows", "silver_range", "incomplete_days", "dst_days",
        "daily_per_measure", "negative_or_zero", "interval_nulls_silver",
    ]  # fmt: skip
    note("pse trial Silver (local DuckDB over scratch Bronze)", {k: result.get(k) for k in silver_keys})
    note("pse trial Gold build_pse", {k: v for k, v in result.items() if k not in silver_keys})


def local() -> int:
    """The model part over a synthetic Bronze (three days, one DST day, one day with nulls): no network."""
    con = duckdb.connect()
    rows_rce, rows_his = [], []
    for day, n in (("2025-10-25", 96), ("2025-10-26", 100), ("2025-10-27", 96)):
        start = datetime.fromisoformat(day) - timedelta(hours=2)
        for i in range(n):
            end = start + timedelta(minutes=15 * (i + 1))
            base = (f"rce_pln_{day.replace('-', '')}", day, f"{end:%Y-%m-%d %H:%M:%S}", f"{end:%Y-%m-%d %H:%M:%S}")
            rows_rce.append((*base, "p", "pu", "x", "2025-10-24 12:00:00.000", str(100.0 + i), "20251024T120000Z"))
            vals = [None if day == "2025-10-27" and i > 80 else str(1000.0 + i)] * 17
            rows_his.append((*base, "p", "pu", "x", "2025-10-24 12:00:00.000", *vals, "20251024T120000Z"))
    cols = "entity, business_date, dtime_utc, dtime, period, period_utc, publication_ts, publication_ts_utc"
    con.execute(
        f"create table bronze_rce as select * from (values {','.join(str(r) for r in rows_rce)}) t ({cols}, rce_pln, source_version)"
    )
    his_cols = ", ".join(VALUES["bronze_his"])
    vals = ",".join(str(r).replace("None", "NULL") for r in rows_his)
    con.execute(f"create table bronze_his as select * from (values {vals}) t ({cols}, {his_cols}, source_version)")
    for t in ("bronze_rce", "bronze_his"):
        con.execute(f"alter table {t} add column raw_sha256 varchar default 'abc'")
        con.execute(f"alter table {t} add column ingested_at timestamptz default now()")
    print(json.dumps(bronze_profile(con, "bronze_his"), default=str))
    result: dict = {}
    models(con, result)
    print(json.dumps(result, default=str, indent=1))
    return 0


def cleanup() -> None:
    from ingestion.common.r2 import catalog, req, s3

    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for bucket_var, label in (
        ("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
        ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects"),
    ):
        bucket = req(bucket_var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
    note("pse trial scratch removed", removed)


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "trial"
    if mode == "probe":
        return probe()
    if mode == "local":
        return local()
    status = 1
    try:
        status = trial()
    except Exception as exc:
        import traceback

        note(
            "pse trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1500:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
