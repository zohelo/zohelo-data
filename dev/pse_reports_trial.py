"""Dev trial of pse_reports (#2870, PSE Reports API v2): the full load Landing → Bronze → Silver → Gold into scratch.
Test branch only, never on main.

Landing: the real extractor, every business day from 2024-06-14 to yesterday (Warsaw) for rce-pln and his-wlk-cal,
into zz_dev_pse_reports/01_landing, run twice (the second run asks only the newest 7 days again and must write
nothing), then verify and inspect. Bronze: the real loader into the namespace and lake prefix zz_dev_pse_reports,
twice (the second appends nothing), then a profile of the Bronze input (rows, keys, range, nulls, non-numeric
strings) checked against the receipts. Silver and Gold: the real `dbt run` and `dbt test` of the two Silver models and
the build stage (dev/pse_reports_dbt.py, local DuckDB file attached as `r2`, nothing written to the catalog), twice;
then the fact_grain check by poc_series_key() x date_key, geography keys against production gold.dim_geography (read
only), value ranges, the metrics row counts, spot values for Poland recomputed from the raw Landing bytes, one day
fetched again from the API, and the licence gate on the code lists. Each stage is guarded: an error is annotated
and the next stage runs. Everything written to R2 is removed in `finally`.
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import traceback

import duckdb
import jinja2
import requests

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_pse_reports"
LANDING = f"{SCRATCH}/01_landing"
START = time.monotonic()
CALLS = [0]
_get = requests.Session.get
SPOT_DAYS = ["2024-06-14", "2025-01-15", "2025-03-30", "2025-06-15", "2025-10-26"]
HIS_VALUES = ["demand", "jg", "jga", "jgm", "jgm1", "jgm2", "jgo", "jgw1", "jgw2", "jgz1", "jgz2", "jgz3", "jnwrb", "pv",
              "swm_np", "swm_p", "wi"]  # fmt: skip


def log(message: str) -> None:
    print(f"[{round((time.monotonic() - START) / 60, 1)} min] {message}", file=sys.__stderr__, flush=True)


def counted(self, *args, **kwargs):
    CALLS[0] += 1
    if CALLS[0] % 200 == 0:
        log(f"{CALLS[0]} PSE requests")
    return _get(self, *args, **kwargs)


requests.Session.get = counted


def q(con, sql: str):
    return con.sql(sql).fetchall()


def plain(obj):
    return json.loads(json.dumps(obj, default=str))


def stage(name: str, annotate, call):
    log(f"{name}: start")
    try:
        return call()
    except Exception:  # every stage reports; the clean-up always runs
        annotate("error", f"pse_reports trial · {name} failed", traceback.format_exc()[-1800:])
        return None
    finally:
        log(f"{name}: end")


def objects(client, bucket: str, prefix: str) -> tuple[int, int]:
    count = size = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            count, size = count + 1, size + o["Size"]
    return count, size


def run_landing(extract, mode: str, budget: float = 20) -> tuple[dict, int, float]:
    before, start, old = CALLS[0], time.monotonic(), sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        code = extract.main(["--mode", mode, "--landing-root", LANDING, "--budget-minutes", str(budget)])
    finally:
        sys.stdout = old
    summary = json.loads([ln for ln in buf.getvalue().splitlines() if ln.startswith("{")][-1])
    summary["exit"] = code
    return summary, CALLS[0] - before, round(time.monotonic() - start)


def clean(client, cat, buckets) -> dict:
    removed, failed, dropped = 0, 0, []
    for bucket in buckets:
        keys = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys += [o["Key"] for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            chunk = keys[i : i + 1000]
            try:
                client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
                removed += len(chunk)
            except Exception:  # counted and reported; never stops the clean-up of the rest
                failed += len(chunk)
    with contextlib.suppress(Exception):
        for ident in cat.list_tables(SCRATCH):
            cat.drop_table(ident)
            dropped.append(".".join(ident))
    with contextlib.suppress(Exception):
        cat.drop_namespace(SCRATCH)
    left = sum(objects(client, b, f"{SCRATCH}/")[0] for b in buckets)
    return {"objects_removed": removed, "delete_failures": failed, "objects_left": left, "tables_dropped": dropped}


def raw_days(client, bucket: str, extract) -> tuple[dict, dict]:
    """Every newest Landing receipt, and the raw JSON rows of the spot days (from the exact Landing bytes)."""
    receipts, _ = extract.listing(client, bucket, LANDING)
    held = extract.read_receipts(client, bucket, [rows[-1][1] for rows in receipts.values()])
    spot = {}
    for r in held:
        if r["business_date"] in SPOT_DAYS:
            body = client.get_object(Bucket=bucket, Key=r["object"]["key"])["Body"].read()
            spot[(r["report"], r["business_date"])] = json.loads(body)["value"]
    return {r["entity"]: r for r in held}, spot


def profile_bronze(rce, his, receipts: dict) -> dict:
    con = duckdb.connect()
    con.register("rce", rce)
    con.register("his", his)
    out = {}
    for t, values in (("rce", ["rce_pln"]), ("his", HIS_VALUES)):
        report = "rce_pln" if t == "rce" else "his_wlk_cal"
        expected_rows = sum(r["rows"] for r in receipts.values() if r["entity"].startswith(report))
        out[t] = {
            "rows_entities_files_min_max_day": q(con, f"select count(*), count(distinct entity), count(distinct raw_sha256), "
                                                      f"min(business_date), max(business_date) from {t}")[0],
            "rows_by_receipts": expected_rows,
            "dup_entity_dtime_utc": q(con, f"select count(*) from (select entity, dtime_utc from {t} group by all having count(*) > 1)")[0][0],
            "null_keys": q(con, f"select count(*) from {t} where entity is null or business_date is null or dtime_utc is null "
                                "or raw_sha256 is null or source_version is null")[0][0],
            "periods_per_day": q(con, f"select n, count(*) from (select entity, count(*) n from {t} group by 1) group by 1 order by 1"),
            "value_nulls": {v: n for v in values if (n := q(con, f"select count(*) from {t} where {v} is null")[0][0])},
            "value_non_numeric": {v: x for v in values if (x := q(con, f"select list(distinct {v})[:5] from {t} where {v} is not null "
                                                                       f"and try_cast({v} as double) is null")[0][0])},
            "versions_first_last": q(con, f"select min(source_version), max(source_version) from {t}")[0],
        }
    out["rce"]["negative_rce_rows_min"] = q(con, "select count(*) filter (where cast(rce_pln as double) < 0), min(cast(rce_pln as double)) from rce")[0]
    out["his"]["nulls_by_year_jg_groups"] = q(con, "select left(business_date, 4) y, " + ", ".join(
        f"count(*) filter (where {v} is null)" for v in ("jga", "jgo", "jgm2", "jgw2", "jgz1", "jgz2", "jgz3")) + " from his group by 1 order by 1")
    out["his"]["zero_rows_demand_wi_pv"] = q(con, "select count(*) filter (where cast(demand as double) = 0), count(*) filter (where cast(wi as double) = 0), "
                                                  "count(*) filter (where cast(pv as double) = 0) from his")[0]
    return out


def analyse(path: str, dim, spot: dict) -> dict:
    con = duckdb.connect()
    con.sql(f"attach '{path}' as r2 (read_only)")
    con.register("dim_geography", dim)
    macro = jinja2.Environment(extensions=["jinja2.ext.do"]).from_string((ROOT / "warehouse/macros/poc_blocks.sql").read_text()).module
    series = macro.poc_series_key()  # exactly as the facts build series_key
    out: dict = {"silver": {}}
    for t in ("pse_reports_rce_pln", "pse_reports_his_wlk_cal"):
        out["silver"][t] = {
            "rows_days_min_max": q(con, f"select count(*), count(distinct business_date), min(business_date), max(business_date) from r2.silver.{t}")[0],
            "dup_grain": q(con, f"select count(*) from (select business_date, dtime_utc from r2.silver.{t} group by all having count(*) > 1)")[0][0],
            "null_keys": q(con, f"select count(*) from r2.silver.{t} where entity is null or business_date is null or dtime_utc is null or raw_sha256 is null")[0][0],
            "days_by_periods_expected": q(con, f"select periods_expected, count(distinct business_date) from r2.silver.{t} group by 1 order by 1"),
            "utc_offset_rows": q(con, f"select utc_offset_hours, count(*) from r2.silver.{t} group by 1 order by 1"),
            "dst_days": q(con, f"select strftime(business_date, '%Y-%m-%d'), count(*) from r2.silver.{t} where periods_expected <> 96 group by 1 order by 1"),
        }
    out["silver"]["pse_reports_his_wlk_cal"]["nulls_demand_wi_pv"] = q(con, "select count(*) filter (where demand is null), "
        "count(*) filter (where wi is null), count(*) filter (where pv is null) from r2.silver.pse_reports_his_wlk_cal")[0]
    out["silver"]["pse_reports_rce_pln"]["rce_min_max_negative_rows"] = q(con, "select min(rce_pln), max(rce_pln), count(*) filter (where rce_pln < 0) from r2.silver.pse_reports_rce_pln")[0]
    g = "r2.gold.build_pse_reports"
    gold = {
        "rows_measures_series_min_max_freq": q(con, f"select count(*), count(distinct measure_key), count(distinct {series}), min(date_key), max(date_key), list(distinct frequency) from {g}")[0],
        "grain_duplicates_series_key_x_date_key": q(con, f"select count(*) from (select {series}, date_key from {g} group by all having count(*) > 1)")[0][0],
        "rows_X_and_keys_not_in_dim_geography": q(con, f"select count(*) filter (where geography_key = '_X'), list(distinct geography_key) "
                                                       f"filter (where geography_key not in (select geography_key from dim_geography)) from {g}")[0],
        "geography_keys": q(con, f"select list(distinct geography_key) from {g}")[0][0],
        "per_measure_rows_min_max_value_min_max": q(con, f"select measure_key, count(*), min(date_key), max(date_key), round(min(value), 3), round(max(value), 3) from {g} group by 1 order by 1"),
        "silver_days_left_out_per_measure": q(con, f"""
            with m as (select measure_key, block_id, count(*) as n from {g} group by measure_key, block_id)
            select measure_key, case when block_id = 'rce_pln'
                then (select count(distinct business_date) from r2.silver.pse_reports_rce_pln)
                else (select count(distinct business_date) from r2.silver.pse_reports_his_wlk_cal) end - n
            from m order by 1"""),
        "days_left_out_list": q(con, f"""
            select 'his' r, strftime(business_date, '%Y-%m-%d') d from (select distinct business_date from r2.silver.pse_reports_his_wlk_cal)
            where business_date not in (select date_key from {g} where block_id = 'his_wlk_cal' and measure_codes = 'wi:daily_mean')
            union all select 'rce', strftime(business_date, '%Y-%m-%d') from (select distinct business_date from r2.silver.pse_reports_rce_pln)
            where business_date not in (select date_key from {g} where block_id = 'rce_pln') order by 1, 2 limit 10"""),
        "null_nan_zero_values": q(con, f"select count(*) filter (where value is null or isnan(value)), count(*) filter (where value = 0) from {g}")[0],
        "metrics_rows_per_kpi_measure": q(con, f"select measure_key, count(*) from {g} where is_total and geography_key <> '_X' group by 1 order by 1"),
        "metrics_rows_per_area_day_max": q(con, f"select max(n) from (select measure_key, geography_key, date_key, count(*) n from {g} group by all)")[0][0],
    }
    import re

    bounds_text = (ROOT / "warehouse/macros/value_range/pse_reports.sql").read_text()
    bounds = re.findall(r"\['(\w+)', '%', '([-0-9.e]+)', '([-0-9.e]+)'\]", bounds_text)
    kpi_measure = {"electricity_price_rce": "rce_pln", "electricity_demand": ":demand:", "wind_generation": ":wi:", "pv_generation": ":pv:"}
    gold["value_range_rows_outside"] = {k: q(con, f"select count(*) from {g} where measure_key like '%{kpi_measure[k]}%' and (value < {lo} or value > {hi})")[0][0]
                                        for k, lo, hi in bounds}
    # spot values: Gold against the mean of the raw Landing rows (exact bytes, json floats)
    spots = []
    for (report, day), rows in sorted(spot.items()):
        fields = ["rce_pln"] if report == "rce-pln" else ["demand", "wi", "pv"]
        for f in fields:
            vals = [r[f] for r in rows]
            raw_mean = round(sum(vals) / len(vals), 6) if all(v is not None for v in vals) else None
            key = f"pse_reports:{'rce_pln:PLN/MWh' if f == 'rce_pln' else 'his_wlk_cal:MW'}:{f}:daily_mean"
            got = q(con, f"select value from {g} where measure_key = '{key}' and date_key = date '{day}'")
            gv = got[0][0] if got else None
            spots.append([day, f, len(vals), raw_mean, gv, None if gv is None or raw_mean is None else abs(gv - raw_mean) < 1e-6,
                          [min(vals), max(vals)] if raw_mean is not None else None])
    out["spots"] = spots
    out["gold"] = gold
    return out


def main() -> int:
    from ingestion.common import bronze_files as bf
    from ingestion.common import bulk_bronze
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.common.report import annotate
    from ingestion.to_bronze.pse_reports import load
    from ingestion.to_landing.pse_reports import extract
    from warehouse.publish import licensed_sources

    from dev import pse_reports_dbt as trial_dbt

    # GitHub keeps 10 annotations per step: the extractor's and loader's own are silenced (their summaries are in
    # this trial's annotations).
    extract.annotate = load.annotate = bulk_bronze.annotate = lambda *_args, **_kw: None

    client, cat = s3(), bf.retry(catalog, "catalog")
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    buckets = (landing_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
    annotate("notice", "pse_reports trial · scratch cleaned before the start", clean(client, cat, buckets))
    status = 0
    try:
        held: dict = {}

        def landing():
            first, calls1, secs1 = run_landing(extract, "load", 200)
            between = objects(client, landing_bucket, f"{LANDING}/")
            second, calls2, secs2 = run_landing(extract, "load")
            after = objects(client, landing_bucket, f"{LANDING}/")
            verify, _, _ = run_landing(extract, "verify")
            inspect, _, _ = run_landing(extract, "inspect")
            receipts, spot = raw_days(client, landing_bucket, extract)
            held.update({"receipts": receipts, "spot": spot})
            data = [r for r in receipts.values()]
            ok = first["exit"] == 0 and second["exit"] == 0 and between == after and verify["exit"] == 0 and inspect["entities_missing"] == 0
            annotate("notice" if ok else "warning", "pse_reports trial · Landing (full load, twice)", plain({
                "first": first, "first_requests": calls1, "first_minutes": round(secs1 / 60, 1),
                "second": second, "second_requests": calls2, "second_seconds": secs2,
                "objects_bytes_before_second_and_after": [between, after], "verify": verify, "inspect": inspect,
                "data_files": len(data), "data_bytes": sum(r["size_bytes"] for r in data),
                "files_bytes_per_report": {p: [sum(1 for r in data if r["entity"].startswith(p)), sum(r["size_bytes"] for r in data if r["entity"].startswith(p))]
                                           for p in ("rce_pln", "his_wlk_cal")},
                "rows_by_receipts_92_96_100": {p: sorted({r["rows"] for r in data if r["entity"].startswith(p)}) for p in ("rce_pln", "his_wlk_cal")},
                "multi_page_days": [r["entity"] for r in data if len(r["pages"]) > 1][:5]}))
            return ok

        landing_ok = stage("landing", annotate, landing)
        status |= 0 if landing_ok else 1

        def bronze_stage():
            ids = {r: (SCRATCH, load.table_of(r)) for r in load.FIELDS}
            load.load(200, landing_root=LANDING, namespace=SCRATCH)
            first = {r: [len(bf.loaded_hashes(cat.load_table(i))), cat.load_table(i).scan().to_arrow().num_rows] for r, i in ids.items()}
            code2 = load.load(20, landing_root=LANDING, namespace=SCRATCH)
            tables = {r: cat.load_table(i).scan().to_arrow() for r, i in ids.items()}
            second = {r: [len(bf.loaded_hashes(cat.load_table(i))), tables[r].num_rows] for r, i in ids.items()}
            try:
                rejected = cat.load_table((SCRATCH, load.REJECTED)).scan().to_arrow().to_pylist()
            except Exception:  # no rejections table when nothing was rejected
                rejected = []
            prof = profile_bronze(tables["rce_pln"], tables["his_wlk_cal"], held.get("receipts", {}))
            ok = not rejected and first == second and code2 == 0
            annotate("notice" if ok else "warning", "pse_reports trial · Bronze (twice) and profile", plain({
                "files_rows_after_first": first, "files_rows_after_second": second, "second_exit": code2,
                "rejected": len(rejected), "rejected_errors": [r.get("error") for r in rejected][:3], "profile": prof}))
            return tables

        tables = stage("bronze", annotate, bronze_stage)
        if tables is None:
            return 1
        with tempfile.TemporaryDirectory(prefix="pse-dbt-") as workdir:
            built = stage("dbt", annotate, lambda: trial_dbt.build(tables["rce_pln"], tables["his_wlk_cal"], workdir))
            if built is None:
                return 1
            path, runs = built
            failing = {k: [r for r in v if r[1] not in ("success", "pass")] for k, v in runs.items() if k.startswith(("run", "test"))}
            bad = any(failing.values()) or runs["rows1"] != runs["rows2"]
            status |= 1 if bad else 0
            annotate("warning" if bad else "notice", "pse_reports trial · dbt run and test (twice)", plain({
                "rows_silver_rce_his_gold_run1_run2": [runs["rows1"], runs["rows2"]],
                "models_run1": [r[:2] + ([r[3]] if r[3] else []) for r in runs["run1"]],
                "tests_run1_status": {s: sum(1 for r in runs["test1"] if r[1] == s) for s in {r[1] for r in runs["test1"]}},
                "tests_run1": [n[:48] for n, _s, _f, _m in runs["test1"]],
                "tests_run2_status": sorted({r[1] for r in runs["test2"]}), "tests_count": [len(runs["test1"]), len(runs["test2"])],
                "not_passing": failing}))

            def gold_stage():
                dim = cat.load_table(("gold", "dim_geography")).scan(selected_fields=("geography_key",)).to_arrow()
                res = analyse(path, dim, held.get("spot", {}))
                sil = res["silver"]
                annotate("warning" if any(v["dup_grain"] or v["null_keys"] for v in sil.values()) else "notice",
                         "pse_reports trial · Silver profile", plain(sil))
                gold = res["gold"]
                print(json.dumps(plain(res), indent=1))
                spots_ok = bool(res["spots"]) and all(s[5] for s in res["spots"])
                bad_gold = gold["grain_duplicates_series_key_x_date_key"] or any(gold["rows_X_and_keys_not_in_dim_geography"]) or not spots_ok
                bad_gold = bad_gold or any(gold["value_range_rows_outside"].values())
                annotate("warning" if bad_gold else "notice", "pse_reports trial · Gold build stage", plain(gold))
                # one day fetched again from the API: the Landing copy is the source's bytes
                session = requests.Session()
                session.headers.update({"User-Agent": extract.USER_AGENT, "Accept": "application/json"})
                again = extract.get(session, f"{extract.API}/rce-pln", {"$filter": "business_date eq '2025-03-30'"})
                landed = held["receipts"].get("rce_pln_20250330", {})
                from hashlib import sha256

                from warehouse import seed_rows

                sources, licences = seed_rows.rows("source"), seed_rows.rows("licence")
                row = next(r for r in sources if r["source_key"] == "pse_reports")
                annotate("notice" if spots_ok else "warning", "pse_reports trial · spot values (Poland), API re-read, licence gate", plain({
                    "spot_day_field_periods_rawmean_gold_equal_rawminmax": res["spots"],
                    "rce_pln_20250330_api_sha_equals_landing": sha256(again).hexdigest() == landed.get("sha256"),
                    "source_csv_licence_key": row["licence_key"],
                    "licence_row": next((r for r in licences if r["licence_key"] == row["licence_key"]), None),
                    "licensed_by_gate": "pse_reports" in licensed_sources(sources, licences)}))
                return 1 if bad_gold else 0

            status |= stage("silver and gold analysis", annotate, gold_stage) or 0
    finally:
        annotate("notice", "pse_reports trial · cleanup", clean(client, cat, buckets))
        log("done")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
