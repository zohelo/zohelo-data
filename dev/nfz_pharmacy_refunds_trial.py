"""Dev trial of nfz_pharmacy_refunds (#2930): the full load Landing → Bronze → Silver → Gold into scratch.

Landing: the real extractor, all 255 entities, into zz_dev_nfz_pharmacy_refunds/01_landing, run twice (the second run
counts its requests and the objects written), then verify. Bronze: the real loader into the namespace
zz_dev_nfz_pharmacy_refunds, run twice, then a profile of the Bronze input. Silver and Gold: the real `dbt run` and
`dbt test` of the Silver model and the Gold block (dev/nfz_pharmacy_refunds_dbt.py, local DuckDB file attached as
`r2`, nothing written to the catalog), twice; then the fact_grain check by poc_series_key() x date_key, geography
keys against production gold.dim_geography (read only), the KPI one-row check, spot values against the probe
(43310762847), the 2024 reconciliation with /yearly-provisions and the provinces-vs-Poland partition (reported, not
forced), and the licence gate on the seeds. Each stage is guarded: an error is annotated and the next stage runs.
Everything written to R2 is removed in `finally`. Deleted before merge.
"""

from __future__ import annotations

import contextlib
import csv
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
SCRATCH = "zz_dev_nfz_pharmacy_refunds"
LANDING = f"{SCRATCH}/01_landing"
VALUES = ["quantity", "refund", "donation", "contribution_of_patient", "value"]
PLN = "nfz_pharmacy_refunds:monthly_provisions:PLN:"
START = time.monotonic()
CALLS = [0]
_get = requests.Session.get
# Probe 43310762847 (2024-12, no ATC filter): refund as the API answered for Poland, mazowieckie (07), śląskie (12).
PROBE_2024_12_REFUND = {"PL": 1140260170.65, "TERC:14": 158076686.08, "TERC:24": 136848164.61}


def log(message: str) -> None:
    print(f"[{round((time.monotonic() - START) / 60, 1)} min] {message}", file=sys.__stderr__, flush=True)


def counted(self, *args, **kwargs):
    CALLS[0] += 1
    if CALLS[0] % 50 == 0:
        log(f"{CALLS[0]} NFZ requests")
    return _get(self, *args, **kwargs)


requests.Session.get = counted


def q(con, sql: str):
    return con.sql(sql).fetchall()


def plain(obj):
    return json.loads(json.dumps(obj, default=str))


def stage(name: str, annotate, call):
    """Run one stage; an error is annotated with its traceback tail and returns None."""
    log(f"{name}: start")
    try:
        return call()
    except Exception:  # every stage reports; the clean-up always runs
        annotate("error", f"nfz_pharmacy_refunds trial · {name} failed", traceback.format_exc()[-1800:])
        return None
    finally:
        log(f"{name}: end")


def objects(client, bucket: str, prefix: str) -> tuple[int, int]:
    count = size = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            count, size = count + 1, size + o["Size"]
    return count, size


def run_landing(extract, mode: str, budget: float = 10) -> tuple[dict, int, float]:
    before, start, old = CALLS[0], time.monotonic(), sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        code = extract.main(["--mode", mode, "--landing-root", LANDING, "--budget-minutes", str(budget)])
    finally:
        sys.stdout = old
    summary = json.loads([ln for ln in buf.getvalue().splitlines() if ln.startswith("{")][-1])
    summary["exit"] = code
    return summary, CALLS[0] - before, round(time.monotonic() - start)


def retried(call, what: str, attempts: int = 6):
    """R2 throttles under load (429/503, trials 43312973452, 43318649265): retry with backoff."""
    from botocore.exceptions import ClientError

    for attempt in range(attempts):
        try:
            return call()
        except ClientError as exc:
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") or 0)
            if (status != 429 and status < 500) or attempt == attempts - 1:
                raise
            log(f"{what}: R2 {status}, retry {attempt + 1}")
            time.sleep(10 * 2**attempt)
    return None


def clean(client, cat, buckets) -> dict:
    """Remove everything under the scratch prefix in both buckets and drop the scratch tables; count what is left."""
    removed, failed, dropped = 0, 0, []
    for bucket in buckets:
        keys = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys += [o["Key"] for o in page.get("Contents", [])]
        for key in keys:
            try:
                retried(lambda key=key, bucket=bucket: client.delete_object(Bucket=bucket, Key=key), "delete", 5)
                removed += 1
            except Exception:  # counted and reported; never stops the clean-up of the rest
                failed += 1
    with contextlib.suppress(Exception):
        for ident in cat.list_tables(SCRATCH):
            cat.drop_table(ident)
            dropped.append(".".join(ident))
    with contextlib.suppress(Exception):
        cat.drop_namespace(SCRATCH)
    left = sum(objects(client, b, f"{SCRATCH}/")[0] for b in buckets)
    return {"objects_removed": removed, "delete_failures": failed, "objects_left": left, "tables_dropped": dropped}


def profile_bronze(bronze) -> dict:
    con = duckdb.connect()
    con.register("bronze", bronze)
    ym = "year || '-' || lpad(month, 2, '0')"
    zero_all = " and ".join(f"try_cast({v} as double) = 0" for v in VALUES)
    return {
        "rows_entities_files_min_max": q(con, f"select count(*), count(distinct entity), count(distinct raw_sha256), min({ym}), max({ym}) from bronze")[0],
        "null_keys": q(con, "select count(*) from bronze where year is null or month is null or entity is null")[0][0],
        "duplicate_entity_month": q(con, "select count(*) from (select entity, year, month from bronze group by all having count(*) > 1)")[0][0],
        "per_value_null_zero_nonnumeric": {v: q(con, f"select count(*) filter (where {v} is null), count(*) filter (where try_cast({v} as double) = 0), "
                                                  f"list(distinct {v}) filter (where {v} is not null and try_cast({v} as double) is null) from bronze")[0]
                                           for v in VALUES},
        "rows_all_five_zero": q(con, f"select count(*), list(distinct entity) from bronze where {zero_all}")[0],
        "rows_quantity_zero_value_nonzero": q(con, "select count(*) from bronze where try_cast(quantity as double) = 0 and try_cast(value as double) <> 0")[0][0],
        "area_codes": q(con, "select list(distinct area_code order by area_code) from bronze")[0][0],
        "atc_codes": q(con, "select list(distinct atc_code order by atc_code) from bronze")[0][0],
        "atc_names": q(con, "select count(distinct atc_name) from bronze")[0][0],
        "entities_by_months": q(con, "select n, count(*) from (select entity, count(*) n from bronze group by 1) group by 1 order by 1"),
        "meta_count_distinct": q(con, "select list(distinct meta_count) from bronze")[0][0],
        "source_version": q(con, "select list(distinct source_version) from bronze")[0][0],
    }


def analyse(path: str, dim, yearly) -> dict:
    """Silver and Gold as dbt built them (local file), production dim_geography keys, the /yearly-provisions answer."""
    con = duckdb.connect()
    con.sql(f"attach '{path}' as r2 (read_only)")
    con.register("dim_geography", dim)
    con.sql("create view silver as select * from r2.silver.nfz_ra_monthly_provisions")
    con.sql("create view gold as select * from r2.gold.build_nfz_pharmacy_refunds")
    macro = jinja2.Environment(extensions=["jinja2.ext.do"]).from_string((ROOT / "warehouse/macros/poc_blocks.sql").read_text()).module
    series = macro.poc_series_key()  # exactly as poc_fact_monthly builds series_key
    out = {"silver": {
        "rows_entities_min_max": q(con, "select count(*), count(distinct entity), min(period_start), max(period_start) from silver")[0],
        "duplicate_grain": q(con, "select count(*) from (select area_code, atc_code, period_start from silver group by all having count(*) > 1)")[0][0],
        "null_keys": q(con, "select count(*) from silver where entity is null or area_code is null or atc_code is null "
                            "or period_start is null or source_version is null or raw_sha256 is null")[0][0],
        "value_nulls": q(con, "select " + ", ".join(f"count(*) filter (where {v} is null)" for v in VALUES) + " from silver")[0],
        "value_zeros": q(con, "select " + ", ".join(f"count(*) filter (where {v} = 0)" for v in VALUES) + " from silver")[0],
        "value_negatives": q(con, "select " + ", ".join(f"count(*) filter (where {v} < 0)" for v in VALUES) + " from silver")[0],
        "value_eq_refund_plus_patient_and_plus_donation": q(con, """select count(*),
            count(*) filter (where abs(value - refund - contribution_of_patient) < 0.015),
            count(*) filter (where abs(value - refund - contribution_of_patient - donation) < 0.015) from silver""")[0],
        "months_per_entity": q(con, "select n, count(*) from (select entity, count(*) n from silver group by 1) group by 1"),
    }}
    out["gold"] = {
        "rows_measures_series_min_max_freq": q(con, f"select count(*), count(distinct measure_key), count(distinct {series}), "
                                                    "min(date_key), max(date_key), list(distinct frequency) from gold")[0],
        "rows_per_block": q(con, "select block_id, count(*) from gold group by 1"),
        "grain_duplicates_series_key_x_date_key": q(con, f"select count(*) from (select {series}, date_key from gold group by all having count(*) > 1)")[0][0],
        "rows_X_and_keys_not_in_dim_geography": q(con, "select count(*) filter (where geography_key = '_X'), list(distinct geography_key) "
                                                       "filter (where geography_key not in (select geography_key from dim_geography)) from gold")[0],
        "geography_keys": q(con, "select count(distinct geography_key) from gold")[0][0],
        "kpi_rows_per_area_month_max": q(con, f"select max(n) from (select geography_key, date_key, measure_key, count(*) n from gold where is_total "
                                              f"and measure_key in ('{PLN}refund', '{PLN}contribution_of_patient', "
                                              "'nfz_pharmacy_refunds:monthly_provisions:packages:quantity') group by all)")[0][0],
        "rows_per_measure_total": q(con, "select measure_key, unit_code, count(*), count(*) filter (where is_total) from gold group by all order by 1"),
        "spot_2024_12_refund_vs_probe": [[g, v, PROBE_2024_12_REFUND[g], v == PROBE_2024_12_REFUND[g]] for g, v in q(con,
            f"select geography_key, value from gold where date_key = date '2024-12-01' and is_total and measure_key = '{PLN}refund' "
            "and geography_key in ('PL', 'TERC:14', 'TERC:24') order by 1")],
        "spot_2024_12_PL_TERC14_TERC24_all_measures": q(con, "select geography_key, measure_codes, value from gold where date_key = date '2024-12-01' "
                                                             "and is_total and geography_key in ('PL', 'TERC:14', 'TERC:24') order by 1, 2"),
        "spot_PL_2024_12_refund_atc_C": q(con, f"select value, breakdown_json, breakdown_label_json from gold where date_key = date '2024-12-01' "
                                               f"and geography_key = 'PL' and measure_key = '{PLN}refund' and breakdown_json like '%\"C\"%'"),
    }
    out["provinces_vs_PL"] = q(con, """
        with s as (select atc_code, period_start, count(*) n, sum(refund) refund, sum(value) value, sum(quantity) quantity
                   from silver where area_code <> 'PL' group by 1, 2)
        select count(*), count(*) filter (where n = 16), max(abs(p.refund - s.refund)), max(abs(p.value - s.value)),
               max(abs(p.quantity - s.quantity)),
               count(*) filter (where abs(p.refund - s.refund) > 0.05 or abs(p.value - s.value) > 0.05)
        from s join silver as p on p.area_code = 'PL' and p.atc_code = s.atc_code and p.period_start = s.period_start""")[0]
    out["PL_refund_total_minus_14_groups_2024"] = q(con, """
        select round(sum(refund) filter (where atc_code = '_T') - sum(refund) filter (where atc_code <> '_T'), 2)
        from silver where area_code = 'PL' and year(period_start) = 2024""")[0][0]
    m = q(con, "select count(*), sum(refund), sum(value), sum(quantity), sum(donation), sum(contribution_of_patient) "
               "from silver where area_code = 'PL' and atc_code = '_T' and year(period_start) = 2024")[0]
    y = next(r for r in yearly["data"] if r["year"] == 2024)
    out["recon_2024_sum_of_months_vs_yearly_diff"] = {
        "months": m[0], "refund": [round(m[1], 2), y["refund"], round(m[1] - y["refund"], 2)],
        "value": [round(m[2], 2), y["value"], round(m[2] - y["value"], 2)],
        "quantity": [round(m[3], 5), y["quantity"], round(m[3] - y["quantity"], 5)],
        "donation": [round(m[4], 2), y["donation"], round(m[4] - y["donation"], 2)],
        "contribution_of_patient": [round(m[5], 2), y["contribution-of-patient"], round(m[5] - y["contribution-of-patient"], 2)]}
    return out


def main() -> int:
    from ingestion.common import bronze_files as bf
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.common.report import annotate
    from ingestion.to_bronze.nfz_pharmacy_refunds import load
    from ingestion.to_landing.nfz_pharmacy_refunds import extract
    from warehouse.publish import licensed_sources

    from dev import nfz_pharmacy_refunds_dbt as trial_dbt
    from ingestion.common import bulk_bronze

    # GitHub keeps 10 notices per step: the extractor's and loader's own annotations are silenced here (their
    # summaries are in this trial's annotations).
    extract.annotate = bulk_bronze.annotate = lambda *_args, **_kw: None

    client, cat = s3(), bf.retry(catalog, "catalog")
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    buckets = (landing_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
    annotate("notice", "nfz_pharmacy_refunds trial · scratch cleaned before the start", clean(client, cat, buckets))
    try:
        def landing():
            first, calls1, secs1 = run_landing(extract, "load", 280)
            between = objects(client, landing_bucket, f"{LANDING}/")
            second, calls2, secs2 = run_landing(extract, "load")
            after = objects(client, landing_bucket, f"{LANDING}/")
            verify, _, _ = run_landing(extract, "verify")
            inspect, _, _ = run_landing(extract, "inspect")
            parts: dict[str, int] = {}
            files = size = 0
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing_bucket, Prefix=f"{LANDING}/"):
                for o in page.get("Contents", []):
                    if not o["Key"].endswith(".receipt.json"):
                        files, size = files + 1, size + o["Size"]
                        parts[o["Key"].split("/")[-2]] = parts.get(o["Key"].split("/")[-2], 0) + 1
            ok = first["exit"] == 0 and second["exit"] == 0 and between == after and verify["exit"] == 0
            annotate("notice" if ok else "warning", "nfz_pharmacy_refunds trial · Landing (full load, twice)", plain({
                "first": first, "first_requests": calls1, "first_minutes": round(secs1 / 60, 1),
                "second": second, "second_requests": calls2, "second_seconds": secs2,
                "objects_bytes_before_second_and_after": [between, after], "verify": verify, "inspect": inspect,
                "data_files": files, "data_bytes": size, "entities": len(parts),
                "entities_by_pages": sorted({v: sum(1 for x in parts.values() if x == v) for v in parts.values()}.items())}))

        def changelog():
            """How often the data move: the documentation's Changelog page (read only, two GETs, 1 s apart)."""
            import html
            import re

            session = requests.Session()
            session.headers.update({"User-Agent": extract.USER_AGENT})
            root = f"{extract.API}/"
            time.sleep(1)
            page = session.get(root, timeout=60)
            links = sorted({h for h in re.findall(r'href="([^"]+)"', page.text) if "changelog" in h.lower()})
            found = {"root_status": page.status_code, "changelog_links": links[:5]}
            if links:
                time.sleep(1)
                target = requests.compat.urljoin(root, links[0])
                doc = session.get(target, timeout=60)
                text = re.sub(r"(?s)<(script|style)[^>]*>.*?</\1>", " ", doc.text)
                text = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text))).strip()
                start = text.lower().find("changelog", text.lower().find("copyright"))
                found.update({"url": target, "status": doc.status_code, "text": text[max(0, start) : max(0, start) + 2600]})
            annotate("notice", "nfz_pharmacy_refunds trial · API changelog (release rhythm)", found)

        stage("changelog", annotate, changelog)
        stage("landing", annotate, landing)
        table_id = (SCRATCH, load.TABLE)

        def bronze_stage():
            retried(lambda: load.load(0, landing_root=LANDING, namespace=SCRATCH), "bronze load 1", 4)
            files1, rows1 = len(bf.loaded_hashes(cat.load_table(table_id))), cat.load_table(table_id).scan().to_arrow().num_rows
            retried(lambda: load.load(0, landing_root=LANDING, namespace=SCRATCH), "bronze load 2", 4)
            bronze = cat.load_table(table_id).scan().to_arrow()
            files2 = len(bf.loaded_hashes(cat.load_table(table_id)))
            try:
                rejected = cat.load_table((SCRATCH, load.REJECTED)).scan().to_arrow().to_pylist()
            except Exception:  # no rejections table when nothing was rejected
                rejected = []
            annotate("notice" if not rejected and files1 == files2 and rows1 == bronze.num_rows else "warning",
                     "nfz_pharmacy_refunds trial · Bronze (twice) and profile", plain({
                         "files_rows_after_first": [files1, rows1], "files_rows_after_second": [files2, bronze.num_rows],
                         "rejected": len(rejected), "rejected_errors": [r.get("error") for r in rejected][:3],
                         "profile": profile_bronze(bronze)}))
            return bronze

        bronze = stage("bronze", annotate, bronze_stage)
        if bronze is None or bronze.num_rows == 0:
            return 1
        with tempfile.TemporaryDirectory(prefix="nfz-dbt-") as workdir:
            built = stage("dbt", annotate, lambda: trial_dbt.build(bronze, workdir))
            if built is None:
                return 1
            path, runs = built
            failing = {k: [r for r in v if r[1] not in ("success", "pass")] for k, v in runs.items() if k.startswith(("run", "test"))}
            annotate("warning" if any(failing.values()) or runs["rows1"] != runs["rows2"] else "notice",
                     "nfz_pharmacy_refunds trial · dbt run and test (twice)", plain({
                         "rows_silver_gold_run1_run2": [runs["rows1"], runs["rows2"]],
                         "models": runs["run1"], "tests_run1": [[n[:60], s, f] for n, s, f in runs["test1"]],
                         "not_passing": failing}))

            def gold_stage():
                dim = cat.load_table(("gold", "dim_geography")).scan(selected_fields=("geography_key",)).to_arrow()
                session = requests.Session()
                session.headers.update({"User-Agent": extract.USER_AGENT, "Accept": "application/json"})
                _, yearly, _ = extract.get_json(session, "yearly-provisions", page=1, limit=25)
                res = analyse(path, dim, yearly)
                annotate("warning" if res["silver"]["duplicate_grain"] or res["silver"]["null_keys"] else "notice",
                         "nfz_pharmacy_refunds trial · Silver profile", plain(res["silver"]))
                gold = res["gold"]
                annotate("warning" if gold["grain_duplicates_series_key_x_date_key"] or any(gold["rows_X_and_keys_not_in_dim_geography"]) else "notice",
                         "nfz_pharmacy_refunds trial · Gold block", plain(gold))
                with open(ROOT / "warehouse/seeds/source.csv", newline="") as s, open(ROOT / "warehouse/seeds/licence.csv", newline="") as lic:
                    sources, licences = list(csv.DictReader(s)), list(csv.DictReader(lic))
                row = next(r for r in sources if r["source_key"] == "nfz_pharmacy_refunds")
                annotate("notice", "nfz_pharmacy_refunds trial · reconciliation, partition, licence gate", plain({
                    "recon_2024_sum_of_months_vs_yearly_diff": res["recon_2024_sum_of_months_vs_yearly_diff"],
                    "provinces_vs_PL_pairs_16areas_maxdiff_refund_value_quantity_off": res["provinces_vs_PL"],
                    "PL_refund_total_minus_14_groups_2024": res["PL_refund_total_minus_14_groups_2024"],
                    "source_csv_licence_key": row["licence_key"],
                    "licence_row": next((r for r in licences if r["licence_key"] == row["licence_key"]), None),
                    "licensed_by_gate": "nfz_pharmacy_refunds" in licensed_sources(sources, licences)}))
                print(json.dumps(plain(res), indent=1))

            stage("silver and gold analysis", annotate, gold_stage)
    finally:
        annotate("notice", "nfz_pharmacy_refunds trial · cleanup", clean(client, cat, buckets))
        log("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
