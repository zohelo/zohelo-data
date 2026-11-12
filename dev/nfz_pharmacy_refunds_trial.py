"""Dev trial of nfz_pharmacy_refunds (#2930): the full load Landing → Bronze → Silver → Gold into scratch.

Landing (the real extractor, all 255 entities) into zz_dev_nfz_pharmacy_refunds/01_landing, run twice (second run:
requests counted, nothing written), then verify; Bronze (the real loader) into the namespace
zz_dev_nfz_pharmacy_refunds, run twice; Bronze profile; the Silver SQL, its tests (unique, not_null,
accepted_values) and tests/nfz_pharmacy_refunds_entities.sql in DuckDB on the scratch Bronze, built twice; the Gold
build SQL with the source's geography macro, the fact_grain check by series_key x date_key, areas against
production gold.dim_geography (read only), the KPI one-row check, spot values and the 2024 reconciliation with
/yearly-provisions (printed, not stored). Everything written is removed in `finally`. Deleted before merge.
"""

from __future__ import annotations

import contextlib
import csv
import io
import json
from pathlib import Path
import sys
import time

import duckdb
import jinja2
import requests

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_nfz_pharmacy_refunds"
LANDING = f"{SCRATCH}/01_landing"
VALUES = ["quantity", "refund", "donation", "contribution_of_patient", "value"]
PLN = "nfz_pharmacy_refunds:monthly_provisions:PLN:"
CALLS = [0]
_get = requests.Session.get


def counted(self, *args, **kwargs):
    CALLS[0] += 1
    return _get(self, *args, **kwargs)


requests.Session.get = counted
MACROS = jinja2.Environment().from_string((ROOT / "warehouse/macros/sources/nfz_pharmacy_refunds.sql").read_text()).module


def render(path: str, refs: dict[str, str]) -> str:
    env = jinja2.Environment()
    env.globals.update(config=lambda **_: "", ref=lambda name: refs[name], source=lambda _s, _t: "bronze",
                       **{n: getattr(MACROS, n) for n in dir(MACROS) if n.startswith("poc_nfz")})
    return env.from_string((ROOT / path).read_text()).render()


def q(con, sql: str):
    return con.sql(sql).fetchall()


def analyse(bronze, dim, yearly) -> dict:
    """The DuckDB part (pure: Bronze and gold.dim_geography as Arrow, the /yearly-provisions answer)."""
    con = duckdb.connect()
    con.register("bronze_arrow", bronze)
    con.sql("create table bronze as select * from bronze_arrow")
    con.register("dim_geography", dim)
    ym = "year || '-' || lpad(month, 2, '0')"
    out: dict = {"bronze": {
        "rows_entities_files_min_max": q(con, f"select count(*), count(distinct entity), count(distinct raw_sha256), min({ym}), max({ym}) from bronze")[0],
        "null_keys": q(con, "select count(*) from bronze where year is null or month is null or entity is null")[0][0],
        "duplicate_entity_month": q(con, "select count(*) from (select entity, year, month from bronze group by all having count(*) > 1)")[0][0],
        "nulls_zeros_nonnumeric": {v: q(con, f"select count(*) filter (where {v} is null), count(*) filter (where try_cast({v} as double) = 0), "
                                             f"list(distinct {v}) filter (where {v} is not null and try_cast({v} as double) is null) from bronze")[0]
                                   for v in VALUES},
        "area_codes": q(con, "select list(distinct area_code order by area_code) from bronze")[0][0],
        "atc_codes": q(con, "select list(distinct atc_code order by atc_code) from bronze")[0][0],
        "entities_by_months": q(con, "select n, count(*) from (select entity, count(*) n from bronze group by 1) group by 1 order by 1"),
    }}
    silver_sql = render("warehouse/models/silver/nfz_pharmacy_refunds/nfz_ra_monthly_provisions.sql", {})
    con.sql("create table silver as " + silver_sql)
    entities_test = q(con, render("warehouse/tests/nfz_pharmacy_refunds_entities.sql", {"nfz_ra_monthly_provisions": "silver"}))
    partition_test = q(con, render("warehouse/tests/nfz_pharmacy_refunds_partition.sql", {"nfz_ra_monthly_provisions": "silver"}))
    silver = {
        "rows_entities_min_max": q(con, "select count(*), count(distinct entity), min(period_start), max(period_start) from silver")[0],
        "rebuild_rows": q(con, f"select count(*) from ({silver_sql})")[0][0],
        "unique_failures": q(con, "select count(*) from (select area_code, atc_code, period_start from silver group by all having count(*) > 1)")[0][0],
        "not_null_failures": q(con, "select count(*) from silver where entity is null or area_code is null or atc_code is null "
                                    "or period_start is null or source_version is null or raw_sha256 is null")[0][0],
        "area_codes": q(con, "select list(distinct area_code order by area_code) from silver")[0][0],
        "atc_codes": q(con, "select list(distinct atc_code order by atc_code) from silver")[0][0],
        "entities_test_failures": entities_test[:10],
        "partition_test_failures": [len(partition_test), partition_test[:5]],
        "value_identity_rows_total_eq_refund_plus_patient_eq_plus_donation": q(con, """select count(*),
            count(*) filter (where abs(value - refund - contribution_of_patient) < 0.015),
            count(*) filter (where abs(value - refund - contribution_of_patient - donation) < 0.015) from silver""")[0],
        "zeros_per_value": q(con, "select " + ", ".join(f"count(*) filter (where {v} = 0)" for v in VALUES) + " from silver")[0],
        "negatives_per_value": q(con, "select " + ", ".join(f"count(*) filter (where {v} < 0)" for v in VALUES) + " from silver")[0],
        "value_nulls": q(con, "select " + ", ".join(f"count(*) filter (where {v} is null)" for v in VALUES) + " from silver")[0],
        "source_version": q(con, "select list(distinct source_version) from silver")[0][0],
    }
    out["silver"] = silver
    out["silver_bad"] = bool(silver["unique_failures"] or silver["not_null_failures"] or entities_test or partition_test
                             or silver["rebuild_rows"] != silver["rows_entities_min_max"][0])
    con.sql(f"create table map_geography as with {MACROS.poc_geography__nfz_pharmacy_refunds()} select * from nfz_pharmacy_refunds")
    con.sql("create table gold as " + render("warehouse/models/gold/poc_build_nfz_pharmacy_refunds.sql",
                                             {"nfz_ra_monthly_provisions": "silver", "poc_map_geography": "map_geography"}))
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    out["gold"] = {
        "rows_measures_series_min_max_freq": q(con, f"select count(*), count(distinct measure_key), count(distinct {series}), "
                                                    "min(date_key), max(date_key), list(distinct frequency) from gold")[0],
        "grain_duplicates": q(con, f"select count(*) from (select {series}, date_key from gold group by all having count(*) > 1)")[0][0],
        "rows_X_and_keys_not_in_dim_geography": q(con, "select count(*) filter (where geography_key = '_X'), list(distinct geography_key) "
                                                       "filter (where geography_key not in (select geography_key from dim_geography)) from gold")[0],
        "areas_mapped_to_dim_geography": q(con, "select count(distinct source_code) from map_geography semi join dim_geography using (geography_key)")[0][0],
        "kpi_rows_per_area_month_max": q(con, f"select max(n) from (select geography_key, date_key, measure_key, count(*) n from gold where is_total "
                                              f"and measure_key in ('{PLN}refund', '{PLN}contribution_of_patient', "
                                              "'nfz_pharmacy_refunds:monthly_provisions:packages:quantity') group by all)")[0][0],
        "rows_per_measure": q(con, "select measure_key, unit_code, count(*), count(*) filter (where is_total) from gold group by all order by 1"),
        "spot_2024_12_total": q(con, "select geography_key, measure_codes, value from gold where date_key = date '2024-12-01' and is_total "
                                     "and geography_key in ('PL', 'TERC:14') order by 1, 2"),
        "spot_PL_2024_12_refund_atc_C": q(con, f"select value, breakdown_json, breakdown_label_json from gold where date_key = date '2024-12-01' "
                                               f"and geography_key = 'PL' and measure_key = '{PLN}refund' and breakdown_json like '%\"C\"%'"),
    }
    out["provinces_minus_PL_max_abs"] = q(con, """
        with t as (select * from silver where atc_code = '_T'),
        s as (select period_start, sum(refund) refund, sum(value) value, sum(quantity) quantity from t where area_code <> 'PL' group by 1)
        select count(*), max(abs(p.refund - s.refund)), max(abs(p.value - s.value)), max(abs(p.quantity - s.quantity))
        from s join t as p on p.area_code = 'PL' and p.period_start = s.period_start""")[0]
    out["PL_2024_refund_total_minus_14_groups"] = q(con, """
        select sum(refund) filter (where atc_code = '_T') - sum(refund) filter (where atc_code <> '_T')
        from silver where area_code = 'PL' and year(period_start) = 2024""")[0][0]
    m = q(con, "select count(*), sum(refund), sum(value), sum(quantity), sum(donation), sum(contribution_of_patient) "
               "from silver where area_code = 'PL' and atc_code = '_T' and year(period_start) = 2024")[0]
    y = next(r for r in yearly["data"] if r["year"] == 2024)
    out["recon_2024_sum_of_months_vs_yearly"] = {
        "months": m[0], "refund": [round(m[1], 2), y["refund"]], "value": [round(m[2], 2), y["value"]],
        "quantity": [round(m[3], 5), y["quantity"]], "donation": [round(m[4], 2), y["donation"]],
        "contribution_of_patient": [round(m[5], 2), y["contribution-of-patient"]]}
    return out


def run_landing(extract, mode: str, budget: int = 10) -> tuple[dict, int, float]:
    before, start, old = CALLS[0], time.monotonic(), sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        code = extract.main(["--mode", mode, "--landing-root", LANDING, "--budget-minutes", str(budget)])
    finally:
        sys.stdout = old
    summary = json.loads([ln for ln in buf.getvalue().splitlines() if ln.startswith("{")][-1])
    summary["exit"] = code
    return summary, CALLS[0] - before, round(time.monotonic() - start)


def plain(obj):
    return json.loads(json.dumps(obj, default=str))


def main() -> int:
    from ingestion.common import bronze_files as bf
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.common.report import annotate
    from ingestion.to_bronze.nfz_pharmacy_refunds import load
    from ingestion.to_landing.nfz_pharmacy_refunds import extract
    from warehouse.publish import licensed_sources

    client, cat = s3(), bf.retry(catalog, "catalog")
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    try:
        first, calls1, secs1 = run_landing(extract, "load", 290)
        second, calls2, secs2 = run_landing(extract, "load")
        verify, _, _ = run_landing(extract, "verify")
        files = receipts = size = 0
        parts: dict[str, int] = {}
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing_bucket, Prefix=f"{LANDING}/"):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".receipt.json"):
                    receipts += 1
                else:
                    files, size = files + 1, size + o["Size"]
                    parts[o["Key"].split("/")[-2]] = parts.get(o["Key"].split("/")[-2], 0) + 1
        annotate("notice" if first["exit"] == 0 and second["exit"] == 0 else "warning",
                 "nfz_pharmacy_refunds trial · Landing (full load, twice)", plain({
                     "first": first, "first_requests": calls1, "first_seconds": secs1,
                     "second": second, "second_requests": calls2, "second_seconds": secs2, "verify": verify,
                     "files": files, "receipts": receipts, "bytes": size, "entities": len(parts),
                     "entities_by_pages": sorted({v: sum(1 for x in parts.values() if x == v) for v in parts.values()}.items())}))

        table_id = (SCRATCH, load.TABLE)
        load.load(0, landing_root=LANDING, namespace=SCRATCH)
        files1 = len(bf.loaded_hashes(cat.load_table(table_id)))
        rows1 = cat.load_table(table_id).scan().to_arrow().num_rows
        load.load(0, landing_root=LANDING, namespace=SCRATCH)
        bronze = cat.load_table(table_id).scan().to_arrow()
        files2 = len(bf.loaded_hashes(cat.load_table(table_id)))
        try:
            rejected = cat.load_table((SCRATCH, load.REJECTED)).scan().to_arrow().to_pylist()
        except Exception:  # no rejections table when nothing was rejected
            rejected = []
        dim = cat.load_table(("gold", "dim_geography")).scan(selected_fields=("geography_key",)).to_arrow()
        _, yearly, _ = extract.get_json(requests.Session(), "yearly-provisions", page=1, limit=25)
        res = analyse(bronze, dim, yearly)
        annotate("notice" if not rejected and files1 == files2 and rows1 == bronze.num_rows else "warning",
                 "nfz_pharmacy_refunds trial · Bronze (twice) and profile", plain({
                     "files_rows_after_first": [files1, rows1], "files_rows_after_second": [files2, bronze.num_rows],
                     "rejected": [r.get("error") for r in rejected][:5], "profile": res["bronze"]}))
        annotate("warning" if res["silver_bad"] else "notice", "nfz_pharmacy_refunds trial · Silver and tests", plain(res["silver"]))
        gold = res["gold"]
        annotate("warning" if gold["grain_duplicates"] or any(gold["rows_X_and_keys_not_in_dim_geography"]) else "notice",
                 "nfz_pharmacy_refunds trial · Gold block", plain(gold))
        with open(ROOT / "warehouse/seeds/source.csv", newline="") as s, open(ROOT / "warehouse/seeds/licence.csv", newline="") as lic:
            sources, licences = list(csv.DictReader(s)), list(csv.DictReader(lic))
        row = next(r for r in sources if r["source_key"] == "nfz_pharmacy_refunds")
        annotate("notice", "nfz_pharmacy_refunds trial · reconciliation, partition, licence gate", plain({
            "recon_2024_sum_of_months_vs_yearly": res["recon_2024_sum_of_months_vs_yearly"],
            "provinces_minus_PL_months_max_abs": res["provinces_minus_PL_max_abs"],
            "PL_2024_refund_total_minus_14_groups": res["PL_2024_refund_total_minus_14_groups"],
            "source_csv_row": row, "licence_row": next((r for r in licences if r["licence_key"] == row["licence_key"]), None),
            "licensed_by_gate": "nfz_pharmacy_refunds" in licensed_sources(sources, licences)}))
        print(json.dumps(plain(res), indent=1))
    finally:
        removed = 0
        for bucket in (landing_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        with contextlib.suppress(Exception):
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(".".join(ident))
        with contextlib.suppress(Exception):
            cat.drop_namespace(SCRATCH)
        annotate("notice", "nfz_pharmacy_refunds trial · cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
