"""Dev trial (wave-2 international sources): Landing → Bronze → Silver into scratch only.

Per source: the bulk_file Landing load into the Landing bucket prefix zz_dev_w2intl/ (twice: the second must be
"unchanged"), the source's Bronze load into Iceberg namespace zz_dev_w2intl and the lake prefix zz_dev_w2intl/
(twice: the second must append nothing), then the Silver model's SQL (rendered from warehouse/models/silver/)
in local DuckDB over that Bronze table, with the model's tests as queries. Everything written is removed in
`finally` (tables purged, namespace dropped, both prefixes deleted). Never writes production namespaces or
prefixes; never publishes. Results are printed to the job log and annotated.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import re
import sys

import duckdb

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_w2intl"
MODELS = {
    "bis": (
        ["warehouse/models/silver/bis/bis_observations.sql"],
        "bis_observations",
        [
            ("rows by entity", "select entity, count(*), count(distinct ref_area), min(time_period), max(time_period) from silver group by 1 order by 1"),
            ("unique", "select count(*) from (select entity, series_key, time_period from silver group by all having count(*) > 1)"),
            ("nulls", "select count(*) from silver where entity is null or series_key is null or freq is null or ref_area is null or time_period is null or raw_sha256 is null"),
            ("bad freq", "select list(distinct freq) from silver where freq not in ('M','Q','A','D')"),
            ("null value", "select entity, obs_status, count(*) from silver where obs_value is null group by all"),
            ("null unit", "select entity, count(*) from silver where unit_measure is null group by all"),
            ("bronze series rows", "select entity, count(*) from bronze where time_period = '' group by all"),
            ("PL series", "select entity, series_key, unit_measure, count(*), min(time_period), max(time_period) from silver where ref_area = 'PL' group by all order by all limit 60"),
            ("PL latest cbpol/spp", "select entity, series_key, time_period, obs_value from silver where ref_area = 'PL' and entity in ('cbpol','spp','credit_gap') qualify row_number() over (partition by series_key order by time_period desc) = 1"),
            ("areas", "select list(distinct ref_area order by ref_area) from silver"),
        ],
    ),
    "ilostat": (
        ["warehouse/models/silver/ilostat/ilostat_observations.sql"],
        "ilostat_observations",
        [
            ("rows by entity", "select entity, count(*), count(distinct ref_area), min(time_period), max(time_period) from silver group by 1 order by 1"),
            ("unique", "select count(*) from (select entity, series_key, time_period from silver group by all having count(*) > 1)"),
            ("nulls", "select count(*) from silver where entity is null or indicator is null or freq is null or ref_area is null or sex is null or time_period is null or raw_sha256 is null"),
            ("null sex", "select entity, count(*) from silver where sex is null or sex = '' group by all"),
            ("null value", "select entity, obs_status, count(*) from silver where obs_value is null group by all"),
            ("sex codes", "select list(distinct sex) from silver"),
            ("classif1 POL", "select entity, classif1, count(*) from silver where ref_area = 'POL' group by all order by all limit 60"),
            ("POL totals", "select entity, source_code, classif1, count(*), min(time_period), max(time_period) from silver where ref_area = 'POL' and sex = 'SEX_T' and classif1 in ('AGE_YTHADULT_YGE15','AGE_YTHADULT_Y15-64','AGE_YTHADULT_Y15-24','AGE_YTHADULT_Y15-74') or (ref_area = 'POL' and entity = 'eip_neet_sex_rt_a' and sex = 'SEX_T') group by all order by all"),
            ("POL latest", "select entity, classif1, time_period, obs_value from silver where ref_area = 'POL' and sex = 'SEX_T' and classif1 in ('AGE_YTHADULT_Y15-64', 'AGE_YTHADULT_Y15-24') qualify row_number() over (partition by entity, classif1 order by time_period desc) = 1"),
            ("POL dup period across sources", "select entity, classif1, time_period, count(*) from silver where ref_area = 'POL' and sex = 'SEX_T' group by all having count(*) > 1 limit 10"),
            ("non-ISO areas", "select list(distinct ref_area order by ref_area) from silver where ref_area like 'X%'"),
            ("EU areas present", "select count(distinct ref_area) from silver where ref_area in ('AUT','BEL','BGR','HRV','CYP','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','IRL','ITA','LVA','LTU','LUX','MLT','NLD','POL','PRT','ROU','SVK','SVN','ESP','SWE')"),
            ("obs_status", "select obs_status, count(*) from silver group by all"),
        ],
    ),
    "undp_hdr": (
        ["warehouse/models/silver/undp_hdr/undp_hdr_indices.sql"],
        "undp_hdr_values",
        [
            ("rows", "select count(*), count(distinct iso3), count(distinct indicator), min(year), max(year) from silver"),
            ("unique", "select count(*) from (select iso3, indicator, year from silver group by all having count(*) > 1)"),
            ("nulls", "select count(*) from silver where iso3 is null or indicator is null or year is null or raw_sha256 is null"),
            ("non-numeric", "select column_name, count(*) from silver where value is null group by all limit 20"),
            ("no-year columns", "select distinct column_name from bronze where not regexp_matches(column_name, '_[0-9]{4}$')"),
            ("indicators", "select indicator, count(*), min(year), max(year) from silver group by all order by all"),
            ("POL", "select indicator, year, value from silver where iso3 = 'POL' and indicator in ('hdi','le','hdi_rank','gnipc','gii','ihdi') qualify row_number() over (partition by indicator order by year desc) = 1"),
            ("aggregates", "select distinct iso3, country from silver where iso3 like 'ZZ%' order by 1"),
            ("versions", "select distinct source_version, raw_sha256 from bronze"),
        ],
    ),
}


def render(path: str, table: str) -> str:
    sql = Path(path).read_text()
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    return re.sub(r"\{\{\s*source\([^)]*\)\s*\}\}", table, sql)


def report(title: str, results: dict) -> None:
    print(f"### {title}: {json.dumps(results, default=str)}", flush=True)
    annotate("notice", title, results)


def trial(source: str) -> None:
    sql_paths, bronze, checks = MODELS[source]
    for attempt in (1, 2):
        status = bulk_file.main(source, "load", landing_root=SCRATCH)
        report(f"{source} landing load {attempt}", {"exit": status})
    loader = importlib.import_module(f"ingestion.to_bronze.{source}.load")
    for attempt in (1, 2):
        status = loader.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        report(f"{source} bronze load {attempt}", {"exit": status})
    cat = catalog()
    rejected = f"{source}_rejected_files"
    if (SCRATCH, rejected) in [tuple(t) for t in cat.list_tables(SCRATCH)]:
        rows = cat.load_table((SCRATCH, rejected)).scan().to_arrow().to_pylist()
        report(f"{source} rejected", {"rows": [{k: str(v)[:300] for k, v in r.items()} for r in rows][:10]})
    con = duckdb.connect()
    con.register("bronze", cat.load_table((SCRATCH, bronze)).scan().to_arrow())
    results = {"bronze_rows": con.execute("select count(*) from bronze").fetchone()[0]}
    results["bronze_files"] = con.execute("select count(distinct raw_sha256) from bronze").fetchone()[0]
    for path in sql_paths:
        con.execute("create or replace table silver as " + render(path, "bronze"))
        results[Path(path).stem + "_rows"] = con.execute("select count(*) from silver").fetchone()[0]
        con.execute("create or replace table silver2 as " + render(path, "bronze"))
        results["rebuild_same_rows"] = con.execute("select count(*) from silver2").fetchone()[0] == results[Path(path).stem + "_rows"]
    report(f"{source} counts (scratch)", results)
    for title, query in checks:
        report(f"{source} {title}", {"rows": [list(map(str, row)) for row in con.execute(query).fetchall()][:60]})


def cleanup() -> None:
    removed = {"tables": [], "landing_objects": 0, "lake_objects": 0}
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
    report("scratch removed", removed)


def main() -> int:
    sources = sys.argv[1:] or list(MODELS)
    failed = 0
    try:
        for source in sources:
            try:
                trial(source)
            except Exception as exc:
                failed += 1
                report(f"{source} trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
