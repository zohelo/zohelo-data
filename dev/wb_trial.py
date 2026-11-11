"""Dev trial (#2736): World Bank Findex, ICP, IDS, WGI — Landing → Bronze → Silver into scratch only.

Per source: Landing load into the Landing prefix zz_dev_wb/ (twice: the second must land nothing new),
the Bronze load into Iceberg namespace zz_dev_wb and lake prefix zz_dev_wb/ (twice: the second appends
nothing), then each Silver model's SQL rendered over the scratch Bronze tables in local DuckDB with its
tests as queries and profiling queries for the Gold design. Everything written is removed in `finally`.
Never writes production namespaces or prefixes. Output goes to the job log (print) and annotations.
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

SCRATCH = "zz_dev_wb"
EU = "AUT BEL BGR HRV CYP CZE DNK EST FIN FRA DEU GRC HUN IRL ITA LVA LTU LUX MLT NLD POL PRT ROU SVK SVN ESP SWE"
EUQ = ",".join(f"'{c}'" for c in EU.split())
SILVER = "warehouse/models/silver"

PLAN = {
    "world_bank_wgi": {
        "tables": ["wgi_observations", "wgi_metadata"],
        "models": ["world_bank_wgi/wgi_indicators", "world_bank_wgi/wgi_observations"],
        "checks": [
            ("rows", "select count(*), count(distinct economy_code), count(distinct indicator_code), min(year), max(year) from wgi_observations"),
            ("dupes", "select count(*) from (select economy_code, indicator_code, year from wgi_observations group by all having count(*) > 1)"),
            ("nulls", "select count(*) from wgi_observations where value is null"),
            ("orphan indicators", "select count(*) from wgi_observations o anti join wgi_indicators i using (indicator_code)"),
            ("indicators", "select indicator_code, topic, unit_of_measure from wgi_indicators order by 1"),
            ("POL EST latest", "select indicator_code, year, value from wgi_observations where economy_code='POL' and indicator_code like '%.EST' qualify row_number() over (partition by indicator_code order by year desc) <= 2 order by 1, 2"),
            ("EU coverage EST", f"select count(distinct economy_code), min(year), max(year) from wgi_observations where economy_code in ({EUQ}) and indicator_code like '%.EST'"),
        ],
    },
    "world_bank_ids": {
        "tables": ["ids_observations", "ids_metadata"],
        "models": ["world_bank_ids/ids_series", "world_bank_ids/ids_observations"],
        "checks": [
            ("rows", "select count(*), count(distinct economy_code), count(distinct series_code), min(year), max(year) from ids_observations"),
            ("dupes", "select count(*) from (select economy_code, counterpart_code, series_code, year from ids_observations group by all having count(*) > 1)"),
            ("dupe sample", "select economy_code, counterpart_code, series_code, year, count(*) from ids_observations group by all having count(*) > 1 limit 5"),
            ("nulls", "select count(*) from ids_observations where value is null"),
            ("non-numeric sample", "select distinct value_text from ids_observations where value is null limit 10"),
            ("series", "select count(*), count(distinct series_code) from ids_series"),
            ("EU or POL", f"select economy_code, count(*) from ids_observations where economy_code in ({EUQ}) group by 1"),
            ("counterparts", "select counterpart_code, count(*) from ids_observations group by 1"),
        ],
    },
    "world_bank_findex": {
        "tables": ["findex_observations"],
        "models": ["world_bank_findex/findex_observations"],
        "checks": [
            ("rows", "select count(*), count(distinct economy_code), count(distinct indicator), min(year), max(year) from findex_observations"),
            ("dupes", "select count(*) from (select economy_code, year, group_name, group_value, indicator from findex_observations group by all having count(*) > 1)"),
            ("dupe sample", "select economy_code, year, group_name, group_value, indicator, count(*) from findex_observations group by all having count(*) > 1 limit 5"),
            ("nulls", "select count(*) from findex_observations where value is null"),
            ("non-numeric sample", "select distinct value_text from findex_observations where value is null limit 10"),
            ("value range", "select min(value), max(value), count(*) filter (where value > 1) from findex_observations"),
            ("big values sample", "select indicator, min(value), max(value) from findex_observations where value > 1 group by 1 limit 20"),
            ("aggregates", "select economy_code, any_value(economy_name) from findex_observations where region = 'NA' or economy_code in ('EAP','ECA','HIC','LMY','WLD','EUU','EMU') group by 1 order by 1"),
            ("POL account", "select year, group_name, group_value, value from findex_observations where economy_code='POL' and indicator='account_t_d' and group_name in ('all','gender') order by 1, 2, 3"),
            ("EU account 2024", f"select count(distinct economy_code) from findex_observations where economy_code in ({EUQ}) and indicator='account_t_d' and group_name='all' and year=2024"),
            ("name sample", "select distinct economy_name from findex_observations where economy_name similar to '.*[^ -~].*'"),
        ],
    },
    "world_bank_icp": {
        "tables": ["icp_observations"],
        "models": ["world_bank_icp/icp_observations"],
        "checks": [
            ("rows", "select count(*), count(distinct economy_code), count(distinct series_code), count(distinct classification_code), min(year), max(year) from icp_observations"),
            ("dupes", "select count(*) from (select economy_code, classification_code, series_code, year from icp_observations group by all having count(*) > 1)"),
            ("nulls", "select count(*) from icp_observations where value is null"),
            ("classifications", "select classification_code, any_value(classification_name), count(*) from icp_observations group by 1 order by 1"),
            ("non-country codes", "select economy_code, any_value(economy_name), count(*) from icp_observations where length(economy_code) <> 3 or economy_name ilike '%union%' or economy_name ilike '%euro%' or economy_name ilike '%world%' or economy_name ilike '%income%' group by 1 order by 1"),
            ("all codes", "select string_agg(distinct economy_code, ' ' order by economy_code) from icp_observations"),
            ("POL GDP", "select classification_code, year, value from icp_observations where economy_code='POL' and series_code='1000000' and classification_code in ('PCAP.PP','PX.WL','PPPGlob','PCAP.PP.IX.WL') order by 1, 2"),
            ("EU27 GDP", "select economy_code, classification_code, year, value from icp_observations where series_code='1000000' and classification_code in ('PCAP.PP','PX.WL') and (economy_name ilike '%european union%' or economy_code in ('EUU','EU27','EUR')) order by 1, 2, 3"),
            ("EU countries", f"select count(distinct economy_code) from icp_observations where economy_code in ({EUQ}) and series_code='1000000' and classification_code='PCAP.PP'"),
        ],
    },
}


def render(path: str, schema: dict[str, str]) -> str:
    sql = Path(path).read_text()
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*source\([^,]*,\s*'([^']+)'\)\s*\}\}", lambda m: f"bronze_{m.group(1)}", sql)
    return re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", lambda m: m.group(1), sql)


def land(source: str) -> None:
    for attempt in (1, 2):
        if source == "world_bank_icp":
            from ingestion.to_landing.world_bank_icp import extract

            status = extract.main(["--mode", "load", "--landing-root", SCRATCH])
        else:
            status = bulk_file.main(source, "load", landing_root=SCRATCH)
        print(source, "landing load", attempt, "exit", status)


def trial(source: str) -> None:
    plan = PLAN[source]
    land(source)
    loader = importlib.import_module(f"ingestion.to_bronze.{source}.load")
    for attempt in (1, 2):
        status = loader.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        print(source, "bronze load", attempt, "exit", status)
    cat = catalog()
    con = duckdb.connect()
    results = {}
    for table in plan["tables"]:
        arrow = cat.load_table((SCRATCH, table)).scan().to_arrow()
        con.register(f"bronze_{table}", arrow)
        results[f"bronze {table}"] = arrow.num_rows
        results[f"bronze {table} files"] = len(cat.load_table((SCRATCH, table)).inspect.data_files())
    rej = f"{plan['tables'][0].split('_')[0]}_rejected_files"
    try:
        results["rejected"] = [list(map(str, r.values())) for r in cat.load_table((SCRATCH, rej)).scan().to_arrow().to_pylist()]
    except Exception as exc:  # noqa: BLE001
        results["rejected"] = f"none ({type(exc).__name__})"
    for model in plan["models"]:
        name = model.split("/")[-1]
        con.execute(f"create table {name} as {render(f'{SILVER}/{model}.sql', {})}")
        results[f"silver {name}"] = con.execute(f"select count(*) from {name}").fetchone()[0]
    for title, query in plan["checks"]:
        try:
            results[title] = [list(map(str, row)) for row in con.execute(query).fetchall()][:80]
        except Exception as exc:  # noqa: BLE001
            results[title] = f"ERROR {type(exc).__name__}: {exc}"[:400]
    print(f"===== {source} RESULTS")
    for k, v in results.items():
        print(f"{source} | {k} | {json.dumps(v, ensure_ascii=False)}")
    annotate("notice", f"{source} trial (scratch)", {k: results[k] for k in list(results)[:6]})


def cleanup() -> None:
    removed = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for bucket_var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(bucket_var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    left = sum(
        len(p.get("Contents", []))
        for v in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(v), Prefix=f"{SCRATCH}/")
    )
    removed["left"] = left
    print("scratch removed", removed)
    annotate("notice", "scratch removed", removed)


def main() -> int:
    sources = sys.argv[1:] or list(PLAN)
    failed = 0
    try:
        for source in sources:
            try:
                trial(source)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                import traceback

                traceback.print_exc()
                annotate("error", f"{source} trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
