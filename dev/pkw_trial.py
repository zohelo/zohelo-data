"""Dev trial for #486 (PKW election results, PLG-029; test branch only, never on main).

Landing -> Bronze -> Silver -> Gold into scratch, Landing and Bronze twice. Landing: the shared bulk_file pattern into
the prefix zz_dev_pkw/ (load twice: the second must land nothing), then inspect and verify. Bronze into the Iceberg
namespace zz_dev_pkw and lake prefix zz_dev_pkw/ (twice: the second appends nothing), profiled. Silver, the Gold build
stage, the elections table and the turnout metrics are the dbt SQL rendered with Jinja and run in local DuckDB over
the scratch Bronze table (twice each), with the model tests, the completeness test and the grain check of
tests/fact_grain.sql (series_key x date_key); geography against production gold.dim_geography (read only).
Everything written is removed in `finally`; never writes a production namespace or prefix.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import time

import duckdb
from jinja2 import Environment

SCRATCH = "zz_dev_pkw"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
MACROS = "".join(p.read_text() for p in sorted(Path("warehouse/macros").glob("*.sql")))
MACROS += Path("warehouse/macros/sources/pkw.sql").read_text()
COLLECTED: dict[str, list] = {}

from ingestion.common import report as report_module  # noqa: E402

ANNOTATE = report_module.annotate


def collect(level: str, title: str, data) -> None:
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


def note(title: str, data, level: str = "notice") -> None:
    print(f"FULL {title}: {json.dumps(data, default=str)}", flush=True)
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / "pkw_trial.jsonl").open("a") as handle:
        handle.write(json.dumps({"title": title, "level": level, "data": data}, default=str) + "\n")
    ANNOTATE(level, title, data)


def q(con, sql: str, limit: int = 60) -> list:
    rows = con.execute(sql).fetchall()[:limit]
    return [[v if isinstance(v, int | float) or v is None else str(v) for v in r] for r in rows]


def count(con, sql: str) -> int:
    return con.execute(f"select count(*) from ({sql})").fetchone()[0]


def render(path_or_text: str, refs: dict[str, str] | None = None, sources: dict[str, str] | None = None) -> str:
    text = Path(path_or_text).read_text() if path_or_text.endswith(".sql") else path_or_text
    return (
        Environment(extensions=["jinja2.ext.do"])
        .from_string(MACROS + text)
        .render(
            config=lambda **_k: "",
            ref=lambda name: (refs or {}).get(name, name),
            source=lambda _s, table: (sources or {})[table],
        )
    )


def trial() -> int:
    from ingestion.common import bulk_bronze
    from ingestion.common.r2 import catalog
    from ingestion.to_bronze.pkw import load as bronze
    from ingestion.to_landing.patterns import bulk_file

    bulk_file.annotate = collect
    bulk_bronze.annotate = collect
    bulk_file.step_summary = lambda _m: None
    bulk_bronze.step_summary = lambda _m: None
    # --- Landing -------------------------------------------------------------------------------------------------
    t0 = time.monotonic()
    exits = [bulk_file.main("pkw", "load", landing_root=SCRATCH, pause_seconds=2) for _ in (1, 2)]
    loads = COLLECTED.get("pkw landing load", [])
    first = [{k: f.get(k) for k in ("entity", "outcome", "size_mb", "version")} for f in loads[0]["files"]]
    second = sorted({f["outcome"] for f in loads[1]["files"]})
    exits.append(bulk_file.main("pkw", "inspect", landing_root=SCRATCH))
    exits.append(bulk_file.main("pkw", "verify", landing_root=SCRATCH))
    inspect = COLLECTED.get("pkw landing inspect", [{}])[-1].get("files", [])
    verify = COLLECTED.get("pkw landing verify", [{}])[-1].get("files", [])
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = []
    for spec in bulk_file.declaration("pkw"):
        rows = bulk_file.landed(client, bucket, bulk_file.prefix("pkw", spec["entity"], SCRATCH))
        held += rows
    note(
        "pkw trial Landing (scratch zz_dev_pkw)",
        {
            "exits_load_load_inspect_verify": exits,
            "files": len(held),
            "bytes": sum(r["size_bytes"] for r in held),
            "versions": sorted({r["version_token"] for r in held}),
            "content_types": sorted({str(r["detail"]["http"]["content_type"]) for r in held}),
            "second_load_outcomes": second,
            "inspect_states": sorted({f.get("state") for f in inspect}),
            "verify_missing": sum(f.get("missing_data", 0) for f in verify),
            "minutes": round((time.monotonic() - t0) / 60, 1),
            "first_load": first,
        },
    )
    # --- Bronze --------------------------------------------------------------------------------------------------
    bexits = [bronze.load(30, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    cat = catalog()
    con = duckdb.connect()
    con.register("b_cells", cat.load_table((SCRATCH, bronze.TABLE)).scan().to_arrow())
    try:
        rejected = cat.load_table((SCRATCH, bronze.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    bl = COLLECTED.get("pkw Bronze from Landing", [])
    note(
        "pkw trial Bronze (scratch)",
        {
            "exits": bexits,
            "loads": [{k: x.get(k) for k in ("candidates", "appended_files", "rows_added", "rejected")} for x in bl],
            "rows": count(con, "select * from b_cells"),
            "files": count(con, "select distinct raw_sha256 from b_cells"),
            "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:5],
            "per_entity": q(
                con,
                "select entity, count(*), max(row_number), max(column_number), count(distinct sheet_name) "
                "from b_cells group by 1 order by 1",
                30,
            ),
        },
    )
    note(
        "pkw trial Bronze profile",
        {
            "header_labels_not_numeric_cells": q(
                con,
                "select regexp_replace(trim(value_text), '\\d', '9', 'g') as shape, count(*) from b_cells "
                "where row_number > 1 and not regexp_matches(trim(value_text), '^-?[0-9]+([.,][0-9]+)?$') "
                "and column_number > 6 group by 1 order by 2 desc",
                25,
            ),
            "duplicate_headers": q(
                con,
                "select entity, value_text, count(*) from b_cells where row_number = 1 group by all "
                "having count(*) > 1",
                10,
            ),
            "header_set": q(
                con,
                "select value_text, count(distinct entity) from b_cells where row_number = 1 "
                "and not regexp_matches(value_text, '^(KOALICYJNY )?KOMITET') group by 1 order by 1",
                80,
            ),
        },
    )
    # --- Silver --------------------------------------------------------------------------------------------------
    sources = {"pkw_cells": "b_cells"}
    sql = render("warehouse/models/silver/pkw/pkw_results.sql", {}, sources)
    silver = []
    for _ in (1, 2):
        con.execute(f"create or replace table pkw_results as {sql}")
        silver.append(count(con, "select * from pkw_results"))
    tests = {
        "unique cell": "select entity, source_row, column_number from pkw_results group by all having count(*) > 1",
        "unique semantic": "select election_key, area_level, file_kind, area_key, column_label, breakdown_code, "
        "settlement_code from pkw_results group by all having count(*) > 1",
        "not_null keys": "select 1 from pkw_results where election_key is null or election_type is null or "
        "election_date is null or area_level is null or file_kind is null or area_kind is null or area_key is null "
        "or column_label is null or measure_code is null or unit_code is null or value_text is null or entity is null "
        "or raw_sha256 is null",
        "accepted area_kind": "select 1 from pkw_results where area_kind not in ('country', 'teryt', 'abroad', "
        "'ships', 'voivodeship_name', 'city_powiat_name')",
        "accepted measure_code": "select 1 from pkw_results where measure_code not in ('turnout', 'eligible_voters', "
        "'ballots_issued', 'valid_votes', 'valid_votes_pct', 'invalid_votes', 'invalid_votes_pct', 'list_votes', "
        "'list_share', 'candidate_votes', 'protocol')",
        "pkw_results_complete": render("warehouse/tests/pkw_results_complete.sql", {}),
    }
    failing = {k: count(con, s) for k, s in tests.items()}
    samples = {k: q(con, tests[k], 8) for k, c in failing.items() if c}
    note(
        "pkw trial Silver (local DuckDB over scratch Bronze)",
        {
            "rows_two_builds": silver,
            "tests_failing_rows": failing,
            "failing_samples": samples,
            "unknown_labels": q(
                con, "select distinct entity, column_label from pkw_results where measure_code = 'unknown'", 15
            ),
            "odd_area_kind": q(
                con,
                "select entity, area_kind, area_name, powiat_name from pkw_results where area_kind in "
                "('no_code', 'bad_code') group by all",
                10,
            ),
            "by_election_level": q(
                con,
                "select election_key, area_level, file_kind, count(*), count(distinct area_key), "
                "count(distinct area_key) filter (where area_kind = 'teryt') from pkw_results group by all order by all",
                40,
            ),
            "area_kinds": q(con, "select area_level, area_kind, count(distinct area_key) from pkw_results "
                            "group by all order by all"),
            "measures": q(
                con,
                "select election_key, measure_code, unit_code, count(*), min(value), max(value), "
                "count(distinct breakdown_code) from pkw_results where measure_code <> 'protocol' group by all "
                "order by all",
                80,
            ),
            "value_null": count(con, "select 1 from pkw_results where value is null"),
        },
    )
    # --- Gold ----------------------------------------------------------------------------------------------------
    con.register("geo_arrow", cat.load_table(("gold", "dim_geography")).scan().to_arrow())
    con.execute("create or replace table geo as select * from geo_arrow")
    gmap = render("{{ poc_geography__pkw() }}")
    con.execute(f"create or replace table gmap as with {gmap} select * from pkw")
    refs = {"poc_map_geography": "gmap", "pkw_results": "pkw_results"}
    gold = {}
    for name in ("poc_build_pkw", "pkw_elections"):
        gsql = render(f"warehouse/models/gold/{name}.sql", refs)
        gold[name] = []
        for _ in (1, 2):
            con.execute(f"create or replace table {name} as {gsql}")
            gold[name].append(count(con, f"select * from {name}"))
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    dups_sql = f"select {series} as series_key, date_key from poc_build_pkw group by all having count(*) > 1"
    dups = count(con, dups_sql)
    con.execute(
        "create or replace table metrics as select * from poc_build_pkw where is_total and geography_key <> '_X' "
        "and measure_key in ('pkw:sejm:pct:turnout', 'pkw:pe:pct:turnout', 'pkw:sejmik:pct:turnout')"
    )
    bound = re.search(
        r"\('voter_turnout', 'pkw', '%', ([\d.e]+), ([\d.e]+)\)", Path("warehouse/tests/metrics_value_range.sql").read_text()
    )
    lo, hi = float(bound.group(1)), float(bound.group(2))
    note(
        "pkw trial Gold build_pkw, pkw_elections, voter_turnout",
        {
            "rows_two_builds": gold,
            "map_rows": count(con, "select * from gmap"),
            "map_duplicate_codes": count(con, "select source_code from gmap group by 1 having count(*) > 1"),
            "dup_series_date": dups,
            "dup_samples": q(con, dups_sql, 5),
            "series": count(con, f"select distinct {series} from poc_build_pkw"),
            "unmatched": q(
                con,
                "select block_id, geography_source_code, count(*) from poc_build_pkw where geography_key = '_X' "
                "group by all order by 3 desc",
                20,
            ),
            "build_check": q(
                con,
                f"select block_id, count(*), count(distinct measure_key), count(distinct {series}), min(date_key), "
                "max(date_key) from poc_build_pkw group by 1 order by 1",
            ),
            "measures": q(
                con,
                "select measure_key, count(*), count(distinct geography_key), min(value), max(value) "
                "from poc_build_pkw group by 1 order by 1",
                40,
            ),
            "elections": q(con, "select * from pkw_elections order by election_date"),
            "metrics_rows": count(con, "select * from metrics"),
            "metrics_max_per_area_period": con.execute(
                "select max(n) from (select geography_key, date_key, count(*) n from metrics group by all)"
            ).fetchone()[0],
            "metrics_range": q(con, "select min(value), max(value) from metrics"),
            "metrics_out_of_bounds": count(con, f"select 1 from metrics where value < {lo} or value > {hi}"),
        },
    )
    note(
        "pkw trial spot values (Poland and voivodeships)",
        {
            "turnout_mazowieckie": q(
                con,
                "select measure_key, date_key, value from poc_build_pkw where geography_key = 'TERC:14' and "
                "measure_codes = 'turnout' order by 2",
            ),
            "sejm_top_lists_mazowieckie": q(
                con,
                "select breakdown_json, value from poc_build_pkw where geography_key = 'TERC:14' and "
                "measure_key = 'pkw:sejm:pct:list_share' order by value desc",
                3,
            ),
            "prezydent_r2_candidates_pomorskie": q(
                con,
                "select breakdown_json, value from poc_build_pkw where geography_key = 'TERC:22' and "
                "measure_key = 'pkw:prezydent:votes:candidate_votes' and date_key = date '2025-06-01' order by 2 desc",
            ),
            "sejmik_poland_ogolem_top": q(
                con,
                "select breakdown_json, value from poc_build_pkw where geography_key = 'PL' and "
                "measure_key = 'pkw:sejmik:pct:list_share' and breakdown_json not like '%settlement%' "
                "order by value desc",
                3,
            ),
            "gmina_turnout_warszawa_districts": q(
                con,
                "select geography_key, date_key, value from poc_build_pkw where geography_key like 'TERC:14650%' "
                "and measure_codes = 'turnout' order by 1, 2",
                6,
            ),
            "sejm_sum_woj_valid_votes": q(
                con,
                "select sum(value) from poc_build_pkw where measure_key = 'pkw:sejm:votes:valid_votes' and "
                "geography_key like 'TERC:__'",
            ),
        },
    )
    con.execute(
        "create or replace table seed_source as select * from read_csv('warehouse/seed_rows/source/*.csv', all_varchar=true, union_by_name=true)"
    )
    con.execute(
        "create or replace table seed_licence as select * from read_csv('warehouse/seed_rows/licence/*.csv', all_varchar=true, union_by_name=true)"
    )
    catalogue = q(
        con,
        "select s.source_key, s.licence_key, l.name, l.attribution_text from seed_source as s left join seed_licence "
        "as l using (licence_key) where s.source_key = 'pkw'",
    )
    problems = {
        "landing_exits": [e for e in exits if e],
        "landed_by_second_load": [o for o in second if o != "unchanged"],
        "rejected_files": len(rejected),
        "bronze_second_load_candidates": (bl or [{}, {}])[-1].get("candidates"),
        "silver_failing_tests": {k: c for k, c in failing.items() if c},
        "silver_rebuild_differs": len(set(silver)) > 1,
        "gold_rebuild_differs": [k for k, v in gold.items() if len(set(v)) > 1],
        "dup_series_date": dups,
        "metrics_out_of_bounds": count(con, f"select 1 from metrics where value < {lo} or value > {hi}"),
        "licence_row": catalogue if not catalogue or catalogue[0][1] != "unresolved" else None,
    }
    bad = {k: v for k, v in problems.items() if v}
    note("pkw trial licence catalogue row", {"catalogue_row": catalogue})
    note("pkw trial verdict", {"problems": bad} if bad else {"result": "all checks passed"}, "error" if bad else "notice")
    return 1 if bad else 0


def cleanup() -> None:
    from ingestion.common.r2 import catalog, req, s3

    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
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
    note("pkw trial scratch removed", removed)


def main() -> int:
    status = 1
    try:
        status = trial()
    except Exception as exc:  # noqa: BLE001
        import traceback

        note(
            "pkw trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1800:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
