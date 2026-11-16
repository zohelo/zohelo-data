"""Dev trial for #2981 (e-Zamówienia BZP contract and result notices; test branch only, never on main).

Landing -> Bronze -> Silver -> Gold into scratch, each step twice. Landing: the whole window (2026-01-01 .. yesterday
UTC) into the Landing prefix zz_dev_uzp_bzp/ (pass 1, about 1,600 requests, one at a time, 2.5 s apart); pass 2 asks
only the newest three days again and must land nothing. Bronze: into the Iceberg namespace zz_dev_uzp_bzp and lake
prefix zz_dev_uzp_bzp/ (twice: the second appends nothing). Silver and Gold are the dbt SQL rendered with Jinja and
run in local DuckDB over the scratch Bronze table, each built twice. Everything written is removed in `finally`;
never writes a production namespace or prefix. No names, e-mails or phones are printed.
"""

from __future__ import annotations

import json
from pathlib import Path
import time

import duckdb
from jinja2 import Environment

from ingestion.common import report as report_module

SCRATCH = "zz_dev_uzp_bzp"
MACRO_FILES = [*sorted(Path("warehouse/macros").glob("*.sql")), Path("warehouse/macros/sources/uzp_bzp.sql")]
MACROS = "".join(p.read_text() for p in MACRO_FILES)
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


def render(text: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return (
        Environment(extensions=["jinja2.ext.do"])
        .from_string(MACROS + text)
        .render(
            config=lambda **_k: "",
            ref=lambda name: refs.get(name, name),
            source=lambda _s, table: sources[table],
        )
    )


def bronze_profile(con) -> dict:
    t = "bronze_bzp"
    cols = [r[0] for r in con.execute(f"describe {t}").fetchall()]
    text_cols = [c for c in cols if c not in ("stats_count", "source_row_number", "ingested_at")]
    personal = " or ".join(f"coalesce({c}, '') like '%@%'" for c in text_cols)
    return {
        "rows_files_entities_notices": q(
            con,
            f"select count(*), count(distinct raw_sha256), count(distinct entity), count(distinct notice_number), "
            f"count(distinct object_id) from {t}",
        )[0],
        "versions_first_last": q(con, f"select min(source_version), max(source_version) from {t}")[0],
        "dup_notice_numbers": q(con, f"select count(*) from (select notice_number from {t} group by 1 having count(*) > 1)")[0][0],
        "nulls": {c: n for c in cols if (n := q(con, f"select count(*) filter (where {c} is null) from {t}")[0][0])},
        "rows_with_at_sign_any_column": q(con, f"select count(*) from {t} where {personal}")[0][0],
        "subsidised_authorities_without_name": q(
            con, f"select count(*) from {t} where client_type like '3%' and organization_name is null"
        )[0][0],
        "value_fields": q(
            con,
            f"select v.f, count(*) from (select unnest(from_json(form_values, '[{{\"field\":\"VARCHAR\"}}]')).field f "
            f"from {t}) v group by 1 order by 2 desc",
        ),
    }


def silver_checks(con) -> dict:
    t = "uzp_bzp_notices"
    tests = {
        "unique notice_number": f"select notice_number from {t} group by 1 having count(*) > 1",
        "unique object_id": f"select object_id from {t} group by 1 having count(*) > 1",
        "not_null keys": f"select 1 from {t} where notice_number is null or object_id is null or bzp_number is null "
        "or notice_type is null or publication_date is null or entity is null or raw_sha256 is null",
        "days complete (tests/uzp_bzp_notices_days_complete.sql)": render(
            Path("warehouse/tests/uzp_bzp_notices_days_complete.sql").read_text(), {"uzp_bzp_notices": t}, {}
        ),
    }
    import yaml

    spec = yaml.safe_load(Path("warehouse/models/silver/uzp_bzp/_uzp_bzp.yml").read_text())
    for column in spec["models"][0]["columns"]:
        for test in column.get("tests", []):
            if isinstance(test, dict) and "accepted_values" in test:
                values = ", ".join("'" + v + "'" for v in test["accepted_values"]["arguments"]["values"])
                tests[f"accepted_values {column['name']}"] = (
                    f"select 1 from {t} where {column['name']} is not null and {column['name']} not in ({values})"
                )
    out = {
        "silver_tests_failing_rows": {k: con.execute(f"select count(*) from ({v})").fetchone()[0] for k, v in tests.items()}
    }
    out["range_rows_days"] = q(
        con, f"select min(publication_date), max(publication_date), count(*), count(distinct publication_date) from {t}"
    )[0]
    out["per_type"] = q(con, f"select notice_type, count(*) from {t} group by 1 order by 1")
    out["per_month_type"] = q(
        con, f"select strftime(publication_date, '%Y-%m'), notice_type, count(*) from {t} group by all order by 1, 2", 30
    )
    for c in ("order_type", "organization_province", "organization_country", "client_type", "is_below_eu_threshold"):
        out[c] = q(con, f"select {c}, count(*) from {t} group by 1 order by 2 desc", 25)
    out["contract_value"] = q(
        con,
        f"select count(*) filter (where notice_type = 'TenderResultNotice'), "
        f"count(*) filter (where contract_value_lines > 0), count(*) filter (where contract_value is not null), "
        f"count(*) filter (where contract_value_unparsed > 0), count(*) filter (where contract_value_lines > 0 "
        f"and contract_value is null and contract_value_unparsed = 0), sum(contract_value_lines), "
        f"min(contract_value), median(contract_value), max(contract_value) from {t}",
    )[0]
    out["contract_value_columns"] = "results, with lines, valued, unparsed, mixed currency, lines, min, median, max"
    out["currencies"] = q(con, f"select contract_value_currency, count(*) from {t} group by 1 order by 2 desc")
    out["estimated_value"] = q(
        con,
        f"select notice_type, count(*) filter (where estimated_value_lines = 1), count(*) filter (where estimated_value_lines > 1), "
        f"count(*) filter (where estimated_value is not null) from {t} group by 1",
    )
    out["parts_concluded_cancelled"] = q(con, f"select sum(parts_concluded), sum(parts_cancelled) from {t}")[0]
    return out


def models(con, result: dict, gold: dict) -> None:
    silver_sql = render(
        Path("warehouse/models/silver/uzp_bzp/uzp_bzp_notices.sql").read_text(), {}, {"uzp_bzp_notices": "bronze_bzp"}
    )
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table uzp_bzp_notices as {silver_sql}")
        counts.append(con.execute("select count(*) from uzp_bzp_notices").fetchone()[0])
    result["silver_rows_two_builds"] = counts
    result.update(silver_checks(con))
    gmap = render("with nuts_pl as ({{ poc_nuts_pl() }}), {{ poc_geography__uzp_bzp() }} select * from uzp_bzp", {}, {})
    con.execute(f"create or replace view gmap as {gmap}")
    refs = {"uzp_bzp_notices": "uzp_bzp_notices", "poc_map_geography": "gmap"}
    build_sql = render(Path("warehouse/models/gold/poc_build_uzp_bzp.sql").read_text(), refs, {})
    rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build_sql}")
        rows.append(con.execute("select count(*) from build").fetchone()[0])
    gold["gold_rows_two_builds"] = rows
    gold["gold_dup_series_date"] = q(
        con,
        "select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json as sk, date_key "
        "from build group by all having count(*) > 1)",
    )[0][0]
    gold["build_check"] = q(
        con,
        "select measure_key, count(*), count(distinct geography_key || breakdown_json), "
        "count(*) filter (where geography_key = '_X'), min(date_key), max(date_key), count(distinct date_key) "
        "from build group by 1",
    )
    gold["unmatched_codes"] = q(con, "select geography_source_code, count(*) from build where geography_key = '_X' group by 1", 10)
    gold["null_nan_or_negative"] = q(con, "select count(*) from build where value is null or isnan(value) or value < 0")[0][0]
    gold["pl_notices_by_month"] = q(
        con,
        "select strftime(date_key, '%Y-%m'), json_extract_string(breakdown_json, '$.notice_type'), value from build "
        "where geography_key = 'PL' and measure_key = 'uzp_bzp:notices:notices' order by 1, 2",
        40,
    )
    gold["pl_contract_value_by_month_mln"] = q(
        con,
        "select strftime(date_key, '%Y-%m'), round(value / 1e6, 1) from build where geography_key = 'PL' "
        "and measure_key like '%contract_value' order by 1",
    )
    gold["voivodeships_vs_pl_2026_10"] = q(
        con,
        "select measure_key, json_extract_string(breakdown_json, '$.notice_type'), "
        "sum(value) filter (where geography_key like 'TERC:%'), max(value) filter (where geography_key = 'PL'), "
        "count(*) filter (where geography_key like 'TERC:%') from build where date_key = date '2026-10-01' group by 1, 2",
    )
    gold["top_voivodeships_2026_10_total"] = q(
        con,
        "select geography_key, value from build where date_key = date '2026-10-01' and measure_key = 'uzp_bzp:notices:notices' "
        "and is_total and geography_key like 'TERC:%' order by value desc limit 3",
    )
    entity_sql = render(Path("warehouse/models/gold/uzp_bzp_notice.sql").read_text(), refs, {})
    ent = []
    for _ in (1, 2):
        con.execute(f"create or replace table entity as {entity_sql}")
        ent.append(con.execute("select count(*) from entity").fetchone()[0])
    gold["entity_rows_two_builds"] = ent
    gold["entity_distinct_nulldate_geo"] = q(
        con,
        "select count(distinct notice_number), count(*) filter (where date_key is null), "
        "count(*) filter (where geography_key = '_X'), count(*) filter (where geography_key = '_Z') from entity",
    )[0]


def stats_month(month_start: str, month_end: str) -> dict:
    """The API's own count of the month (notice/stats), one request: the spot check of the Gold counts."""
    import requests

    from ingestion.to_landing.uzp_bzp import extract as landing

    session = requests.Session()
    session.headers.update({"User-Agent": landing.USER_AGENT, "Accept": "application/json"})
    body = landing.get(
        session, landing.STATS, {"PublicationDateFrom": f"{month_start}T00:00:00", "PublicationDateTo": f"{month_end}T23:59:59"}
    )
    return {a["noticeType"]: a["numberOfNotices"] for a in json.loads(body) if a["noticeType"] in landing.TYPES.values()}


def trial() -> int:
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.uzp_bzp import load as bronze
    from ingestion.to_landing.uzp_bzp import extract as landing

    bronze.annotate = collect
    landing.annotate = collect
    result: dict = {}
    last = landing.last_day()
    started = time.monotonic()
    result["landing_exit"] = [landing.extract(300, landing_root=SCRATCH, last=last) for _ in (1, 2)]
    result["landing_minutes"] = round((time.monotonic() - started) / 60, 1)
    result["extract"] = [{k: v for k, v in r.items() if k != "level"} for r in COLLECTED.get("uzp_bzp landing load", [])]
    result["verify_exit"] = landing.verify(landing_root=SCRATCH)
    result["verify"] = COLLECTED.get("uzp_bzp landing verify")
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    receipts, data = landing.listing(client, bucket, SCRATCH)
    held = landing.read_receipts(client, bucket, [rows[-1][1] for rows in receipts.values()])
    result["landing_held"] = {
        "entities": len(receipts),
        "receipts": sum(len(v) for v in receipts.values()),
        "data_files": len(data),
        "bytes_landed_newest": sum(r["size_bytes"] for r in held),
        "bytes_original_pages": sum(p["size_bytes"] for r in held for p in r["original_pages"]),
        "pages": sum(len(r["original_pages"]) for r in held),
        "entities_with_two_copies": sorted(e for e, k in receipts.items() if len(k) > 1)[:10],
        "days": len({r["publication_day"] for r in held}),
        "empty_files": sum(r["stats_count"] == 0 for r in held),
        "notices_by_receipts": {
            t: sum(r["stats_count"] for r in held if r["notice_type"] == t) for t in landing.TYPES.values()
        },
        "first_last": [min(receipts), max(receipts)] if receipts else None,
    }
    note("uzp_bzp trial Landing (scratch zz_dev_uzp_bzp)", result)
    t0 = time.monotonic()
    result_b: dict = {"bronze_exit": [bronze.load(m, landing_root=SCRATCH, namespace=SCRATCH) for m in (190, 20)]}
    result_b["bronze_minutes"] = round((time.monotonic() - t0) / 60, 1)
    result_b["loads"] = COLLECTED.get("uzp_bzp Bronze from Landing")
    cat = catalog()
    con = duckdb.connect()
    con.register("bronze_bzp", cat.load_table((SCRATCH, "uzp_bzp_notices")).scan().to_arrow())
    try:
        rejected = cat.load_table((SCRATCH, "uzp_bzp_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    result_b["rejected"] = [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:5]
    result_b["profile"] = bronze_profile(con)
    note("uzp_bzp trial Bronze (scratch)", result_b)
    result_s: dict = {}
    gold: dict = {}
    models(con, result_s, gold)
    tests_keys = ("silver_rows_two_builds", "silver_tests_failing_rows", "range_rows_days", "per_type", "contract_value",
                  "contract_value_columns", "currencies", "estimated_value", "parts_concluded_cancelled")
    note("uzp_bzp trial Silver tests (local DuckDB over scratch Bronze)", {k: result_s[k] for k in tests_keys})
    note("uzp_bzp trial Silver profile", {k: v for k, v in result_s.items() if k not in tests_keys})
    gold["api_stats_2026_10"] = stats_month("2026-10-01", "2026-10-31")
    note("uzp_bzp trial Gold (build stage, entity table)", gold)
    failing = sum(result_s["silver_tests_failing_rows"].values())
    return 1 if failing or gold["gold_dup_series_date"] or 1 in result["landing_exit"] else 0


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
    note("uzp_bzp trial scratch removed", removed)


def main() -> int:
    status = 1
    try:
        status = trial()
    except Exception as exc:
        import traceback

        note(
            "uzp_bzp trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1500:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
