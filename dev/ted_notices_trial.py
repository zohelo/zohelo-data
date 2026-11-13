"""Dev trial for #2976 (TED notices of Polish buyers; test branch only, never on main).

Landing -> Bronze -> Silver -> Gold into scratch, each step twice. Landing: the whole window (2025-01-01 ..
yesterday UTC) into the Landing prefix zz_dev_ted_notices/ (pass 1, about 1,600 requests, one at a time); pass 2
asks only the newest three days again and must land nothing. Bronze: into the Iceberg namespace zz_dev_ted_notices
and lake prefix zz_dev_ted_notices/ (twice: the second appends nothing). Silver and Gold are the dbt SQL rendered with
Jinja and run in local DuckDB over the scratch Bronze table, each built twice. Everything written is removed in
`finally`; never writes a production namespace or prefix.
"""

from __future__ import annotations

from collections import Counter
from datetime import date
import json
from pathlib import Path
import time

import duckdb
from jinja2 import Environment

from ingestion.common import report as report_module

SCRATCH = "zz_dev_ted_notices"
MACRO_FILES = [*sorted(Path("warehouse/macros").glob("*.sql")), Path("warehouse/macros/sources/ted_notices.sql")]
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
    t = "bronze_ted"
    cols = [r[0] for r in con.execute(f"describe {t}").fetchall()]
    out = {
        "rows_files_entities": q(
            con,
            f"select count(*), count(distinct raw_sha256), count(distinct entity), count(distinct publication_number) from {t}",
        )[0],
        "days_first_last": q(
            con, f"select min(source_version), max(source_version), count(distinct source_version) from {t}"
        )[0],
        "dup_publication_numbers": q(
            con, f"select count(*) from (select publication_number from {t} group by 1 having count(*) > 1)"
        )[0][0],
        "nulls": {c: n for c in cols if (n := q(con, f"select count(*) filter (where {c} is null) from {t}")[0][0])},
        "rows_per_entity": q(
            con,
            f"select n, count(*) from (select entity, count(*) n from {t} group by 1) group by 1 order by 2 desc limit 6",
        ),
    }
    shapes = {}
    for c in cols:
        if c in ("entity", "raw_sha256", "source_version", "ingested_at", "source_row_number", "publication_number"):
            continue
        shapes[c] = q(con, f"select left({c}, 1), count(*) from {t} group by 1 order by 2 desc limit 4")
    out["first_char"] = shapes
    return out


def models(con, result: dict) -> None:
    silver_sql = render(
        Path("warehouse/models/silver/ted_notices/ted_notices.sql").read_text(), {}, {"ted_notices": "bronze_ted"}
    )
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table ted_notices as {silver_sql}")
        counts.append(con.execute("select count(*) from ted_notices").fetchone()[0])
    result["silver_rows_two_builds"] = counts
    result.update(silver_checks(con))
    gmap = render(
        "with nuts_pl as ({{ poc_nuts_pl() }}), {{ poc_geography__ted_notices() }} select * from ted_notices", {}, {}
    )
    con.execute(f"create or replace view gmap as {gmap}")
    build_sql = render(
        Path("warehouse/models/gold/poc_build_ted_notices.sql").read_text(),
        {"ted_notices": "ted_notices", "poc_map_geography": "gmap"},
        {},
    )
    gold = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build_sql}")
        gold.append(con.execute("select count(*) from build").fetchone()[0])
    result["gold_rows_two_builds"] = gold
    result["gold_dup_series_date"] = q(
        con,
        "select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json as sk, date_key "
        "from build group by all having count(*) > 1)",
    )[0][0]
    result["build_check"] = q(
        con,
        "select block_id, count(*), count(distinct measure_key || '|' || geography_key || '|' || breakdown_json), "
        "count(*) filter (where geography_key = '_X'), min(date_key), max(date_key), count(distinct date_key) from build group by 1",
    )
    result["unmatched_codes"] = q(
        con, "select geography_source_code, count(*) from build where geography_key = '_X' group by 1", 10
    )
    result["gold_null_or_nan"] = q(con, "select count(*) from build where value is null or isnan(value)")[0][0]
    result["geographies"] = q(con, "select geography_key, count(*) from build group by 1 order by 1", 25)
    # what poc_concept_metrics keeps: total series, matched areas
    con.execute("create or replace table metrics as select * from build where is_total and geography_key <> '_X'")
    result["metrics_rows_max_per_area_month_minmax"] = q(
        con,
        "select count(*), max(n), min(v), max(v) from (select geography_key, date_key, count(*) n, max(value) v "
        "from metrics group by all)",
    )[0]
    result["metrics_pl"] = q(
        con, "select strftime(date_key, '%Y-%m'), value from metrics where geography_key = 'PL' order by date_key", 30
    )
    result["metrics_max_voivodeship"] = q(
        con,
        "select geography_key, date_key, value from metrics where geography_key like 'TERC:%' order by value desc limit 3",
    )
    result["voivodeship_sum_vs_pl"] = q(
        con,
        "select strftime(m.date_key, '%Y-%m'), sum(m.value) filter (where m.geography_key like 'TERC:%'), "
        "max(m.value) filter (where m.geography_key = 'PL') from metrics m group by m.date_key order by m.date_key limit 4",
    )
    result["breakdowns_pl_newest"] = q(
        con,
        "select breakdown_json, value from build where geography_key = 'PL' and date_key = (select max(date_key) from build) "
        "and not is_total order by value desc",
        12,
    )
    entity_sql = render(
        Path("warehouse/models/gold/ted_notice.sql").read_text(),
        {"ted_notices": "ted_notices", "poc_map_geography": "gmap"},
        {},
    )
    ent = []
    for _ in (1, 2):
        con.execute(f"create or replace table entity as {entity_sql}")
        ent.append(con.execute("select count(*) from entity").fetchone()[0])
    result["gold_entity_rows_two_builds"] = ent
    result["gold_entity_unique_nulls"] = q(
        con,
        "select count(distinct publication_number), count(*) filter (where date_key is null), "
        "count(*) filter (where list_contains(buyer_geography_keys, '_X')), count(*) filter (where len(buyer_geography_keys) = 0), "
        "count(*) filter (where len(buyer_geography_keys) > 1) from entity",
    )[0]


def silver_checks(con) -> dict:
    t = "ted_notices"
    tests = {
        "unique publication_number": f"select publication_number from {t} group by 1 having count(*) > 1",
        "not_null keys": f"select 1 from {t} where publication_number is null or publication_year is null or notice_type is null "
        "or form_type is null or publication_date is null or buyer_countries is null or entity is null or raw_sha256 is null",
        "pages complete (tests/ted_notices_pages_complete.sql)": render(
            Path("warehouse/tests/ted_notices_pages_complete.sql").read_text(), {"ted_notices": t}, {}
        ),
        "publication_date = entity day": f"select 1 from {t} where strftime(publication_date, '%Y%m%d') <> split_part(entity, '_', 2)",
        "no POL buyer": f"select 1 from {t} where not list_contains(buyer_countries, 'POL')",
    }
    import yaml

    spec = yaml.safe_load(Path("warehouse/models/silver/ted_notices/_ted_notices.yml").read_text())
    for column in spec["models"][0]["columns"]:
        for test in column.get("tests", []):
            if isinstance(test, dict) and "accepted_values" in test:
                values = ", ".join("'" + v + "'" for v in test["accepted_values"]["values"])
                tests[f"accepted_values {column['name']}"] = (
                    f"select 1 from {t} where {column['name']} is not null and {column['name']} not in ({values})"
                )
    out = {
        "silver_tests_failing_rows": {
            k: con.execute(f"select count(*) from ({v})").fetchone()[0] for k, v in tests.items()
        }
    }
    out["range_rows_days"] = q(
        con, f"select min(publication_date), max(publication_date), count(*), count(distinct publication_date) from {t}"
    )[0]
    out["per_year"] = q(con, f"select year(publication_date), count(*) from {t} group by 1 order by 1")
    for c in ("notice_type", "form_type", "procedure_type", "estimated_value_currency"):
        out[c] = q(con, f"select {c}, count(*) from {t} group by 1 order by 2 desc", 40)
    for c in ("contract_nature", "buyer_legal_types", "buyer_countries"):
        out[c] = q(con, f"select u, count(*) from (select unnest({c}) u from {t}) group by 1 order by 2 desc", 25)
    out["list_lengths"] = q(
        con,
        f"select max(len(buyer_names)), max(len(buyer_nuts)), max(len(cpv_codes)), count(*) filter (where buyer_nuts is null), "
        f"count(*) filter (where buyer_names is null), count(*) filter (where estimated_value is not null), "
        f"count(*) filter (where dispatch_date is null) from {t}",
    )[0]
    out["nuts_prefixes"] = q(
        con, f"select left(u, 4), count(*) from (select unnest(buyer_nuts) u from {t}) group by 1 order by 2 desc", 30
    )
    out["estimated_value"] = q(
        con,
        f"select estimated_value_currency, count(*), min(estimated_value), median(estimated_value), max(estimated_value) from {t} where estimated_value is not null group by 1 order by 2 desc",
        8,
    )
    return out


def precheck(landing) -> bool:
    """The declared fields on one small request: valid, and the shape of each value (two notices)."""
    import requests

    session = requests.Session()
    session.headers.update({"User-Agent": landing.USER_AGENT, "Accept": "application/json"})
    body = {**landing.request_body(date(2026, 11, 10), 1), "limit": 2}
    try:
        answer = json.loads(landing.post(session, body))
    except RuntimeError as exc:
        note("ted_notices trial field precheck failed", {"fields": landing.FIELDS, "error": str(exc)[:1500]}, "error")
        return False
    notices = answer["notices"]
    note(
        "ted_notices trial field precheck",
        {
            "total_2026_11_10": answer.get("totalNoticeCount"),
            "keys": sorted({k for n in notices for k in n}),
            "notice": [
                json.dumps({k: v for k, v in n.items() if k != "links"}, ensure_ascii=False)[:1500] for n in notices
            ],
        },
    )
    return True


def trial() -> int:
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.ted_notices import load as bronze
    from ingestion.to_landing.ted_notices import extract as landing

    bronze.annotate = collect
    landing.annotate = collect
    result: dict = {}
    if not precheck(landing):
        return 1
    last = landing.last_day()
    started = time.monotonic()
    result["landing_exit"] = [landing.extract(300, landing_root=SCRATCH, last=last) for _ in (1, 2)]
    result["landing_minutes"] = round((time.monotonic() - started) / 60, 1)
    result["extract"] = [
        {k: v for k, v in r.items() if k != "level"} for r in COLLECTED.get("ted_notices landing load", [])
    ]
    result["verify_exit"] = landing.verify(landing_root=SCRATCH)
    result["verify"] = COLLECTED.get("ted_notices landing verify")
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    receipts, data = landing.listing(client, bucket, SCRATCH)
    held = landing.read_receipts(client, bucket, [rows[-1][1] for rows in receipts.values()])
    result["landing_held"] = {
        "entities": len(receipts),
        "receipts": sum(len(v) for v in receipts.values()),
        "data_files": len(data),
        "bytes_newest": sum(r["size_bytes"] for r in held),
        "entities_with_two_copies": sorted(e for e, k in receipts.items() if len(k) > 1)[:10],
        "days": len({r["publication_day"] for r in held}),
        "days_empty": len({r["publication_day"] for r in held if r["day_total_notices"] == 0}),
        "pages_per_day": dict(Counter(r["part"]["total"] for r in held if r["part"]["index"] == 1)),
        "notices_by_receipts": sum(r["day_total_notices"] for r in held if r["part"]["index"] == 1),
        "first_last": [min(receipts), max(receipts)],
    }
    note("ted_notices trial Landing (scratch zz_dev_ted_notices)", result)
    t0 = time.monotonic()
    result_b: dict = {"bronze_exit": [bronze.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]}
    result_b["bronze_minutes"] = round((time.monotonic() - t0) / 60, 1)
    result_b["loads"] = COLLECTED.get("ted_notices Bronze from Landing")
    cat = catalog()
    con = duckdb.connect()
    con.register("bronze_ted", cat.load_table((SCRATCH, "ted_notices")).scan().to_arrow())
    try:
        rejected = cat.load_table((SCRATCH, "ted_notices_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    result_b["rejected"] = [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:5]
    result_b["profile"] = bronze_profile(con)
    note("ted_notices trial Bronze (scratch)", result_b)
    result_m: dict = {}
    models(con, result_m)
    silver_keys = [
        k
        for k in result_m
        if not k.startswith(("gold", "build", "unmatched", "geographies", "metrics", "voivodeship", "breakdowns"))
    ]
    note("ted_notices trial Silver (local DuckDB over scratch Bronze)", {k: result_m[k] for k in silver_keys})
    gold = {k: v for k, v in result_m.items() if k not in silver_keys}
    month = "2025-06"
    gold["python_recount_pl_" + month] = recount(client, bucket, receipts, month)
    gold["gold_pl_" + month] = q(
        con, f"select value from metrics where geography_key = 'PL' and strftime(date_key, '%Y-%m') = '{month}'"
    )
    note("ted_notices trial Gold (build, metrics rows, entity)", gold)
    return 0


def recount(client, bucket: str, receipts: dict, month: str) -> list:
    """Notices of one month counted in Python straight from the landed answers (independent of the SQL)."""
    from ingestion.common import bulk_bronze
    from ingestion.to_landing.ted_notices import extract as landing

    keys = [rows[-1][1] for e, rows in receipts.items() if e.startswith("day_" + month.replace("-", ""))]
    numbers, files = set(), 0
    for r in landing.read_receipts(client, bucket, keys):
        body = json.loads(bulk_bronze.read_raw(client, bucket, "ted_notices", r))
        numbers |= {n["publication-number"] for n in body["notices"]}
        files += 1
    return [files, len(numbers)]


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
    note("ted_notices trial scratch removed", removed)


def main() -> int:
    status = 1
    try:
        status = trial()
    except Exception as exc:
        import traceback

        note(
            "ted_notices trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1500:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
