"""Dev trial for #3263 (dane.gov.pl catalogue via data.europa.eu, PLG-030); test branch only, never on main.

Landing -> Bronze -> Silver -> Gold into scratch, Landing and Bronze twice (data.europa.eu search API: one request at a
time, a second apart). Landing into the prefix zz_dev_dane_gov_pl/ (pass 2 must land nothing new), then inspect and
verify. Bronze into the Iceberg namespace and lake prefix of the same name (the second load appends nothing). Silver,
the Gold entity table and the build stage are the dbt SQL rendered with Jinja and run in local DuckDB over the scratch
Bronze tables, each built twice, with the model tests and the completeness tests. Everything written is removed in
`finally`; never writes a production namespace or prefix.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import re
import time

import duckdb
from jinja2 import Environment

from ingestion.common import report as report_module

SOURCE = "dane_gov_pl"
SCRATCH = "zz_dev_dane_gov_pl"
MACROS = "".join(p.read_text() for p in sorted(Path("warehouse/macros").glob("*.sql")))
MACROS += Path("warehouse/macros/sources/dane_gov_pl.sql").read_text()
COLLECTED: dict[str, list] = {}


def collect(level: str, title: str, data) -> None:
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


ANNOTATE = report_module.annotate
report_module.annotate = collect
report_module.step_summary = lambda _m: None


def note(title: str, data, level: str = "notice") -> None:
    """One annotation (at most 9 per run: GitHub keeps 10 per step), also printed to the log in full."""
    print(f"NOTE [{level}] {title}: {json.dumps(data, default=str)}", flush=True)
    ANNOTATE(level, title, data)


def q(con, sql: str, limit: int = 60) -> list:
    return [[v if isinstance(v, int | float) or v is None else str(v) for v in r] for r in con.execute(sql).fetchall()][
        :limit
    ]


def count(con, sql: str) -> int:
    return con.execute(f"select count(*) from ({sql})").fetchall()[0][0]


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


def landing_step() -> dict:
    from ingestion.to_landing.dane_gov_pl import extract

    extract.annotate = collect
    t0 = time.monotonic()
    passes = []
    for _ in (1, 2):
        COLLECTED.pop(f"{SOURCE} landing extract", None)
        code = extract.extract(30, SCRATCH)
        s = COLLECTED[f"{SOURCE} landing extract"][-1]
        passes.append(
            {
                k: s.get(k)
                for k in (
                    "outcome",
                    "records",
                    "count",
                    "distributions",
                    "pages",
                    "repeated",
                    "requests",
                    "mb_read",
                    "seconds",
                    "newest_modified",
                    "newest_harvest_record",
                )
            }
            | {"exit": code}
        )
    COLLECTED.pop(f"{SOURCE} landing inspect", None)
    inspect_exit = extract.inspect(SCRATCH)
    inspect = COLLECTED.get(f"{SOURCE} landing inspect", [{}])[-1]
    from ingestion.to_landing.patterns import bulk_file

    bulk_file.annotate = collect
    COLLECTED.pop(f"{SOURCE} landing verify", None)
    from ingestion.to_landing import dane_gov_pl as landing

    verify_exit = bulk_file.main(SOURCE, "verify", SCRATCH, specs=landing.specs())
    verify = COLLECTED.get(f"{SOURCE} landing verify", [{}])[-1]
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = size = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
        for o in page.get("Contents", []):
            if not o["Key"].endswith(".receipt.json"):
                files += 1
                size += o["Size"]
    result = {
        "pass1": passes[0],
        "pass2": passes[1],
        "minutes_both": round((time.monotonic() - t0) / 60, 1),
        "inspect_exit": inspect_exit,
        "inspect_files": inspect.get("files"),
        "verify_exit": verify_exit,
        "verify_missing": sum(f.get("missing_data", 0) for f in verify.get("files", [])),
        "files": files,
        "bytes": size,
    }
    note(f"{SOURCE} trial Landing (scratch {SCRATCH})", result)
    return result


def bronze_step(con) -> dict[str, str]:
    from ingestion.common import bulk_bronze
    from ingestion.common.r2 import catalog
    from ingestion.to_bronze.dane_gov_pl import load as bronze

    bulk_bronze.annotate = collect
    t0 = time.monotonic()
    loads = []
    for _ in (1, 2):
        COLLECTED.pop(f"{SOURCE} Bronze from Landing", None)
        code = bronze.load(30, landing_root=SCRATCH, namespace=SCRATCH)
        got = COLLECTED.get(f"{SOURCE} Bronze from Landing", [])
        loads.append(
            {
                "exit": code,
                "appended_files": sum(e.get("appended_files", 0) for e in got),
                "rows_added": sum(e.get("rows_added", 0) for e in got),
                "rejected": sum(e.get("rejected", 0) for e in got),
            }
        )
    cat = catalog()
    sources = {}
    for table in bronze.TABLES:
        con.register(f"b_{table}", cat.load_table((SCRATCH, table)).scan().to_arrow())
        sources[table] = f"b_{table}"
    try:
        rejected = cat.load_table((SCRATCH, bronze.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    d, x = sources["dane_gov_pl_datasets"], sources["dane_gov_pl_distributions"]
    profile = {
        "datasets": q(
            con,
            f"select count(*), count(distinct dataset_id), count(distinct raw_sha256), min(modified), max(modified), "
            f"count(*) filter (where modified is null), count(*) filter (where identifier is null), "
            f"count(*) filter (where publisher_name is null), count(*) filter (where publisher_resource is null), "
            f"count(*) filter (where categories = '[]'), count(*) filter (where accrual_periodicity is null), "
            f"count(*) filter (where catalog_record_modified is null), max(catalog_record_modified) from {d}",
        ),
        "distributions": q(
            con,
            f"select count(*), count(distinct distribution_id), count(distinct dataset_id), "
            f"count(*) filter (where modified is null), count(*) filter (where format is null), "
            f"count(*) filter (where byte_size is null), count(*) filter (where try_cast(byte_size as bigint) is null "
            f"and byte_size is not null), min(modified), max(modified) from {x}",
        ),
        "distribution_ids_on_two_datasets": count(
            con, f"select distribution_id from {x} group by 1 having count(distinct dataset_id) > 1"
        ),
        "is_hvd": q(con, f"select is_hvd, count(*) from {d} group by 1 order by 1"),
        "status": q(con, f"select status, count(*) from {x} group by 1 order by 1"),
    }
    collect("notice", f"{SOURCE} trial Bronze profile (log only)", profile)
    note(
        f"{SOURCE} trial Bronze (scratch {SCRATCH})",
        {
            "loads": loads,
            "minutes": round((time.monotonic() - t0) / 60, 1),
            "rows": {t: count(con, f"select * from {v}") for t, v in sources.items()},
            "files": {t: count(con, f"select distinct raw_sha256 from {v}") for t, v in sources.items()},
            "rejected_count": len(rejected),
            "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:4],
            "profile": profile,
        },
    )
    return sources


def build(con, names: list[str], folder: str, refs: dict, sources: dict) -> dict[str, list[int]]:
    rows = {}
    for name in names:
        sql = render(f"warehouse/models/{folder}/{name}.sql", refs, sources)
        rows[name] = []
        for _ in (1, 2):
            con.execute(f"create or replace table {name} as {sql}")
            rows[name].append(count(con, f"select * from {name}"))
    return rows


def accepted(yml: str, model: str, column: str) -> str | None:
    block = yml.split(f"- name: {model}\n", 1)[1].split("\n  - name: ", 1)[0]
    found = re.search(rf"- name: {column}\n(?:\s*#.*\n)?\s+tests: .*?values: \[([^\]]*)\]", block)
    return found.group(1) if found else None


def silver_step(con, sources: dict) -> None:
    names = ["dane_gov_pl_datasets", "dane_gov_pl_dataset_categories", "dane_gov_pl_distribution_months"]
    rows = build(con, names, "silver/dane_gov_pl", {}, sources)
    yml = Path("warehouse/models/silver/dane_gov_pl/_dane_gov_pl.yml").read_text()
    tests = {
        "unique datasets": "select dataset_number from dane_gov_pl_datasets group by 1 having count(*) > 1",
        "unique dataset_id": "select dataset_id from dane_gov_pl_datasets group by 1 having count(*) > 1",
        "unique categories": "select dataset_number, category_code from dane_gov_pl_dataset_categories "
        "group by all having count(*) > 1",
        "unique distribution_months": "select dataset_number, modified_month, format_code "
        "from dane_gov_pl_distribution_months group by all having count(*) > 1",
        "not_null datasets": "select 1 from dane_gov_pl_datasets where dataset_number is null or dataset_id is null "
        "or modified_at is null or modified_on is null or country_code is null or harvest_records is null "
        "or distribution_count is null or raw_sha256 is null",
        "not_null categories": "select 1 from dane_gov_pl_dataset_categories where dataset_number is null "
        "or category_code is null or raw_sha256 is null",
        "not_null distribution_months": "select 1 from dane_gov_pl_distribution_months where dataset_number is null "
        "or modified_month is null or format_code is null or distributions is null or raw_sha256 is null",
    }
    for model, column in (
        ("dane_gov_pl_datasets", "periodicity_code"),
        ("dane_gov_pl_datasets", "publisher_type"),
        ("dane_gov_pl_datasets", "country_code"),
        ("dane_gov_pl_dataset_categories", "category_code"),
        ("dane_gov_pl_distribution_months", "format_code"),
    ):
        values = accepted(yml, model, column)
        tests[f"accepted {column}"] = (
            f"select {column}, count(*) from {model} where {column} not in ({values}) group by 1"
        )
    for name in names:
        tests[f"{name}_complete"] = render(f"warehouse/tests/{name}_complete.sql")
    result = {k: count(con, sql) for k, sql in tests.items()}
    samples = {k: q(con, tests[k], 6) for k, c in result.items() if c}
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    note(
        f"{SOURCE} trial Silver (local DuckDB over scratch Bronze)",
        {
            "rows_two_builds": rows,
            "tests_failing_rows": result,
            "failing_samples": samples,
            "versions": q(
                con,
                "select harvest_records, count(*) from dane_gov_pl_datasets group by 1 order by 1",
            ),
            "range": q(
                con,
                "select min(modified_on), max(modified_on), count(distinct date_trunc('month', modified_on)), "
                "count(distinct publisher_number), count(*) filter (where publisher_number is null), "
                "count(*) filter (where publisher_name is null), count(*) filter (where category_count = 0), "
                "sum(distribution_count) from dane_gov_pl_datasets",
            ),
            "distribution_months": q(
                con,
                "select count(*), sum(distributions), sum(sized), min(modified_month), max(modified_month), "
                "count(distinct format_code) from dane_gov_pl_distribution_months",
            ),
            "harvest_lag": q(
                con,
                f"select round(date_diff('minute', max(modified_at), timestamptz '{now}+00') / 60.0, 1), "
                f"round(date_diff('minute', max(harvest_modified_at), timestamptz '{now}+00') / 60.0, 1), "
                f"round(median(date_diff('minute', modified_at, harvest_modified_at)) "
                f"filter (where modified_at >= timestamptz '{now}+00' - interval 30 day) / 60.0, 1), "
                f"round(quantile_cont(date_diff('minute', modified_at, harvest_modified_at), 0.9) "
                f"filter (where modified_at >= timestamptz '{now}+00' - interval 30 day) / 60.0, 1), "
                f"count(*) filter (where harvest_modified_at < modified_at) from dane_gov_pl_datasets",
            ),
            "periodicity": q(
                con, "select periodicity_code, count(*) from dane_gov_pl_datasets group by 1 order by 2 desc"
            ),
        },
    )


def geo(con) -> None:
    con.execute("create or replace table geo as select 'PL' as iso2, 'PL' as geography_key, 'country' as level_name")
    con.execute(
        "create or replace view gmap as with "
        + render("{{ poc_geography__dane_gov_pl() }}")
        + " select * from dane_gov_pl"
    )


def bounds() -> list:
    text = Path("warehouse/macros/value_range/dane_gov_pl.sql").read_text()
    return re.findall(r"\['(\w+)', '([^']+)', '([\d.e-]+)', '([\d.e]+)'\]", text)


def gold_step(con, sources: dict) -> None:
    rows = build(
        con, ["open_data_datasets_pl", "poc_build_dane_gov_pl"], "gold", {"poc_map_geography": "gmap"}, sources
    )
    m = "poc_build_dane_gov_pl"
    kpis = {
        "open_data_datasets_modified": "dane_gov_pl:catalogue:datasets:total:monthly",
        "open_data_distributions_modified": "dane_gov_pl:catalogue:distributions:total:monthly",
    }
    out_of_bounds = {
        c: count(con, f"select 1 from {m} where measure_key = '{kpis[c]}' and (value < {lo} or value > {hi})")
        for c, _p, lo, hi in bounds()
    }
    note(
        f"{SOURCE} trial Gold (entity table and build_dane_gov_pl)",
        {
            "rows_two_builds": rows,
            "dup_series_date": count(
                con,
                f"select measure_key || '|' || geography_key || '|' || breakdown_json, date_key from {m} "
                f"group by all having count(*) > 1",
            ),
            "kpi_rows_per_area_period_max": q(
                con,
                f"select measure_key, max(n) from (select measure_key, geography_key, date_key, count(*) n from {m} "
                f"where is_total group by all) group by 1",
            ),
            "unmatched_geography": count(con, f"select 1 from {m} where geography_key = '_X'"),
            "null_or_nan": count(con, f"select 1 from {m} where value is null or isnan(value) or date_key is null"),
            "value_bounds": bounds(),
            "rows_out_of_bounds": out_of_bounds,
            "measures": q(
                con,
                f"select measure_key, count(*), count(distinct breakdown_json), min(date_key), max(date_key), "
                f"max(value), sum(value) filter (where is_total), "
                f"count(*) filter (where observation_status_key = 'provisional') from {m} group by 1 order by 1",
            ),
            "largest_month": q(
                con, f"select measure_key, arg_max(period_label, value), max(value) from {m} where is_total group by 1"
            ),
            "spot_pl": q(
                con,
                f"select measure_key, period_label, value, observation_status_key from {m} where is_total "
                f"and period_label in ('2025-12', '2026-01', '2026-10', '2026-11') order by 1, 2",
            ),
            "spot_publishers": q(
                con,
                f"select breakdown_label_json, sum(value) from {m} "
                f"where measure_key = 'dane_gov_pl:catalogue:datasets:publisher:monthly' group by 1 order by 2 desc",
                5,
            ),
            "totals_match_silver": q(
                con,
                f"select (select sum(value) from {m} where measure_key = '{kpis['open_data_datasets_modified']}'), "
                f"(select count(*) from dane_gov_pl_datasets), "
                f"(select sum(value) from {m} where measure_key = '{kpis['open_data_distributions_modified']}'), "
                f"(select sum(distributions) from dane_gov_pl_distribution_months)",
            ),
            "entity_sample": q(
                con,
                "select dataset_number, publisher_number, periodicity_code, distribution_count, date_key, "
                "harvest_lag_hours from open_data_datasets_pl where dataset_number in (288, 1067, 7044) order by 1",
            ),
        },
    )


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
    note(f"{SOURCE} trial scratch removed", removed)


def main() -> int:
    status = 0
    con = duckdb.connect()
    collect("notice", f"{SOURCE} trial versions (log only)", {"duckdb": duckdb.__version__})
    try:
        geo(con)
        landing_step()
        sources = bronze_step(con)
        silver_step(con, sources)
        gold_step(con, sources)
    except Exception as exc:
        import traceback

        status = 1
        note(
            f"{SOURCE} trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1800:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
