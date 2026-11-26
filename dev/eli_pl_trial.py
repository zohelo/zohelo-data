"""Dev trial for #483 (ELI Poland, PLG-026); test branch only, never on main.

Landing -> Bronze -> Silver -> Gold into scratch, each step twice. Landing into the prefix zz_dev_eli_pl/ (pass 2
must land nothing new), then inspect and verify. Bronze into the Iceberg namespace and lake prefix of the same name
(twice: the second appends nothing). Silver, the Gold entity table and the build stage are the dbt SQL rendered with
Jinja and run in local DuckDB over the scratch Bronze tables, each built twice, with the model tests and the
completeness test. One request to the API documentation page for its licence wording. Everything written is removed
in `finally`; never writes a production namespace or prefix. The host api.sejm.gov.pl: one request at a time, 2 s
apart, robots.txt first (the source's own Landing code).
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import time

import duckdb
from jinja2 import Environment

from ingestion.common import report as report_module

SCRATCH = "zz_dev_eli_pl"
MACROS = "".join(p.read_text() for p in sorted(Path("warehouse/macros").glob("*.sql")))
MACROS += Path("warehouse/macros/sources/eli_pl.sql").read_text()
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


def outcomes(title: str) -> dict:
    out: dict[str, int] = {}
    for entry in COLLECTED.get(title, []):
        for f in entry.get("files", []):
            key = f.get("outcome") or f.get("state") or "?"
            out[key] = out.get(key, 0) + 1
    return out


def licence_page() -> dict:
    """The API documentation page: does it state terms? (one request; words searched, nothing stored)."""
    import requests

    from ingestion.to_landing.patterns.bulk_file import USER_AGENT

    try:
        page = requests.get("https://api.sejm.gov.pl/eli.html", headers={"User-Agent": USER_AGENT}, timeout=60)
    except requests.RequestException as exc:
        return {"error": type(exc).__name__}
    text = re.sub(r"<[^>]+>", " ", page.text)
    words = r"licenc\w*|licen[cs]e|warunk\w* (?:korzystania|użytkowania)|terms of use|prawa autorskie|copyright|CC BY\S*|attribution|uznanie autorstwa|ponowne wykorzystywanie|limit\w*"
    hits = []
    for m in re.finditer(words, text, flags=re.IGNORECASE):
        hits.append(" ".join(text[max(0, m.start() - 80) : m.end() + 80].split()))
    return {"status": page.status_code, "bytes": len(page.content), "hits": hits[:8], "hit_count": len(hits)}


def landing_step(landing) -> None:
    t0 = time.monotonic()
    passes = []
    for _ in (1, 2):
        COLLECTED.pop("eli_pl landing load", None)
        exit_code = landing.run("load", SCRATCH)
        passes.append({"exit": exit_code, "outcomes": outcomes("eli_pl landing load")})
    minutes = round((time.monotonic() - t0) / 60, 1)
    inspect_exit = landing.run("inspect", SCRATCH)
    COLLECTED.pop("eli_pl landing verify", None)
    verify_exit = landing.run("verify", SCRATCH)
    verify = COLLECTED.get("eli_pl landing verify", [{}])[-1]
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = size = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
        for o in page.get("Contents", []):
            if not o["Key"].endswith(".receipt.json"):
                files += 1
                size += o["Size"]
    inspect = {k: v for k, v in COLLECTED.get("eli_pl landing inspect", [{}])[-1].items() if k != "level"}
    note(
        f"eli_pl trial Landing (scratch {SCRATCH})",
        {
            "pass1": passes[0],
            "pass2": passes[1],
            "minutes_both": minutes,
            "inspect_exit": inspect_exit,
            "inspect_missing": inspect.get("missing"),
            "inspect_held": inspect.get("held"),
            "verify_exit": verify_exit,
            "verify_missing": sum(f.get("missing_data", 0) for f in verify.get("files", [])),
            "files": files,
            "bytes": size,
        },
    )


def bronze_step(bronze, con) -> dict[str, str]:
    from ingestion.common.r2 import catalog

    t0 = time.monotonic()
    loads = []
    for _ in (1, 2):
        COLLECTED.pop("eli_pl Bronze from Landing", None)
        exit_code = bronze.load(30, landing_root=SCRATCH, namespace=SCRATCH)
        entries = COLLECTED.get("eli_pl Bronze from Landing", [])
        loads.append(
            {
                "exit": exit_code,
                "appended_files": sum(e.get("appended_files", 0) for e in entries),
                "rows_added": sum(e.get("rows_added", 0) for e in entries),
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
    note(
        f"eli_pl trial Bronze (scratch {SCRATCH})",
        {
            "loads": loads,
            "minutes": round((time.monotonic() - t0) / 60, 1),
            "rows": {t: count(con, f"select * from {v}") for t, v in sources.items()},
            "files": {t: count(con, f"select distinct raw_sha256 from {v}") for t, v in sources.items()},
            "rejected_count": len(rejected),
            "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:6],
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


def failing(con, tests: dict[str, str]) -> tuple[dict, dict]:
    result = {k: count(con, sql) for k, sql in tests.items()}
    return result, {k: q(con, tests[k], 4) for k, c in result.items() if c}


def bounds() -> list:
    text = Path("warehouse/macros/value_range/eli_pl.sql").read_text()
    return [list(b) for b in re.findall(r"\['(\w+)', '([^']+)', '([-\d.e]+)', '([-\d.e]+)'\]", text)]


def main_trial(con) -> None:
    from ingestion.to_bronze.eli_pl import load as bronze
    from ingestion.to_landing import eli_pl as landing

    bronze.annotate = collect
    landing_step(landing)
    s = bronze_step(bronze, con)
    a = s["eli_pl_acts"]
    note(
        "eli_pl trial Bronze profile",
        {
            "repeated_positions_in_year": q(
                con,
                f"select a.publisher, a.year, a.position, a.volume, a.eli_id, left(a.title, 50) from {a} a "
                f"join (select publisher, year, position from {a} group by all having count(*) > 1) d "
                f"using (publisher, year, position) order by 1, 2, 3, 4",
                30,
            ),
            "repeated_address": count(
                con, f"select 1 from {a} group by publisher, year, volume, position having count(*) > 1"
            ),
            "volume_null_or_empty": count(con, f"select 1 from {a} where coalesce(volume, '') = ''"),
            "publishers": q(con, f"select publisher, name, acts_count, years_listed from {s['eli_pl_publishers']}"),
            "acts": q(
                con,
                f"select publisher, count(*), count(distinct eli_id), count(distinct entity), min(year::int), "
                f"max(year::int), count(*) filter (where coalesce(promulgation, '') = ''), "
                f"count(*) filter (where try_cast(position as int) is null), count(*) filter (where act_type is null), "
                f"count(*) filter (where status is null), min(promulgation), max(promulgation) from {a} group by 1",
            ),
            "non_date_promulgation": q(
                con,
                f"select promulgation, count(*) from {a} where promulgation <> '' and try_cast(promulgation as date) "
                f"is null group by 1",
                5,
            ),
            "types": count(con, f"select distinct act_type from {a}"),
            "statuses": count(con, f"select distinct status from {a}"),
            "licence_page": licence_page(),
        },
    )
    silver = build(con, ["eli_pl_acts"], "silver/eli_pl", {}, s)
    tests = {
        "unique eli_id": "select eli_id from eli_pl_acts group by all having count(*) > 1",
        "unique address (publisher, year, volume, position)": "select publisher, year, volume, position "
        "from eli_pl_acts group by all having count(*) > 1",
        "not_null": "select 1 from eli_pl_acts where eli_id is null or publisher is null or year is null "
        "or volume is null or position is null or act_type is null or status is null or raw_sha256 is null",
        "accepted publisher": "select 1 from eli_pl_acts where publisher not in ('DU', 'MP')",
        "eli_pl_acts_complete": render("warehouse/tests/eli_pl_acts_complete.sql"),
    }
    yml = Path("warehouse/models/silver/eli_pl/_eli_pl.yml").read_text()
    for column in ("act_type", "status"):
        found = re.search(rf"name: {column}\n(?:.*\n)*?.*values: \[([^\]]*)\]", yml)
        if found and "'" in found.group(1):
            tests[f"accepted {column} (warn)"] = f"select 1 from eli_pl_acts where {column} not in ({found.group(1)})"
    result, samples = failing(con, tests)
    note(
        "eli_pl trial Silver (local DuckDB over scratch Bronze)",
        {
            "rows_two_builds": silver,
            "tests_failing_rows": result,
            "failing_samples": samples,
            "positions_repeated_in_year_kept": count(
                con, "select publisher, year, position from eli_pl_acts group by all having count(*) > 1"
            ),
            "acts": q(
                con,
                "select publisher, count(*), count(distinct eli_id), max(acts_listed), min(year), max(year), "
                "min(promulgation_date), max(promulgation_date), count(*) filter (where promulgation_date is null) "
                "from eli_pl_acts group by 1 order by 1",
            ),
        },
    )
    con.execute("create or replace table geo as select 'PL' as iso2, 'PL' as geography_key, 'country' as level_name")
    con.execute(
        "create or replace view gmap as with " + render("{{ poc_geography__eli_pl() }}") + " select * from eli_pl"
    )
    gold = build(con, ["legal_acts", "poc_build_eli_pl"], "gold", {"poc_map_geography": "gmap"}, s)
    m = "poc_build_eli_pl"
    kpi = "eli_pl:acts:acts:DU"
    lo, hi = bounds()[0][2:4] if bounds() else ("1", "5000")
    note(
        "eli_pl trial Gold build_eli_pl and legal_acts",
        {
            "rows_two_builds": gold,
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
            "kpi_rows_out_of_bounds": count(
                con, f"select 1 from {m} where measure_key = '{kpi}' and is_total and (value < {lo} or value > {hi})"
            ),
            "kpi_rows": count(con, f"select 1 from {m} where measure_key = '{kpi}' and is_total"),
            "measures": q(
                con,
                f"select measure_key, count(*), count(distinct breakdown_json), min(date_key), max(date_key), "
                f"max(value) filter (where is_total), arg_max(period_label, value) filter (where is_total) "
                f"from {m} group by 1 order by 1",
            ),
            "status_by_measure": q(
                con,
                f"select measure_key, observation_status_key, count(*) from {m} where is_total group by all order by all",
            ),
            "journal_years_left_out": q(
                con,
                "select publisher, count(*) filter (where undated > 0.01 * acts), "
                "count(*) filter (where undated > 0 and undated <= 0.01 * acts), count(*) "
                "from (select publisher, year, count(*) filter (where promulgation_date is null) undated, count(*) acts "
                "from eli_pl_acts group by all) group by 1 order by 1",
            ),
            "legal_acts_unique": count(con, "select eli_id from legal_acts group by 1 having count(*) > 1"),
            "spot_poland": q(
                con,
                f"select measure_key, period_label, breakdown_json, value, observation_status_key from {m} "
                f"where period_label in ('2025-12', '2026-10') and (is_total or breakdown_json like '%Ustawa%') "
                f"order by 1, 2, 3",
            ),
            "year_2025": q(con, "select publisher, count(*) from eli_pl_acts where year = 2025 group by 1 order by 1"),
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
    note("eli_pl trial scratch removed", removed)


def main() -> int:
    status = 0
    con = duckdb.connect()
    print(json.dumps({"duckdb": duckdb.__version__}), flush=True)
    try:
        main_trial(con)
    except Exception as exc:
        import traceback

        status = 1
        note(
            "eli_pl trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1800:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
