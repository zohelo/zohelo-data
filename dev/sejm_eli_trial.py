"""Dev trial for #481 (Sejm API, PLG-024) and #483 (ELI Poland, PLG-026); test branch only, never on main.

Landing -> Bronze -> Silver -> Gold into scratch, each step twice, one source after the other (both use the host
api.sejm.gov.pl: one request at a time, 2 s apart, robots.txt first). Landing into the prefixes zz_dev_sejm/ and
zz_dev_eli_pl/ (pass 2 must land nothing new), then inspect and verify. Bronze into the Iceberg namespaces and lake
prefixes of the same names (twice: the second appends nothing). Silver, the Gold entity tables and the build stages
are the dbt SQL rendered with Jinja and run in local DuckDB over the scratch Bronze tables, each built twice, with
the model tests and the completeness tests. Everything written is removed in `finally`; never writes a production
namespace or prefix. Never prints MP personal data (the MP list is reduced before Landing).
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import time

import duckdb
from jinja2 import Environment

from ingestion.common import report as report_module

SCRATCH = {"sejm": "zz_dev_sejm", "eli_pl": "zz_dev_eli_pl"}
MACROS = "".join(p.read_text() for p in sorted(Path("warehouse/macros").glob("*.sql")))
MACROS += (
    Path("warehouse/macros/sources/sejm.sql").read_text() + Path("warehouse/macros/sources/eli_pl.sql").read_text()
)
COLLECTED: dict[str, list] = {}


def collect(level: str, title: str, data) -> None:
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


ANNOTATE = report_module.annotate
report_module.annotate = collect
report_module.step_summary = lambda _m: None


def note(title: str, data, level: str = "notice") -> None:
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
    """Landing outcomes per kind from the bulk_file summaries collected under title."""
    out: dict[str, int] = {}
    for entry in COLLECTED.get(title, []):
        for f in entry.get("files", []):
            out[f.get("outcome") or f.get("state") or "?"] = out.get(f.get("outcome") or f.get("state") or "?", 0) + 1
    return out


def landing_step(source: str, landing) -> None:
    root = SCRATCH[source]
    t0 = time.monotonic()
    passes = []
    for _ in (1, 2):
        COLLECTED.pop(f"{source} landing load", None)
        exit_code = landing.run("load", root)
        passes.append({"exit": exit_code, "outcomes": outcomes(f"{source} landing load")})
    minutes = round((time.monotonic() - t0) / 60, 1)
    inspect_exit = landing.run("inspect", root)
    COLLECTED.pop(f"{source} landing verify", None)
    verify_exit = landing.run("verify", root)
    verify = COLLECTED.get(f"{source} landing verify", [{}])[-1]
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = size = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{root}/"):
        for o in page.get("Contents", []):
            if not o["Key"].endswith(".receipt.json"):
                files += 1
                size += o["Size"]
    note(
        f"{source} trial Landing (scratch {root})",
        {
            "pass1": passes[0],
            "pass2": passes[1],
            "minutes_both": minutes,
            "inspect_exit": inspect_exit,
            "inspect": {k: v for k, v in COLLECTED.get(f"{source} landing inspect", [{}])[-1].items() if k != "level"},
            "verify_exit": verify_exit,
            "verify_missing": sum(f.get("missing_data", 0) for f in verify.get("files", [])),
            "files": files,
            "bytes": size,
        },
    )


def bronze_step(source: str, bronze, con) -> dict[str, str]:
    from ingestion.common.r2 import catalog

    root = SCRATCH[source]
    t0 = time.monotonic()
    loads = []
    for _ in (1, 2):
        COLLECTED.pop(f"{source} Bronze from Landing", None)
        exit_code = bronze.load(30, landing_root=root, namespace=root)
        loads.append(
            {
                "exit": exit_code,
                "appended_files": sum(
                    e.get("appended_files", 0) for e in COLLECTED.get(f"{source} Bronze from Landing", [])
                ),
                "rows_added": sum(e.get("rows_added", 0) for e in COLLECTED.get(f"{source} Bronze from Landing", [])),
            }
        )
    cat = catalog()
    sources = {}
    for table in bronze.TABLES:
        con.register(f"b_{table}", cat.load_table((root, table)).scan().to_arrow())
        sources[table] = f"b_{table}"
    try:
        rejected = cat.load_table((root, bronze.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    note(
        f"{source} trial Bronze (scratch {root})",
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


def bounds(concepts: list[str]) -> list:
    text = Path("warehouse/tests/metrics_value_range.sql").read_text()
    return [b for b in re.findall(r"\('(\w+)', '(\w+)', '%', ([\d.e-]+), ([\d.e]+)\)", text) if b[0] in concepts]


def gold_checks(con, model: str, kpis: dict[str, str]) -> dict:
    out_of_bounds = {
        c: count(
            con,
            f"select 1 from {model} where measure_key = '{kpis[c]}' and is_total and (value < {lo} or value > {hi})",
        )
        for c, _s, lo, hi in bounds(list(kpis))
    }
    return {
        "dup_series_date": count(
            con,
            f"select measure_key || '|' || geography_key || '|' || breakdown_json, date_key from {model} "
            f"group by all having count(*) > 1",
        ),
        "kpi_rows_per_area_period_max": q(
            con,
            f"select measure_key, max(n) from (select measure_key, geography_key, date_key, count(*) n from {model} "
            f"where is_total group by all) group by 1",
        ),
        "unmatched_geography": count(con, f"select 1 from {model} where geography_key = '_X'"),
        "null_or_nan": count(con, f"select 1 from {model} where value is null or isnan(value) or date_key is null"),
        "value_bounds": bounds(list(kpis)),
        "rows_out_of_bounds": out_of_bounds,
        "measures": q(
            con,
            f"select measure_key, count(*), count(distinct breakdown_json), min(date_key), max(date_key), "
            f"max(value) filter (where is_total), count(*) filter (where observation_status_key = 'provisional') "
            f"from {model} group by 1 order by 1",
        ),
    }


def geo(con) -> None:
    con.execute("create or replace table geo as select 'PL' as iso2, 'PL' as geography_key, 'country' as level_name")
    con.execute(
        "create or replace view gmap as with "
        + render("{{ poc_geography__sejm() }}, {{ poc_geography__eli_pl() }}")
        + " select * from sejm union all select * from eli_pl"
    )


def sejm_trial(con) -> None:
    from ingestion.to_bronze.sejm import load as bronze
    from ingestion.to_landing import sejm as landing

    bronze.annotate = collect
    landing_step("sejm", landing)
    s = bronze_step("sejm", bronze, con)
    p, vd, v, pr, mp = (
        s[t] for t in ("sejm_proceedings", "sejm_voting_days", "sejm_votings", "sejm_prints", "sejm_mps")
    )
    note(
        "sejm trial Bronze profile",
        {
            "terms": q(
                con, f"select term, date_from, date_to, is_current, prints_count from {s['sejm_terms']} order by 1"
            ),
            "proceedings": q(
                con,
                f"select term, count(*), count(*) filter (where sitting_number = '0'), "
                f"count(distinct sitting_number), max(try_cast(sitting_number as int)), "
                f"count(*) filter (where coalesce(dates, '') = ''), min(dates), max(split_part(dates, '|', -1)) "
                f"from {p} group by 1 order by 1",
            ),
            "voting_days": q(
                con,
                f"select term, count(*), count(distinct sitting_number), sum(votings_num::int), min(voting_date), "
                f"max(voting_date) from {vd} group by 1 order by 1",
            ),
            "votings": q(
                con,
                f"select term, count(*), count(distinct entity), min(voted_at), max(voted_at), "
                f"count(*) filter (where voted_at is null), count(*) filter (where try_cast(yes as int) is null), "
                f"count(*) filter (where try_cast(total_voted as int) is null), "
                f"count(*) filter (where try_cast(present as int) is null), "
                f"count(*) filter (where try_cast(against_all as int) is null) from {v} group by 1 order by 1",
            ),
            "kind": q(
                con,
                f"select kind, count(*), sum(try_cast(yes as int)), sum(try_cast(no as int)) from {v} group by 1 order by 2 desc",
            ),
            "majority_type": q(con, f"select majority_type, count(*) from {v} group by 1 order by 2 desc"),
            "yes_no_abstain_sum_ne_total": count(
                con,
                f"select 1 from {v} where kind <> 'ON_LIST' and yes::int + no::int + abstain::int <> total_voted::int",
            ),
            "prints": q(
                con,
                f"select term, count(*), count(distinct print_number), count(*) filter (where delivery_date is null), "
                f"count(*) filter (where document_date is null), min(delivery_date), max(delivery_date), "
                f"count(*) filter (where title is null) from {pr} group by 1 order by 1",
            ),
            "mps": q(
                con,
                f"select count(*), count(distinct mp_id), count(*) filter (where is_active = 'true'), "
                f"count(distinct club), count(distinct district_num), count(*) filter (where district_num is null) "
                f"from {mp}",
            ),
        },
    )
    names = ["sejm_sitting_days", "sejm_votings", "sejm_prints", "sejm_mps"]
    silver = build(con, names, "silver/sejm", {}, s)
    tests = {
        "unique days": "select term, sitting_number, day_date from sejm_sitting_days group by all having count(*) > 1",
        "unique votings": "select term, sitting_number, voting_number from sejm_votings group by all having count(*) > 1",
        "unique prints": "select term, print_number from sejm_prints group by all having count(*) > 1",
        "unique mps": "select term, mp_id from sejm_mps group by all having count(*) > 1",
        "not_null days": "select 1 from sejm_sitting_days where term is null or sitting_number is null or day_date is null "
        "or is_held is null or raw_sha256 is null",
        "not_null votings": "select 1 from sejm_votings where term is null or sitting_number is null or voting_number is null "
        "or voted_at is null or kind is null or majority_type is null or raw_sha256 is null",
        "not_null prints": "select 1 from sejm_prints where term is null or print_number is null or title is null or raw_sha256 is null",
        "not_null mps": "select 1 from sejm_mps where term is null or mp_id is null or is_active is null or district_num is null "
        "or raw_sha256 is null",
        "accepted term": "select 1 from (select term from sejm_sitting_days union all select term from sejm_votings union all "
        "select term from sejm_prints) where term not in (9, 10)",
    }
    yml = Path("warehouse/models/silver/sejm/_sejm.yml").read_text()
    for column in ("kind", "majority_type"):
        found = re.search(rf"name: {column}\n\s+tests: .*values: \[([^\]]*)\]", yml)
        if found and "'" in found.group(1):
            tests[f"accepted {column}"] = f"select 1 from sejm_votings where {column} not in ({found.group(1)})"
    for name in ("sejm_sitting_days_complete", "sejm_votings_complete", "sejm_prints_complete", "sejm_mps_complete"):
        tests[name] = render(f"warehouse/tests/{name}.sql")
    result, samples = failing(con, tests)
    note(
        "sejm trial Silver (local DuckDB over scratch Bronze)",
        {
            "rows_two_builds": silver,
            "tests_failing_rows": result,
            "failing_samples": samples,
            "days": q(
                con,
                "select term, count(*), count(*) filter (where is_held), min(day_date), max(day_date), "
                "count(votings_listed), count(distinct sitting_number) from sejm_sitting_days group by 1 order by 1",
            ),
            "voting_days_not_in_sitting_days": count(
                con,
                f"select 1 from {vd} v anti join sejm_sitting_days d on d.term = v.term::int and "
                f"d.sitting_number = v.sitting_number::int and d.day_date = v.voting_date::date",
            ),
            "votings": q(
                con,
                "select term, count(*), count(*) filter (where is_list_vote), min(voting_date), "
                "max(voting_date) from sejm_votings group by 1 order by 1",
            ),
            "prints": q(
                con,
                "select term, count(*), max(prints_listed), count(*) filter (where delivery_date is null), "
                "min(delivery_date), max(delivery_date) from sejm_prints group by 1 order by 1",
            ),
            "mps_active": q(con, "select count(*), count(*) filter (where is_active) from sejm_mps"),
        },
    )
    refs = {"poc_map_geography": "gmap"}
    gold = build(
        con,
        [
            "parliament_sejm_sitting_days",
            "parliament_sejm_votings",
            "parliament_sejm_prints",
            "parliament_sejm_mps",
            "poc_build_sejm",
        ],
        "gold",
        refs,
        s,
    )
    kpis = {
        "sejm_sitting_days": "sejm:sitting_days:days:monthly",
        "sejm_votings": "sejm:votings:votings:monthly",
        "sejm_prints": "sejm:prints:prints:monthly",
    }
    note(
        "sejm trial Gold build_sejm and entity tables",
        {
            "rows_two_builds": gold,
            **gold_checks(con, "poc_build_sejm", kpis),
            "max_month": q(
                con, "select measure_key, max(value), arg_max(period_label, value) from poc_build_sejm group by 1"
            ),
            "spot_2026": q(
                con,
                "select measure_key, period_label, value, observation_status_key from poc_build_sejm "
                "where period_label in ('2026-09', '2026-10', '2026-11') order by 1, 2",
            ),
            "spot_sitting_68": q(
                con,
                "select count(*), min(voting_date), max(voting_date), max(votings_listed) "
                "from sejm_votings where term = 10 and sitting_number = 68",
            ),
            "spot_voting_10_1_1": q(
                con,
                "select kind, majority_type, yes, no, abstain, total_voted, not_participating "
                "from sejm_votings where term = 10 and sitting_number = 1 and voting_number in (1, 147)",
            ),
        },
    )


def eli_trial(con) -> None:
    from ingestion.to_bronze.eli_pl import load as bronze
    from ingestion.to_landing import eli_pl as landing

    bronze.annotate = collect
    landing_step("eli_pl", landing)
    s = bronze_step("eli_pl", bronze, con)
    a = s["eli_pl_acts"]
    note(
        "eli_pl trial Bronze profile",
        {
            "publishers": q(con, f"select publisher, name, acts_count, years_listed from {s['eli_pl_publishers']}"),
            "acts": q(
                con,
                f"select publisher, count(*), count(distinct eli_id), count(distinct entity), min(year::int), max(year::int), "
                f"count(*) filter (where coalesce(promulgation, '') = ''), "
                f"count(*) filter (where coalesce(announcement_date, '') = ''), "
                f"count(*) filter (where try_cast(position as int) is null), count(*) filter (where act_type is null), "
                f"count(*) filter (where status is null), min(promulgation), max(promulgation) from {a} group by 1",
            ),
            "promulgation_year_ne_year": count(
                con, f"select 1 from {a} where year(try_cast(promulgation as date)) <> year::int"
            ),
            "types": q(con, f"select act_type, count(*) from {a} group by 1 order by 2 desc", 80),
            "statuses": q(con, f"select status, count(*) from {a} group by 1 order by 2 desc", 30),
        },
    )
    silver = build(con, ["eli_pl_acts"], "silver/eli_pl", {}, s)
    tests = {
        "unique eli_id": "select eli_id from eli_pl_acts group by all having count(*) > 1",
        "unique position": "select publisher, year, position from eli_pl_acts group by all having count(*) > 1",
        "not_null": "select 1 from eli_pl_acts where eli_id is null or publisher is null or year is null or position is null "
        "or act_type is null or status is null or raw_sha256 is null",
        "accepted publisher": "select 1 from eli_pl_acts where publisher not in ('DU', 'MP')",
        "eli_pl_acts_complete": render("warehouse/tests/eli_pl_acts_complete.sql"),
    }
    yml = Path("warehouse/models/silver/eli_pl/_eli_pl.yml").read_text()
    for column in ("act_type", "status"):
        found = re.search(rf"name: {column}\n\s+tests: .*values: \[([^\]]*)\]", yml)
        if found and "'" in found.group(1):
            tests[f"accepted {column} (warn)"] = f"select 1 from eli_pl_acts where {column} not in ({found.group(1)})"
    result, samples = failing(con, tests)
    note(
        "eli_pl trial Silver (local DuckDB over scratch Bronze)",
        {
            "rows_two_builds": silver,
            "tests_failing_rows": result,
            "failing_samples": samples,
            "acts": q(
                con,
                "select publisher, count(*), max(acts_listed), min(promulgation_date), max(promulgation_date), "
                "count(*) filter (where promulgation_date is null) from eli_pl_acts group by 1 order by 1",
            ),
        },
    )
    gold = build(con, ["legal_acts", "poc_build_eli_pl"], "gold", {"poc_map_geography": "gmap"}, s)
    note(
        "eli_pl trial Gold build_eli_pl and legal_acts",
        {
            "rows_two_builds": gold,
            **gold_checks(con, "poc_build_eli_pl", {"legal_acts_published": "eli_pl:acts:acts:monthly"}),
            "max_month": q(con, "select max(value), arg_max(period_label, value) from poc_build_eli_pl where is_total"),
            "legal_acts_unique": count(con, "select eli_id from legal_acts group by 1 having count(*) > 1"),
            "spot": q(
                con,
                "select period_label, breakdown_json, value, observation_status_key from poc_build_eli_pl "
                "where period_label in ('2025-12', '2026-10') and (is_total or breakdown_json like '%_T\"}' "
                "or breakdown_json like '%Ustawa%') order by 1, 2",
            ),
            "year_2025": q(con, "select publisher, count(*) from eli_pl_acts where year = 2025 group by 1 order by 1"),
        },
    )


def cleanup() -> None:
    from ingestion.common.r2 import catalog, req, s3

    for scratch in SCRATCH.values():
        removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
        cat = catalog()
        try:
            for table in cat.list_tables(scratch):
                try:
                    cat.purge_table(table)
                except Exception:
                    cat.drop_table(table)
                removed["tables"].append(".".join(table))
            cat.drop_namespace(scratch)
        except Exception as exc:
            removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        client = s3()
        for bucket_var, label in (
            ("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
            ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects"),
        ):
            bucket = req(bucket_var)
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{scratch}/"):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                    removed[label] += len(keys)
        removed["objects_left"] = sum(
            len(p.get("Contents", []))
            for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
            for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{scratch}/")
        )
        note(f"{scratch} trial scratch removed", removed)


def main() -> int:
    status = 0
    con = duckdb.connect()
    note("sejm_eli trial versions", {"duckdb": duckdb.__version__})
    try:
        geo(con)
        for name, step in (("sejm", sejm_trial), ("eli_pl", eli_trial)):
            try:
                step(con)
            except Exception as exc:
                import traceback

                status = 1
                note(
                    f"{name} trial failed",
                    {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1800:]},
                    "error",
                )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
