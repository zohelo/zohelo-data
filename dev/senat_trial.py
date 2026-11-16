"""Dev trial for #482 (Senate, PLG-025; test branch only, never on main).

Landing -> Bronze -> Silver -> Gold into scratch, each step twice. Landing into the prefix zz_dev_senat/: argument `all`
(default) crawls the whole of term XI as production does, in passes of the production budget (2,000 requests or 75
minutes each, one request at a time, 1.1 s apart) until no vote page is missing (at most MAX_PASSES), which also
proves the resume; a number N instead lands only the N newest vote pages. Then one more full extract pass must land
nothing (the list pages, the senators list and the re-read days unchanged). Then landing inspect and verify.
Bronze into the Iceberg namespace zz_dev_senat and lake prefix zz_dev_senat/ (twice: the second appends nothing).
Silver, the Gold entity tables and the build stage are the dbt SQL rendered with Jinja and run in local DuckDB over
the scratch Bronze tables, each built twice, with the model tests and the completeness tests. Everything written is
removed in `finally`; never writes a production namespace or prefix.
"""

from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
import re
import sys
import time

import duckdb
from jinja2 import Environment

from ingestion.common import report as report_module

SCRATCH = "zz_dev_senat"
ARG = sys.argv[1] if len(sys.argv) > 1 else "all"
VOTES = None if ARG == "all" else int(ARG)
MAX_PASSES = 4
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
MACROS = "".join(p.read_text() for p in sorted(Path("warehouse/macros").glob("*.sql")))
MACROS += Path("warehouse/macros/sources/senat.sql").read_text()
COLLECTED: dict[str, list] = {}


def collect(level: str, title: str, data) -> None:
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


ANNOTATE = report_module.annotate
report_module.annotate = collect
report_module.step_summary = lambda _m: None


def note(title: str, data, level: str = "notice") -> None:
    full = json.dumps(data, default=str)
    print(f"FULL {title}: {full}", flush=True)  # the annotation keeps about 4 KB; the log and the artifact keep all
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / "senat_trial.jsonl").open("a") as handle:
        handle.write(json.dumps({"title": title, "level": level, "data": data}, default=str) + "\n")
    ANNOTATE(level, title, data)


def q(con, sql: str, limit: int = 60) -> list:
    return [[v if isinstance(v, int | float) or v is None else str(v) for v in r] for r in con.execute(sql).fetchall()][
        :limit
    ]


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


def count(con, sql: str) -> int:
    return con.execute(f"select count(*) from ({sql})").fetchone()[0]


def trial() -> int:
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.senat import load as bronze
    from ingestion.to_landing import senat as landing

    bronze.annotate = collect
    # --- Landing ---------------------------------------------------------------------------------------------
    t0 = time.monotonic()
    passes, timing = [], []
    for _ in range(MAX_PASSES if VOTES is None else 1):
        started = time.monotonic()
        passes.append(landing.run("extract", SCRATCH, max_new_votes=VOTES))
        timing.append(round((time.monotonic() - started) / 60, 1))
        last = passes[-1]
        if last["exit"] or (not last.get("stopped") and last.get("votes_missing_after") == 0):
            break
    if passes[-1]["exit"]:
        raise RuntimeError(f"Landing stopped: {passes[-1].get('stopped')}")
    started = time.monotonic()
    again = landing.run("extract", SCRATCH, max_new_votes=None if VOTES is None else 0)
    timing.append(round((time.monotonic() - started) / 60, 1))
    runs = [passes[0], again]
    landed_again = sum(n for kind in again["outcomes"].values() for k, n in kind.items() if k != "unchanged")
    minutes = round((time.monotonic() - t0) / 60, 1)
    inspect = landing.run("inspect", SCRATCH, max_requests=40)
    verify_exit = landing.verify(SCRATCH)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landing.held_receipts(client, bucket, SCRATCH)
    receipts = bronze.newest_receipts(client, bucket, SCRATCH)
    by_kind = Counter(bronze.kind_of(r["entity"]) for r in receipts)
    size = {k: sum(r["size_bytes"] for r in receipts if bronze.kind_of(r["entity"]) == k) for k in by_kind}
    note(
        "senat trial Landing (scratch zz_dev_senat)",
        {
            "pass1": {
                k: runs[0].get(k)
                for k in (
                    "requests",
                    "outcomes",
                    "sittings",
                    "sittings_finished",
                    "sitting_numbers",
                    "days_listed",
                    "days_missing",
                    "votes_known",
                    "votes_missing_before",
                    "votes_missing_after",
                    "stopped",
                    "exit",
                )
            },
            "resume_passes": [
                {k: r.get(k) for k in ("requests", "outcomes", "votes_missing_after", "stopped", "exit")}
                for r in passes[1:]
            ],
            "idempotency_pass": {
                k: again.get(k) for k in ("requests", "outcomes", "votes_missing_after", "stopped", "exit")
            },
            "landed_by_idempotency_pass": landed_again,
            "minutes_per_pass": timing,
            "minutes_all": minutes,
            "vote_pages_limit": VOTES or "all",
            "inspect": {
                k: inspect.get(k)
                for k in (
                    "requests",
                    "sittings",
                    "days_listed",
                    "days_missing",
                    "votes_known",
                    "votes_missing_before",
                    "exit",
                )
            },
            "verify_exit": verify_exit,
            "verify": COLLECTED.get("senat landing verify"),
            "entities": len(held),
            "files_by_kind": dict(by_kind),
            "bytes_by_kind": size,
        },
    )
    # --- Bronze ----------------------------------------------------------------------------------------------
    t0 = time.monotonic()
    exits = [bronze.load(30, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    cat = catalog()
    con = duckdb.connect()
    sources = {}
    for spec in bronze.TABLES.values():
        con.register(f"b_{spec['table']}", cat.load_table((SCRATCH, spec["table"])).scan().to_arrow())
        sources[spec["table"]] = f"b_{spec['table']}"
    try:
        rejected = cat.load_table((SCRATCH, bronze.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    note(
        "senat trial Bronze (scratch)",
        {
            "exits": exits,
            "minutes": round((time.monotonic() - t0) / 60, 1),
            "loads": COLLECTED.get("senat Bronze from Landing"),
            "rows": {t: count(con, f"select * from {v}") for t, v in sources.items()},
            "files": {t: count(con, f"select distinct raw_sha256, entity from {v}") for t, v in sources.items()},
            "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:5],
            "rejected_count": len(rejected),
        },
    )
    s, d, v, n = (sources[t] for t in ("senat_sittings", "senat_day_votes", "senat_vote_results", "senat_senators"))
    note(
        "senat trial Bronze profile",
        {
            "sittings": q(
                con,
                f"select count(*), count(distinct sitting_id), min(sitting_number::int), "
                f"max(sitting_number::int), count(distinct entity) from {s}",
            ),
            "status_text": q(con, f"select status_text, count(*) from {s} group by 1 order by 2 desc"),
            "date_text_shapes": q(
                con,
                f"select regexp_replace(regexp_replace(date_text, '\\d+', 'N', 'g'), "
                f"'\\p{{L}}+', 'M', 'g') as shape, count(*), min(date_text) from {s} "
                f"group by 1 order by 2 desc",
            ),
            "day_votes": q(
                con,
                f"select count(*), count(vote_id), count(distinct vote_id), count(distinct entity), "
                f"count(*) filter (where vote_id is null), count(*) filter (where subject is null "
                f"and vote_id is not null), count(subtitle) from {d}",
            ),
            "day_status": q(con, f"select status_text, count(distinct entity) from {d} group by 1"),
            "results": q(
                con,
                f"select count(*), count(distinct vote_id), "
                f"count(*) filter (where try_cast(voted as int) is null), "
                f"count(*) filter (where try_cast(votes_for as int) is null), "
                f"count(*) filter (where try_cast(abstained as int) is null), "
                f"min(senators_listed), max(senators_listed) from {v}",
            ),
            "non_numeric_totals": q(
                con,
                f"select voted, votes_for, votes_against, abstained from {v} where "
                f"try_cast(voted as int) is null or try_cast(votes_for as int) is null or "
                f"try_cast(votes_against as int) is null or try_cast(abstained as int) "
                f"is null limit 5",
            ),
            "marks": q(
                con,
                f"select split_part(m, '=', 1), count(*), sum(split_part(m, '=', 2)::int) from {v}, "
                f"unnest(string_split(mark_counts, '|')) as t (m) group by 1 order by 2 desc",
            ),
            "sum_ne_voted": q(
                con,
                f"select count(*) from {v} where votes_for::int + votes_against::int + abstained::int <> voted::int",
            ),
            "for_ne_za_marks": q(
                con,
                f"select count(*) from {v} where votes_for <> coalesce(regexp_extract("
                f"mark_counts, '(?:^|\\|)za=(\\d+)', 1), '0')",
            ),
            "title_ne_subject": q(
                con,
                f"select count(*) from {v} r join {d} l using (vote_id) where r.title <> "
                f"l.subject or coalesce(r.subtitle, '') <> coalesce(l.subtitle, '')",
            ),
            "senators": q(con, f"select count(*), count(distinct senator_id), count(annotation) from {n}"),
        },
    )
    # --- Silver ----------------------------------------------------------------------------------------------
    silver = {}
    for name in ("senat_sitting_days", "senat_votes", "senat_senators"):
        sql = render(f"warehouse/models/silver/senat/{name}.sql", {}, sources)
        silver[name] = []
        for _ in (1, 2):
            con.execute(f"create or replace table {name} as {sql}")
            silver[name].append(count(con, f"select * from {name}"))
    tests = {
        "unique days": "select sitting_id, day_number from senat_sitting_days group by all having count(*) > 1",
        "unique votes": "select vote_id from senat_votes group by all having count(*) > 1",
        "unique vote numbers": "select sitting_id, vote_number from senat_votes group by all having count(*) > 1",
        "unique senators": "select senator_id from senat_senators group by all having count(*) > 1",
        "not_null days": "select 1 from senat_sitting_days where sitting_id is null or sitting_number is null or "
        "day_number is null or day_date is null or status_text is null or raw_sha256 is null",
        "not_null votes": "select 1 from senat_votes where vote_id is null or sitting_id is null or day_number "
        "is null or day_date is null or vote_number is null or subject is null or raw_sha256 is null",
        "not_null senators": "select 1 from senat_senators where senator_id is null or term is null or name is null "
        "or raw_sha256 is null",
        "accepted status": "select 1 from senat_sitting_days where status_text not in ('Zakończone')",
        "accepted term": "select 1 from senat_senators where term <> 11",
    }
    for name in ("senat_sitting_days_complete", "senat_votes_complete", "senat_senators_complete"):
        tests[name] = render(f"warehouse/tests/{name}.sql")
    failing = {k: count(con, sql) for k, sql in tests.items()}
    samples = {k: q(con, tests[k], 5) for k, c in failing.items() if c}
    note(
        "senat trial Silver (local DuckDB over scratch Bronze)",
        {
            "rows_two_builds": silver,
            "tests_failing_rows": failing,
            "failing_samples": samples,
            "days": q(
                con,
                "select count(*), count(distinct sitting_id), min(day_date), max(day_date), "
                "count(*) filter (where is_finished), count(*) filter (where day_page_landed), "
                "sum(votes_listed) from senat_sitting_days",
            ),
            "votes": q(
                con,
                "select count(*), min(day_date), max(day_date), count(*) filter (where results_landed), "
                "min(vote_id), max(vote_id) from senat_votes",
            ),
            "senators": q(
                con, "select count(*), count(*) filter (where in_office), count(mandate_ended_on) from senat_senators"
            ),
            "votes_without_results": q(
                con,
                "select sitting_number, count(*) from senat_votes where not results_landed group by 1 order by 1",
                20,
            ),
            "votes_null_date": count(con, "select 1 from senat_votes where day_date is null"),
            "status_on_days": q(con, "select status_text, count(*) from senat_sitting_days group by 1"),
            "totals_profile": q(
                con,
                "select min(voted), max(voted), min(votes_for), max(votes_for), max(votes_against), "
                "max(abstained), min(senators_listed), max(senators_listed), "
                "count(*) filter (where voted = 0) from senat_votes where results_landed",
            ),
            "days_per_sitting": q(
                con,
                "select n, count(*) from (select sitting_id, count(*) n from "
                "senat_sitting_days group by 1) group by 1 order by 1",
            ),
        },
    )
    # --- Gold ------------------------------------------------------------------------------------------------
    con.execute("create or replace table geo as select 'PL' as iso2, 'PL' as geography_key, 'country' as level_name")
    con.execute(f"create or replace view gmap as with {render('{{ poc_geography__senat() }}')} select * from senat")
    refs = {"poc_map_geography": "gmap"}
    gold = {}
    for name in ("senate_sitting_days", "senate_votes", "poc_build_senat"):
        sql = render(f"warehouse/models/gold/{name}.sql", refs)
        gold[name] = []
        for _ in (1, 2):
            con.execute(f"create or replace table {name} as {sql}")
            gold[name].append(count(con, f"select * from {name}"))
    dups = count(
        con,
        "select measure_key || '|' || geography_key || '|' || breakdown_json, "
        "date_key from poc_build_senat group by all having count(*) > 1",
    )
    bounds = re.findall(
        r"\('(senate_\w+)', 'senat', '%', ([\d.e]+), ([\d.e]+)\)",
        Path("warehouse/tests/metrics_value_range.sql").read_text(),
    )
    kpi = {"senate_sitting_days": "senat:sitting_days:days:monthly", "senate_votes": "senat:votes:votes:monthly"}
    out_of_bounds = {
        k: count(
            con, f"select 1 from poc_build_senat where measure_key = '{kpi[k]}' and (value < {lo} or value > {hi})"
        )
        for k, lo, hi in bounds
    }
    note(
        "senat trial Gold build_senat and entity tables",
        {
            "rows_two_builds": gold,
            "dup_series_date": dups,
            "unmatched_geography": count(con, "select 1 from poc_build_senat where geography_key = '_X'"),
            "null_or_nan": count(
                con, "select 1 from poc_build_senat where value is null or isnan(value) or date_key is null"
            ),
            "build_check": q(
                con, "select block_id, count(*), count(distinct measure_key) from poc_build_senat group by 1 order by 1"
            ),
            "measures": q(
                con,
                "select measure_key, frequency, count(*), min(date_key), max(date_key), "
                "max(value), count(*) filter (where observation_status_key = 'provisional') "
                "from poc_build_senat group by 1, 2 order by 1",
            ),
            "value_bounds": bounds,
            "rows_out_of_bounds": out_of_bounds,
            "entity_unique": [
                count(con, "select sitting_id, day_number from senate_sitting_days group by all having count(*) > 1"),
                count(con, "select vote_id from senate_votes group by all having count(*) > 1"),
            ],
            "spot_2026": q(
                con,
                "select measure_key, period_label, value, observation_status_key from "
                "poc_build_senat where period_label in ('2026-10', '2026-11', '2026') order by 1, 2",
            ),
            "spot_sitting_68": q(
                con,
                "select day_number, day_date, votes_listed from senat_sitting_days where "
                "sitting_number = 68 order by 1",
            ),
            "spot_vote_14586": q(
                con,
                "select vote_id, sitting_number, day_date, vote_number, voted, votes_for, "
                "votes_against, abstained from senat_votes where vote_id = 14586",
            ),
        },
    )
    con.execute(
        "create or replace table seed_source as select * from read_csv('warehouse/seeds/source.csv', all_varchar=true)"
    )
    con.execute(
        "create or replace table seed_licence as select * from read_csv('warehouse/seeds/licence.csv', all_varchar=true)"
    )
    note(
        "senat trial licence and Gold maxima",
        {
            "catalogue_row": q(
                con,
                "select s.*, l.name, l.attribution_text, l.commercial_ok, l.share_alike from seed_source as s "
                "left join seed_licence as l using (licence_key) where s.source_key = 'senat'",
            ),
            "max_per_measure": q(
                con,
                "select measure_key, max(value), arg_max(period_label, value) from poc_build_senat group by 1 "
                "order by 1",
            ),
            "spot_2026_05": q(
                con,
                "select measure_key, value, observation_status_key from poc_build_senat "
                "where period_label = '2026-05' order by 1",
            ),
        },
    )
    problems = {
        "landed_by_idempotency_pass": landed_again,
        "rejected_files": len(rejected),
        "bronze_second_load_candidates": (COLLECTED.get("senat Bronze from Landing") or [{}, {}])[-1].get("candidates"),
        "silver_failing_tests": {k: c for k, c in failing.items() if c and k != "accepted status"},
        "silver_rebuild_differs": [k for k, v in silver.items() if len(set(v)) > 1],
        "gold_rebuild_differs": [k for k, v in gold.items() if len(set(v)) > 1],
        "dup_series_date": dups,
        "rows_out_of_bounds": {k: c for k, c in out_of_bounds.items() if c},
    }
    bad = {k: v for k, v in problems.items() if v}
    note("senat trial verdict", {"problems": bad} if bad else {"result": "all checks passed"}, "error" if bad else "notice")
    return 1 if bad else 0


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
    note("senat trial scratch removed", removed)


def main() -> int:
    status = 1
    try:
        status = trial()
    except Exception as exc:
        import traceback

        note(
            "senat trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1800:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
