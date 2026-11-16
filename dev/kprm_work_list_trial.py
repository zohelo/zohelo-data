"""Dev trial for #491 (KPRM work list of the Council of Ministers, PLG-034; test branch only, never on main).

Landing -> Bronze -> Silver -> Gold into scratch, each step twice. Landing through the shared bulk_file pattern into
the prefix zz_dev_kprm/ (pass 2 must be `unchanged`), then landing inspect and verify. The landed file is profiled
with DuckDB (header, columns, closed lists, shapes; never the official's name column or long texts). Bronze into the
Iceberg namespace zz_dev_kprm and lake prefix zz_dev_kprm/ (twice: the second appends nothing). Silver, the Gold
entity table and the build stage are the dbt SQL rendered with Jinja and run in local DuckDB over the scratch Bronze
table, each built twice, with the model tests. Everything written is removed in `finally`.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import tempfile

import duckdb
from jinja2 import Environment
import yaml

from ingestion.common import report as report_module

SOURCE = "kprm_work_list"
SCRATCH = "zz_dev_kprm"
MACROS = "".join(p.read_text() for p in sorted(Path("warehouse/macros").glob("*.sql")))
MACROS += Path(f"warehouse/macros/sources/{SOURCE}.sql").read_text()
COLLECTED: dict[str, list] = {}
ANNOTATE = report_module.annotate


def collect(level: str, title: str, data) -> None:
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


def note(title: str, data, level: str = "notice", quiet: bool = False) -> None:
    print(f"FULL {title}: {json.dumps(data, default=str, ensure_ascii=False)}", flush=True)
    quiet = quiet or title.startswith(("kprm trial Bronze profile", "kprm trial Bronze cross", "kprm trial Bronze shapes"))
    if not quiet:  # profiles go to the job log only (at most 10 annotations per step)
        ANNOTATE(level, title, data)


def q(con, sql: str, limit: int = 80) -> list:
    return [[v if isinstance(v, int | float) or v is None else str(v) for v in r] for r in con.execute(sql).fetchall()][
        :limit
    ]


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


def shape(column: str) -> str:
    return f"left(regexp_replace(regexp_replace(coalesce({column}, '<null>'), '[0-9]', 'N', 'g'), '\\p{{L}}+', 'L', 'g'), 50)"


def profile_raw(body: bytes) -> None:
    """Header and per-column profile of the landed file, by DuckDB's own reading of the header."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "f.csv"
        path.write_bytes(body)
        con = duckdb.connect()
        con.execute(
            f"create table t as select * from read_csv('{path}', delim=';', quote='\"', escape='\"', header=true, "
            "all_varchar=true, strict_mode=true, null_padding=false)"
        )
        cols = [r[0] for r in con.execute("describe t").fetchall()]
        prof = []
        for i, c in enumerate(cols):
            qc = f'"{c}"'
            nn, nd, ml = con.execute(
                f"select count({qc}) filter (where trim({qc}) <> ''), count(distinct {qc}), max(length({qc})) from t"
            ).fetchone()
            prof.append([i, c, nn, nd, ml])
        note(
            "kprm trial raw file",
            {
                "bytes": len(body),
                "bom": body[:3] == b"\xef\xbb\xbf",
                "crlf": b"\r\n" in body[:20000],
                "rows": con.execute("select count(*) from t").fetchone()[0],
                "profile": prof,
            },
        )


def trial() -> int:
    from ingestion.common import bulk_bronze
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.kprm_work_list import load as bronze
    from ingestion.to_landing.patterns import bulk_file

    for module in (bulk_file, bulk_bronze):
        module.annotate = collect
        module.step_summary = lambda _m: None
    # --- Landing ---------------------------------------------------------------------------------------------
    exits = [bulk_file.main(SOURCE, "load", landing_root=SCRATCH) for _ in (1, 2)]
    loads = [r["files"] for r in COLLECTED.get(f"{SOURCE} landing load", [])]
    inspect_exit = bulk_file.main(SOURCE, "inspect", landing_root=SCRATCH)
    verify_exit = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, "work_list", SCRATCH))
    note(
        "kprm trial Landing (scratch zz_dev_kprm)",
        {
            "exits_load_load_inspect_verify": [*exits, inspect_exit, verify_exit],
            "loads": loads,
            "inspect": [r["files"] for r in COLLECTED.get(f"{SOURCE} landing inspect", [])],
            "verify": [r["files"] for r in COLLECTED.get(f"{SOURCE} landing verify", [])],
            "receipts": [
                {k: r.get(k) for k in ("version_token", "version_basis", "source_url", "final_url", "sha256", "size_bytes")}
                for r in held
            ],
            "http": held[-1]["detail"].get("http") if held else None,
        },
    )
    profile_raw(bulk_bronze.read_raw(client, bucket, SOURCE, held[-1]))
    # --- Bronze ----------------------------------------------------------------------------------------------
    bexits = [bronze.load(30, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    cat = catalog()
    try:
        rejected = cat.load_table((SCRATCH, bronze.REJECTED_ID[1])).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    con = duckdb.connect()
    b = "b_drafts"
    con.register(b, cat.load_table((SCRATCH, bronze.TABLE_ID[1])).scan().to_arrow())
    note(
        "kprm trial Bronze (scratch)",
        {
            "exits": bexits,
            "loads": COLLECTED.get(f"{SOURCE} Bronze from Landing"),
            "rows": count(con, f"select * from {b}"),
            "files": count(con, f"select distinct raw_sha256 from {b}"),
            "rejected_count": len(rejected),
            "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3],
        },
    )
    cols = [c for c, _ in bronze.COLUMNS if c not in ("entity", "raw_sha256", "source_version", "ingested_at")]
    note(
        "kprm trial Bronze profile",
        {
            "keys": q(
                con,
                f"select count(*), count(distinct draft_number), count(*) filter (where nullif(trim(draft_number), '') "
                f"is null), min(published_at), max(published_at) from {b}",
            ),
            "non_empty": {c: count(con, f"select 1 from {b} where nullif(trim({c}), '') is not null") for c in cols},
            "distinct": {c: count(con, f"select distinct {c} from {b}") for c in cols},
            "max_length": {c: con.execute(f"select max(length({c})) from {b}").fetchone()[0] for c in cols},
        },
    )
    closed = {
        c: q(con, f"select {c}, count(*) from {b} group by 1 order by 2 desc")
        for c in ("document_kind", "document_type", "status_text", "empty_1", "empty_2", "empty_3")
    }
    note("kprm trial Bronze closed lists", closed, quiet=True)
    for c in ("responsible_body", "drafting_body"):
        note(f"kprm trial Bronze {c}", {c: q(con, f"select {c}, count(*) from {b} group by 1 order by 2 desc")}, quiet=True)
    note(
        "kprm trial Bronze shapes",
        {
            c: q(con, f"select {shape(c)}, count(*), min({c}), max({c}) from {b} group by 1 order by 2 desc", 15)
            if c in ("draft_number", "published_at", "planned_adoption")
            else q(con, f"select {shape(c)}, count(*) from {b} group by 1 order by 2 desc", 8)
            for c in ("draft_number", "published_at", "planned_adoption", "preview_url", "withdrawal_info",
                      "additional_info", "cooperating_body")
        },
    )
    note(
        "kprm trial Bronze cross",
        {
            "prefix_kind": q(
                con,
                f"select regexp_extract(draft_number, '^[^0-9]+'), document_kind, count(*) from {b} group by 1, 2 "
                "order by 3 desc",
            ),
            "status_withdrawal": q(
                con,
                f"select status_text, withdrawal_info is not null, count(*) from {b} group by 1, 2 order by 3 desc",
            ),
            "body_differs": count(con, f"select 1 from {b} where responsible_body is distinct from drafting_body"),
            "per_month": q(con, f"select left(published_at, 7), count(*) from {b} group by 1 order by 1"),
        },
    )
    # --- Silver ----------------------------------------------------------------------------------------------
    sources = {"kprm_work_list_drafts": b}
    sql = render("warehouse/models/silver/kprm_work_list/kprm_work_list_drafts.sql", {}, sources)
    silver_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table kprm_work_list_drafts as {sql}")
        silver_rows.append(count(con, "select * from kprm_work_list_drafts"))
    spec = yaml.safe_load(Path("warehouse/models/silver/kprm_work_list/_kprm_work_list.yml").read_text())
    tests = {
        "unique draft_number": "select draft_number from kprm_work_list_drafts group by 1 having count(*) > 1",
        "complete": render("warehouse/tests/kprm_work_list_drafts_complete.sql"),
    }
    for column in spec["models"][0]["columns"]:
        for test in column.get("tests", []):
            if test == "not_null":
                tests[f"not_null {column['name']}"] = (
                    f"select 1 from kprm_work_list_drafts where {column['name']} is null"
                )
            elif isinstance(test, dict) and "accepted_values" in test:
                values = test["accepted_values"]["arguments"]["values"]
                listed = ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)
                severity = (test["accepted_values"].get("config") or {}).get("severity", "error")
                tests[f"accepted_values {column['name']} ({severity})"] = (
                    f"select {column['name']}, count(*) from kprm_work_list_drafts where {column['name']} "
                    f"not in ({listed}) group by 1"
                )
    failing = {k: count(con, s) for k, s in tests.items()}
    note(
        "kprm trial Silver (local DuckDB over scratch Bronze)",
        {
            "rows_two_builds": silver_rows,
            "tests_failing_rows": failing,
            "failing_samples": {k: q(con, tests[k], 8) for k, c in failing.items() if c},
            "range": q(
                con,
                "select count(*), min(published_at), max(published_at), count(*) filter (where status_key = "
                "'in_progress'), count(*) filter (where status_key = 'realised'), count(*) filter (where status_key = "
                "'withdrawn'), count(planned_adoption_text), count(responsible_body) from kprm_work_list_drafts",
            ),
        },
    )
    # --- Gold ------------------------------------------------------------------------------------------------
    con.execute("create or replace table geo as select 'PL' as iso2, 'PL' as geography_key, 'country' as level_name")
    con.execute(
        f"create or replace view gmap as with {render('{{ poc_geography__kprm_work_list() }}')} "
        "select * from kprm_work_list"
    )
    refs = {"poc_map_geography": "gmap"}
    gold = {}
    for name in ("government_drafts", "poc_build_kprm_work_list"):
        gsql = render(f"warehouse/models/gold/{name}.sql", refs)
        gold[name] = []
        for _ in (1, 2):
            con.execute(f"create or replace table {name} as {gsql}")
            gold[name].append(count(con, f"select * from {name}"))
    p = "poc_build_kprm_work_list"
    note(
        "kprm trial Gold build_kprm_work_list and government_drafts",
        {
            "rows_two_builds": gold,
            "dup_series_date": count(
                con,
                f"select measure_key || '|' || geography_key || '|' || breakdown_json, date_key from {p} "
                "group by all having count(*) > 1",
            ),
            "unmatched_geography": count(con, f"select 1 from {p} where geography_key = '_X'"),
            "null_or_nan": count(
                con, f"select 1 from {p} where value is null or isnan(value) or date_key is null or value < 1"
            ),
            "measures": q(
                con,
                f"select measure_key, frequency, count(*), count(distinct breakdown_json), min(date_key), "
                f"max(date_key), max(value), count(*) filter (where observation_status_key = 'provisional') "
                f"from {p} group by 1, 2 order by 1",
            ),
            "sums_match_total": q(
                con,
                f"select measure_key, sum(value) from {p} where frequency = 'A' group by 1 order by 1",
            ),
            "entity_unique": count(con, "select draft_number from government_drafts group by 1 having count(*) > 1"),
            "spot_2026": q(
                con,
                f"select measure_key, period_label, value, observation_status_key from {p} where breakdown_json = "
                "'{}' or measure_key like '%:kind:%' order by 1, 2 desc",
                30,
            ),
        },
    )
    return 0


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
    note("kprm trial scratch removed", removed)


def main() -> int:
    status = 1
    try:
        status = trial()
    except Exception as exc:
        import traceback

        note(
            "kprm trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1800:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
