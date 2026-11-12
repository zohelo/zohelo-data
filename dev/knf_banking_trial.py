"""Dev trial (scratch only) of knf_banking: robots.txt, Landing and Bronze into zz_dev_knf_banking (both buckets and
the catalog namespace), twice each, a Bronze profile of the workbooks, then the Silver and Gold SQL rendered and run in
DuckDB on the scratch Bronze table with every yml test. Everything written is removed at the end (finally). Never on
the build branch.

python -m dev.knf_banking_trial [profile]
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
import re
import sys
import traceback
from urllib import robotparser

import duckdb
import jinja2
import requests
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.knf_banking import load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "knf_banking"
SCRATCH = "zz_dev_knf_banking"
LANDING = f"{SCRATCH}/01_landing"
SILVER_DIR = ROOT / "warehouse/models/silver/knf_banking"
GOLD = ROOT / "warehouse/models/gold/poc_build_knf_banking.sql"
REAL = report.annotate
PAGE = "https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne?articleId=56224&p_id=18"


def quiet(level, title, data):  # activity annotations would crowd the per-step cap: print them instead
    print(f"[{level}] {title}: {str(data)[:1500]}", flush=True)


def show(con, title, sql, limit=400):
    print(f"\n=== {title}", flush=True)
    try:
        rel = con.sql(sql)
        print("\t".join(d[0] for d in rel.description))
        rows = rel.fetchall()
        for r in rows[:limit]:
            print("\t".join("" if v is None else str(v)[:140] for v in r))
        if len(rows) > limit:
            print(f"... {len(rows)} rows")
        return rows
    except Exception as exc:  # noqa: BLE001 - trial: report and go on
        print("   failed:", str(exc)[:800])
        return []


def one(con, sql):
    return con.sql(sql).fetchall()[0][0]


def robots() -> dict:
    url = "https://www.knf.gov.pl/robots.txt"
    r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=30)
    print("\n=== robots.txt", r.status_code, dict(r.headers), "\n", r.text[:4000], flush=True)
    out = {"status": r.status_code, "server": r.headers.get("Server"), "bytes": len(r.content)}
    if r.status_code == 200:
        rp = robotparser.RobotFileParser()
        rp.parse(r.text.splitlines())
        out["allowed"] = {
            "page": rp.can_fetch(bulk_file.USER_AGENT, PAGE),
            "files": rp.can_fetch(bulk_file.USER_AGENT, "https://www.knf.gov.pl/knf/pl/komponenty/img/x.xlsx"),
        }
        out["disallow_lines"] = [ln.strip() for ln in r.text.splitlines() if ln.lower().startswith("disallow")][:15]
    return out


def macros() -> str:
    return "".join(p.read_text() for p in sorted((ROOT / "warehouse/macros").glob("*knf_banking*.sql"))) + (
        ROOT / "warehouse/macros/xlsx_cells.sql"
    ).read_text()


def render(path: Path, refs: dict) -> str:
    text = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(macros() + text).render(
        source=lambda a, b: "cells", ref=lambda n: refs[n], var=lambda n, d=None: d
    )


def yml_tests(con, name: str) -> list:
    spec = next(
        m for f in SILVER_DIR.glob("_*.yml") for m in yaml.safe_load(f.read_text())["models"] if m["name"] == name
    )
    out = []
    for t in spec.get("tests", []):
        if "unique" in t:
            expr = t["unique"]["column_name"]
            out.append(("unique", one(con, f"select count(*) - count(distinct {expr}) from {name}")))
    for col in spec.get("columns", []):
        for t in col.get("tests", []):
            if t == "not_null":
                out.append((f"not_null {col['name']}", one(con, f"select count(*) from {name} where {col['name']} is null")))
            elif t == "unique":
                out.append((f"unique {col['name']}", one(con, f"select count(*) - count(distinct {col['name']}) from {name}")))
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lit = ", ".join(repr(v) if isinstance(v, str) else str(v) for v in vals)
                out.append((
                    f"accepted {col['name']}",
                    one(con, f"select count(*) from {name} where {col['name']} is not null and {col['name']} not in ({lit})"),
                ))
    return out


def profile(con) -> None:
    show(con, "sheets per entity", """
        select entity, sheet_index, sheet_name, count(*) cells, max(row_number) max_row, max(column_number) max_col,
               count(*) filter (where cell_type = 'n') numbers, count(*) filter (where cell_type <> 'n') texts
        from cells group by all order by entity, sheet_index""")
    show(con, "number formats", """
        select entity, number_format, count(*) from cells where cell_type = 'n' group by all order by entity, 3 desc""", 80)
    show(con, "non-numeric strings per entity (top 60)", """
        select entity, trim(value_text) v, count(*) n from cells
        where cell_type <> 'n' and try_cast(replace(trim(value_text), ',', '.') as double) is null
          and length(trim(value_text)) <= 6
        group by all order by entity, n desc""", 120)
    # date header rows: date-formatted numbers per row
    show(con, "rows with >= 3 date cells (header rows)", """
        select entity, sheet_name, row_number, count(*) n, min(column_number) c0, max(column_number) c1,
               min(date '1899-12-30' + cast(round(try_cast(value_text as double)) as integer))::varchar d0,
               max(date '1899-12-30' + cast(round(try_cast(value_text as double)) as integer))::varchar d1,
               any_value(number_format) fmt
        from cells
        where cell_type = 'n' and regexp_matches(lower(coalesce(number_format, '')), 'y|m') and not contains(coalesce(number_format, ''), '#')
          and not contains(coalesce(number_format, ''), '0')
        group by all having count(*) >= 3 order by entity, sheet_name, row_number""", 300)
    show(con, "text header rows (>= 3 text cells from column 2 on, first 14 rows of each sheet)", """
        select entity, sheet_name, row_number, count(*) n,
               string_agg(cell_ref || '=' || left(regexp_replace(value_text, '\\s+', ' ', 'g'), 40), ' | ' order by column_number) cells
        from cells where cell_type <> 'n' and row_number <= 14
        group by all order by entity, sheet_name, row_number""", 600)
    show(con, "column A/B/C labels per sheet (all rows)", """
        with n as (select entity, sheet_name, row_number, count(*) numbers, min(column_number) first_num_col
                   from cells where cell_type = 'n' group by all)
        select c.entity, c.sheet_name, c.row_number,
               string_agg(c.cell_ref || '=' || left(regexp_replace(c.value_text, '\\s+', ' ', 'g'), 70), ' | ' order by c.column_number) labels,
               any_value(n.numbers) numbers, any_value(n.first_num_col) first_num_col
        from cells c left join n using (entity, sheet_name, row_number)
        where c.cell_type <> 'n' and c.column_number <= 3
        group by all order by 1, 2, 3""", 6000)
    show(con, "numeric sample: first data row of each sheet (first 12 cells)", """
        with r as (select entity, sheet_name, min(row_number) rn from cells where cell_type = 'n' and column_number >= 2
                   and not regexp_matches(lower(coalesce(number_format, '')), 'yy') group by all)
        select c.entity, c.sheet_name, c.row_number,
               string_agg(c.cell_ref || '=' || c.value_text || ' [' || coalesce(c.number_format, '') || ']', ' | ' order by c.column_number) sample
        from cells c join r using (entity, sheet_name) where c.row_number = r.rn and c.column_number <= 14
        group by all order by 1, 2""", 200)
    show(con, "texts in rows above the first number of each sheet (titles, units)", """
        with r as (select entity, sheet_name, min(row_number) rn from cells where cell_type = 'n' group by all)
        select c.entity, c.sheet_name, c.cell_ref, left(regexp_replace(c.value_text, '\\s+', ' ', 'g'), 160) t
        from cells c join r using (entity, sheet_name) where c.cell_type <> 'n' and c.row_number < r.rn
          and (regexp_matches(lower(c.value_text), 'mln|tys|zł|%|proc|jednost|dane|stan na') or c.column_number = 1)
        order by 1, 2, c.row_number, c.column_number""", 800)


def main(argv) -> int:
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = quiet
    client, cat = s3(), bf.retry(catalog, "catalog")
    findings: dict = {"head": os.environ.get("GITHUB_SHA", "")[:7]}
    try:
        findings["robots"] = robots()
        REAL("notice", "knf_banking robots.txt", findings["robots"])
        if findings["robots"].get("allowed") and not all(findings["robots"]["allowed"].values()):
            REAL("error", "knf_banking stopped", "robots.txt disallows the route")
            return 1
        landing = {"exit": []}
        for mode in ("inspect", "load", "load", "verify"):
            print(f"\n##### landing {mode}", flush=True)
            landing["exit"].append(bulk_file.main(SOURCE, mode, landing_root=LANDING))
        held = {
            d["entity"]: bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix(SOURCE, d["entity"], LANDING))
            for d in bulk_file.declaration(SOURCE)
        }
        landing["files"] = {e: len(h) for e, h in held.items()}
        landing["bytes"] = {e: sum(r["size_bytes"] for r in h) for e, h in held.items()}
        landing["urls"] = {e: h[-1]["source_url"] for e, h in held.items() if h}
        landing["content_type"] = {e: h[-1]["detail"]["http"]["content_type"] for e, h in held.items() if h}
        rows = []
        for n in (1, 2):
            print(f"\n##### bronze load {n}", flush=True)
            load.load(0, landing_root=LANDING, namespace=SCRATCH)
            rows.append(cat.load_table((SCRATCH, "knf_banking_cells")).scan().to_arrow().num_rows)
        cells = cat.load_table((SCRATCH, "knf_banking_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "knf_banking_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001 - no table when nothing was rejected
            rejected = []
        con = duckdb.connect()
        con.register("cells", cells)
        landing["bronze_rows_load1_load2"] = rows
        landing["rejected"] = [(r["entity"], r["error"][:300]) for r in rejected]
        landing["bronze_per_entity"] = con.sql(
            "select entity, count(distinct raw_sha256), count(*), count(distinct sheet_name) from cells group by 1 order by 1"
        ).fetchall()
        REAL("notice", "knf_banking landing + bronze", landing)
        profile(con)
        sheets = con.sql(
            "select entity, list(sheet_name || ':' || n order by i) from (select entity, sheet_index i, sheet_name, count(*) n from cells group by all) group by 1"
        ).fetchall()
        REAL("notice", "knf_banking sheets", sheets)
        if "profile" in argv or not SILVER_DIR.exists():
            return 0
        refs: dict = {}
        silver = {}
        for path in sorted(SILVER_DIR.glob("*.sql")):
            name = path.stem
            counts = []
            for _ in (1, 2):
                con.execute(f"create or replace table {name} as {render(path, refs)}")
                counts.append(con.sql(f"select count(*), sum(hash(*)) from (select * exclude (ingested_at) from {name})").fetchall()[0])
            refs[name] = name
            results = yml_tests(con, name)
            silver[name] = {
                "rows": counts[0][0],
                "rebuild_identical": counts[0] == counts[1],
                "tests_run": len(results),
                "test_failures": {k: v for k, v in results if v},
            }
        findings["silver"] = silver
        extra = Path(__file__).with_name("knf_banking_trial_checks.sql")
        print("silver", silver, flush=True)
        if GOLD.exists():
            con.execute(
                "create table poc_map_geography as select * from (values ('knf_banking', 'PL', 'PL', true)) "
                "as t (source_system, source_code, geography_key, matched)"
            )
            refs["poc_map_geography"] = "poc_map_geography"
            con.execute(f"create table g as {render(GOLD, refs)}")
            findings["gold"] = {
                "rows": one(con, "select count(*) from g"),
                "per_block": con.sql("select block_id || ':' || frequency, count(*), count(distinct measure_key) from g group by 1 order by 1").fetchall(),
                "dup_series_key_x_date_key": one(con, """select count(*) from (select measure_key || '|' || geography_key ||
                    case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as s,
                    date_key from g group by all having count(*) > 1)"""),
                "rows_geography_unmatched": one(con, "select count(*) from g where geography_key = '_X'"),
                "null_value_or_date": one(con, "select count(*) from g where value is null or date_key is null"),
                "range": one(con, "select min(period_label) || '..' || max(period_label) from g"),
                "status": con.sql("select observation_status_key, count(*) from g group by 1").fetchall(),
            }
        if extra.exists():
            for block in extra.read_text().split(";\n"):
                if block.strip():
                    title = block.strip().splitlines()[0].lstrip("- ")
                    rows_ = show(con, title, block)
                    if title.startswith("ANNOTATE"):
                        REAL("notice", f"knf_banking {title[9:60]}", [list(r) for r in rows_][:60])
        REAL("notice" if not any(v["test_failures"] for v in silver.values()) else "error", "knf_banking silver", silver)
        if "gold" in findings:
            REAL("notice" if findings["gold"]["dup_series_key_x_date_key"] == 0 else "error", "knf_banking gold", findings["gold"])
    except Exception:  # noqa: BLE001
        print("CRASH", traceback.format_exc()[-4000:], flush=True)
        REAL("error", "knf_banking trial crash", traceback.format_exc()[-1500:])
        return 1
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        with contextlib.suppress(Exception):
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        left = sum(
            len(p.get("Contents", []))
            for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
            for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/")
        )
        REAL("notice", "knf_banking trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
