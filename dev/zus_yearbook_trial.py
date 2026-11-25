"""Dev trial of zus_yearbook (PLS-010, #434): Landing and Bronze into zz_dev_zus_yearbook/ (both buckets) and namespace
zz_dev_zus_yearbook, each twice; then the branch's Silver, tests and Gold build SQL applied in memory (Jinja-rendered
with the source's macro file) to the scratch Bronze; gold.dim_geography is read (read-only) for the macro's `geo`.
Annotations: landing+bronze, bronze profile, silver, tests, gold, Poland spot values, licence, cleanup. The library
annotations of the Landing and Bronze code are printed to the log instead (at most 10 annotations per step).
Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2
import requests

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_zus_yearbook"
SOURCE = "zus_yearbook"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/zus_yearbook.sql").read_text()
ANNOTATE = report.annotate
CAPTURED: list[tuple[str, str, object]] = []


def quiet(level, title, data) -> None:
    CAPTURED.append((level, title, data))
    print(f"[{level}] {title}: {data}", flush=True)


report.annotate = bulk_bronze.annotate = bulk_file.annotate = quiet


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=120) -> None:
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:300] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def objects(client, bucket) -> tuple[int, int, int]:
    """(receipts, data files, data bytes) under the scratch Landing prefix."""
    receipts = files = size = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/{SOURCE}/"):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                receipts += 1
            elif "/data/" in o["Key"]:
                files += 1
                size += o["Size"]
    return receipts, files, size


def licence(con) -> dict:
    """What the yearbook page and the editions' front matter say about re-use (one page request)."""
    out: dict = {}
    from ingestion.to_landing import zus_yearbook as landing

    try:
        r = requests.get(landing.PAGE, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
        text = re.sub(r"<[^>]+>", " ", r.text)
        text = re.sub(r"\s+", " ", text)
        hits = [m.group(0) for m in re.finditer(r"(?i).{0,90}(licencj|prawa autorsk|ponowne wykorzyst|copyright|©).{0,90}", text)]
        out["page_status"] = r.status_code
        out["page_terms_snippets"] = hits[:4]
    except requests.RequestException as exc:
        out["page_error"] = str(exc)[:120]
    out["pdf_front_matter"] = con.sql(
        "select string_agg(entity || ' p' || page_number || ': ' || trim(line_text), ' | ') from (select * from bronze_lines "
        "where page_number <= 4 and regexp_matches(lower(line_text), 'prawa|copyright|wykorzyst|cytowa|licenc|©') "
        "order by entity, page_number, line_number limit 8)"
    ).fetchall()[0][0]
    return out


def main(argv) -> int:
    from ingestion.to_bronze.zus_yearbook import load as bronze
    from ingestion.to_landing import zus_yearbook as landing

    client = s3()
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    one = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    try:
        # Landing, twice
        idem: dict[str, object] = {}
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            idem[f"landing_exit_{attempt}"] = landing.extract(60, landing_root=LAND)
            run = [d for lvl, t, d in CAPTURED if t.endswith("landing extract")][-1]
            idem[f"landing_{attempt}"] = {k: run[k] for k in ("editions_listed", "planned", "landed", "unchanged", "refused")}
            idem[f"receipts_files_bytes_{attempt}"] = objects(client, landing_bucket)
        held = sorted(landing.held_entities(LAND))
        idem["inspect_missing"] = len(landing.plan(landing.discover(), set(held))) - 1  # the newest is planned again
        idem["landing_verify_exit"] = bulk_file.main(SOURCE, "verify", landing_root=LAND, specs=[{"entity": e} for e in held])

        # Bronze, twice
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            t0 = time.monotonic()
            bronze.load(60, landing_root=LAND, namespace=SCRATCH)
            run = [d for lvl, t, d in CAPTURED if t.endswith("Bronze from Landing")][-1]
            table = cat.load_table((SCRATCH, "zus_yearbook_lines"))
            idem[f"bronze_{attempt}"] = {
                "appended_files": run["appended_files"],
                "rows_added": run["rows_added"],
                "rejected": run["rejected"],
                "rows": table.scan().to_arrow().num_rows,
                "snapshots": len(table.metadata.snapshots),
                "seconds": round(time.monotonic() - t0),
            }
        lines = cat.load_table((SCRATCH, "zus_yearbook_lines")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "zus_yearbook_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001 - no rejections table
            rejected = []
        idem["rejected_files"] = [(r.get("entity"), (r.get("error") or "")[:100]) for r in rejected]
        idem["editions"] = held
        print("LANDING+BRONZE", idem, flush=True)
        ANNOTATE("notice", "zus_yearbook trial landing+bronze", idem)
        con.register("bronze_lines", lines)
        ANNOTATE("notice", "zus_yearbook trial bronze profile", {
            "rows": lines.num_rows,
            "editions": one("select count(distinct entity) from bronze_lines"),
            "pages": one("select string_agg(entity || ':' || p, ' ' order by entity) from (select entity, max(page_number) p from bronze_lines group by 1)"),
            "versions": one("select string_agg(distinct entity || '@' || source_version, ' ') from bronze_lines"),
            "null_keys": one("select count(*) from bronze_lines where entity is null or page_number is null or line_number is null or line_text is null or raw_sha256 is null"),
            "dup_keys": one("select count(*) from (select 1 from bronze_lines group by entity, page_number, line_number having count(*) > 1)"),
            "files": one("select count(distinct raw_sha256) from bronze_lines"),
        })

        # Silver
        silver_sql = ROOT / "warehouse/models/silver/zus_yearbook/zus_yearbook_tables.sql"
        con.sql("create table silver as " + render(silver_sql, {"zus_yearbook_lines": "bronze_lines"}))
        s = "silver"
        areas = ("polska dolnośląskie kujawsko-pomorskie lubelskie lubuskie łódzkie małopolskie mazowieckie opolskie "
                 "podkarpackie podlaskie pomorskie śląskie świętokrzyskie warmińsko-mazurskie wielkopolskie zachodniopomorskie").split()
        quoted = ", ".join(f"'{a}'" for a in areas)
        ANNOTATE("notice", "zus_yearbook trial silver", {
            "rows": one(f"select count(*) from {s}"),
            "years": one(f"select min(period_year) || '-' || max(period_year) || ' (' || count(distinct period_year) || ')' from {s}"),
            "per_edition": one(f"select string_agg(x, '; ' order by x) from (select edition || ':' || measure_code || ' p' || min(page_number) || ' n=' || count(*) x from {s} group by edition, measure_code)"),
            "dup_grain": one(f"select count(*) from (select 1 from {s} group by measure_code, area_name, period_year having count(*) > 1)"),
            "null_keys": one(f"select count(*) from {s} where edition is null or table_code is null or measure_code is null or unit_code is null or area_name is null or period_year is null or cell_status is null or raw_sha256 is null"),
            "not_accepted": one(f"select count(*) from {s} where area_name not in ({quoted}) or measure_code not in ('payers', 'insured') or table_code not in ('payers_voiv', 'insured_voiv') or cell_status not in ('ok', 'nil', 'missing', 'not_applicable')"),
            "cell_status": one(f"select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from {s} group by 1)"),
            "non_numeric_values": one(f"select coalesce(string_agg(value_text || '=' || c, ' '), 'none') from (select value_text, count(*) c from {s} where not regexp_full_match(value_text, '[0-9]{{1,3}}( [0-9]{{3}})*') group by 1)"),
            "zeros": one(f"select count(*) from {s} where value = 0"),
        })
        show(con, "silver per edition and measure",
             f"select edition, measure_code, count(*) n, count(distinct area_name) areas, min(period_year), max(period_year), min(page_number) page from {s} group by all order by all")
        show(con, "silver all", f"select edition, measure_code, area_name, period_year, value_text, cell_status, page_number, line_number from {s} order by measure_code, area_name, period_year", limit=600)

        # the band of every table found (log only): what the model read, to review the layout of each edition
        dbg = render(silver_sql, {"zus_yearbook_lines": "bronze_lines"})
        dbg = dbg[: dbg.rindex("\nselect\n    'zus_yearbook' as source_system")]
        show(con, "bands", dbg + "\nselect * from bands order by all", limit=60)
        show(con, "regions", dbg + "\nselect edition, measure_code, page_number, line_number, text from region order by all", limit=1500)

        # tests
        tests = {}
        for test in sorted((ROOT / "warehouse/tests").glob("zus_yearbook_*.sql")):
            rows = con.sql(render(test, {"zus_yearbook_tables": s, "zus_yearbook_lines": "bronze_lines", "poc_build_zus_yearbook": "build"})).fetchall()
            tests[test.stem] = {"failing_rows": len(rows), "first": [str(r)[:160] for r in rows[:5]]}
        ANNOTATE("notice" if all(v["failing_rows"] == 0 for v in tests.values()) else "error", "zus_yearbook trial tests", tests)

        # Gold build stage, geography through the source's macro on gold.dim_geography (read-only)
        try:
            con.register("dim_geography", cat.load_table(("gold", "dim_geography")).scan().to_arrow())
        except Exception as exc:  # noqa: BLE001 - fall back to the TERYT list
            print("gold.dim_geography not readable, using silver.teryt_terc:", exc, flush=True)
            con.register("terc", cat.load_table(("silver", "teryt_terc")).scan().to_arrow())
            con.sql("create table dim_geography as select 'TERC:' || terc_code as geography_key, name_pl, unit_level as level_name from terc")
        geo_macro = re.search(r"macro poc_geography__zus_yearbook\(\) %\}(.*?)\{% endmacro", MACROS, re.S).group(1)
        geo_sql = jinja2.Environment().from_string(MACROS + geo_macro).render()
        con.sql(f"create table map_geography as with geo as (select * from dim_geography), {geo_sql} select * from zus_yearbook")
        con.sql("create table build as " + render(ROOT / "warehouse/models/gold/poc_build_zus_yearbook.sql",
                                                   {"poc_map_geography": "map_geography", "zus_yearbook_tables": s}))
        ANNOTATE("notice", "zus_yearbook trial gold", {
            "map_rows": one("select count(*) from map_geography"),
            "gold_rows": one("select count(*) from build"),
            "dup_series_x_date": one("select count(*) from (select 1 from build group by measure_key || '|' || geography_key || '|' || breakdown_json, date_key having count(*) > 1)"),
            "unmatched_geography": one("select count(*) from build where geography_key = '_X'"),
            "geographies": one("select count(distinct geography_key) from build"),
            "null_values": one("select count(*) from build where value is null"),
            "frequencies": one("select string_agg(distinct frequency, ' ') from build"),
            "per_measure": one("select string_agg(x, '; ' order by x) from (select measure_key || ' ' || min(period_label) || '-' || max(period_label) || ' n=' || count(*) x from build group by measure_key)"),
            "max_rows_per_source_area_period": one("select max(c) from (select count(*) c from build group by measure_key, geography_key, date_key)"),
        })
        show(con, "gold Poland", "select measure_codes, period_label, value, unit_code, geography_key from build where geography_key = 'PL' order by 1, 2")
        ANNOTATE("notice", "zus_yearbook trial gold Poland spot values", {
            m: one(f"select string_agg(period_label || '=' || cast(value as bigint), ' ' order by period_label) from build where geography_key = 'PL' and measure_codes = '{m}'")
            for m in ("payers", "insured")
        })
        lic = licence(con)
        print("LICENCE", lic, flush=True)
        ANNOTATE("notice", "zus_yearbook trial licence", lic)
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:  # noqa: BLE001
            print("drop failed", exc)
        ANNOTATE("notice", "zus_yearbook trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
