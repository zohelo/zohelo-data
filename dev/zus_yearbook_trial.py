"""Dev trial of zus_yearbook (PLS-010): Landing and Bronze into zz_dev_zus_yearbook/ (both buckets) and namespace
zz_dev_zus_yearbook, each twice; the Bronze lines of every edition written to $DEV_OUTPUT_DIR (run artifact); then,
when the branch has them, the Silver and Gold SQL applied in memory (Jinja-rendered), checks printed and annotated.
Args: `dane` also searches dane.gov.pl. Everything written to R2 is removed at the end (finally). Deleted before
merge."""

from __future__ import annotations

import os
from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2
import requests

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_zus_yearbook"
SOURCE = "zus_yearbook"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def dane() -> None:
    for url in (
        "https://api.dane.gov.pl/1.4/search?q=rocznik%20statystyczny%20ubezpiecze%C5%84%20spo%C5%82ecznych&per_page=20",
        "https://api.dane.gov.pl/1.4/datasets?q=Zak%C5%82ad%20Ubezpiecze%C5%84%20Spo%C5%82ecznych&per_page=50",
    ):
        try:
            r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=(90, 120))
            data = r.json()
            print("dane", r.status_code, url, "count", data.get("meta", {}).get("count"), flush=True)
            for item in data.get("data", []):
                a = item.get("attributes", {})
                print("  ", item.get("type"), item.get("id"), (a.get("title") or "")[:120], flush=True)
        except Exception as exc:  # noqa: BLE001 - probe continues
            print("dane error", url, type(exc).__name__, str(exc)[:160], flush=True)
        time.sleep(3)


def render(path: Path, tables: dict[str, str]) -> str:
    macros = "".join(p.read_text() for p in sorted((ROOT / "warehouse/macros/sources").glob(f"{SOURCE}*.sql")))
    text = macros + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=150):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:110] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def main(argv) -> int:
    from ingestion.to_bronze.zus_yearbook import load as bronze
    from ingestion.to_landing import zus_yearbook as landing

    if "dane" in argv:
        dane()
    out = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
    out.mkdir(parents=True, exist_ok=True)
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    try:
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            landing.extract(60, landing_root=land)
        held = sorted(landing.held_entities(land))
        sizes = {}
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=req("STORAGE_LANDING_BUCKET_NAME"), Prefix=f"{land}/{SOURCE}/"
        ):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith(".receipt.json"):
                    sizes[obj["Key"].split("/")[-2]] = obj["Size"]
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            t0 = time.monotonic()
            bronze.load(60, landing_root=land, namespace=SCRATCH)
            print("bronze seconds", round(time.monotonic() - t0), flush=True)
        lines = cat.load_table((SCRATCH, "zus_yearbook_lines")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "zus_yearbook_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001 - no rejections table yet
            rejected = []
        con.register("zus_yearbook_lines", lines)
        n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
        annotate("notice", "zus_yearbook trial landing+bronze", {
            "editions": held, "bytes": sizes, "bronze_rows": lines.num_rows,
            "files_in_bronze": n("select count(distinct raw_sha256) from zus_yearbook_lines"),
            "pages": n("select string_agg(entity || ':' || p, ' ' order by entity) from (select entity, max(page_number) p from zus_yearbook_lines group by 1)"),
            "rejected": [(r.get("entity"), (r.get("error") or "")[:100]) for r in rejected],
        })
        for entity in held:
            rows = con.sql(f"select page_number, line_text from zus_yearbook_lines where entity = '{entity}' order by page_number, line_number").fetchall()
            with open(out / f"{entity}.txt", "w", encoding="utf-8") as f:
                last = None
                for page_number, text in rows:
                    if page_number != last:
                        f.write(f"\n=================== PAGE {page_number}\n")
                        last = page_number
                    f.write(text + "\n")
        silver = ROOT / "warehouse/models/silver/zus_yearbook/zus_yearbook_tables.sql"
        if silver.exists():
            con.sql("create or replace table zus_yearbook_tables as " + render(silver, {"zus_yearbook_lines": "zus_yearbook_lines"}))
            s = "zus_yearbook_tables"
            stats = {
                "silver_rows": n(f"select count(*) from {s}"),
                "silver_dup": n(f"select count(*) from (select 1 from {s} group by table_code, measure_code, period_year, edition having count(*) > 1)"),
                "silver_null_keys": n(f"select count(*) from {s} where table_code is null or measure_code is null or period_year is null or raw_sha256 is null"),
                "per_measure": n(f"select string_agg(x, '; ' order by x) from (select table_code || '/' || measure_code || ' ' || min(period_year) || '-' || max(period_year) || ' n=' || count(*) x from {s} group by all)"),
                "status": n(f"select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from {s} group by 1)"),
            }
            annotate("notice", "zus_yearbook trial silver", stats)
            show(con, "silver all", f"select * exclude (raw_sha256, source_version) from {s} order by table_code, measure_code, period_year, edition", limit=600)
            for test in sorted((ROOT / "warehouse/tests").glob("zus_yearbook_*.sql")):
                failing = con.sql(render(test, {"zus_yearbook_tables": s})).fetchall()
                annotate("notice" if not failing else "error", f"zus_yearbook test {test.stem}", {"failing_rows": len(failing), "first": [str(r) for r in failing[:10]]})
            gold = ROOT / "warehouse/models/gold/poc_build_zus_yearbook.sql"
            if gold.exists():
                con.sql("create or replace table geo as select 'zus_yearbook' as source_system, 'PL' as source_code, 'PL' as geography_key")
                con.sql("create or replace table g as " + render(gold, {"zus_yearbook_tables": s, "poc_map_geography": "geo"}))
                annotate("notice", "zus_yearbook trial gold", {
                    "gold_rows": n("select count(*) from g"),
                    "dup_series_date": n("select count(*) from (select 1 from g group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
                    "unmatched": n("select count(*) from g where geography_key = '_X'"),
                    "years": n("select min(period_label) || '-' || max(period_label) from g"),
                    "status": n("select string_agg(observation_status_key || '=' || c, ' ') from (select observation_status_key, count(*) c from g group by 1)"),
                })
                annotate("notice", "zus_yearbook trial gold Poland newest", {"values": n(
                    "select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes) from g "
                    "where (measure_key, date_key) in (select measure_key, max(date_key) from g group by 1)")})
                show(con, "gold sample", "select measure_key, period_label, value, unit_code, observation_status_key from g order by all", limit=400)
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
        annotate("notice", "zus_yearbook trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
