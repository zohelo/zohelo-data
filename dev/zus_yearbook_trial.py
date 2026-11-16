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


def robots() -> None:
    for host in ("https://www.zus.pl", "https://psz.zus.pl"):
        try:
            r = requests.get(host + "/robots.txt", headers={"User-Agent": UA}, timeout=60)
            print("robots", host, r.status_code, repr(r.text[:400]), flush=True)
            annotate("notice", f"zus_yearbook robots {host}", {"status": r.status_code, "lines": r.text.splitlines()[:12]})
        except Exception as exc:  # noqa: BLE001 - probe continues
            print("robots error", host, exc, flush=True)
        time.sleep(3)


def dump(con, held) -> None:
    """Log: every page's first two lines (titles), the first 12 pages in full, and full pages that mention
    voivodeships with pensions, insured persons or payers (newest and oldest edition)."""
    for entity in held:
        print(f"\n##### TITLES {entity}", flush=True)
        for page, text in con.sql(
            f"select page_number, string_agg(line_text, ' || ' order by line_number) from zus_yearbook_lines "
            f"where entity = '{entity}' and line_number <= 3 group by 1 order by 1"
        ).fetchall():
            print(f"{entity} p{page}: {' '.join(text.split())[:220]}")
    for entity in [held[0], held[-1]] if held else []:
        pages = [r[0] for r in con.sql(
            f"select page_number from zus_yearbook_lines where entity = '{entity}' group by 1 "
            "having bool_or(page_number <= 12) or (bool_or(lower(line_text) like '%wojew%') and "
            "bool_or(regexp_matches(lower(line_text), 'emeryt|ubezpieczon|płatnik|zasiłk|renc')))"
            " order by 1"
        ).fetchall()]
        print(f"\n##### PAGES {entity}: {pages}", flush=True)
        for page in pages[:45]:
            print(f"\n======== {entity} PAGE {page}")
            for (line,) in con.sql(
                f"select line_text from zus_yearbook_lines where entity = '{entity}' and page_number = {page} order by line_number"
            ).fetchall():
                print(line[:230])


TERC = {"polska": "PL", "dolnośląskie": "TERC:02", "kujawsko-pomorskie": "TERC:04", "lubelskie": "TERC:06",
        "lubuskie": "TERC:08", "łódzkie": "TERC:10", "małopolskie": "TERC:12", "mazowieckie": "TERC:14",
        "opolskie": "TERC:16", "podkarpackie": "TERC:18", "podlaskie": "TERC:20", "pomorskie": "TERC:22",
        "śląskie": "TERC:24", "świętokrzyskie": "TERC:26", "warmińsko-mazurskie": "TERC:28",
        "wielkopolskie": "TERC:30", "zachodniopomorskie": "TERC:32"}
TITLES = r"(?i)(płatnicy|ubezpieczeni)\s+według\s+województw|emerytury i renty wypłacone przez (zus|zakład)"


def profile(con) -> None:
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    annotate("notice", "zus_yearbook trial bronze profile", {
        "rows": n("select count(*) from zus_yearbook_lines"),
        "editions": n("select count(distinct entity) from zus_yearbook_lines"),
        "null_keys": n("select count(*) from zus_yearbook_lines where entity is null or page_number is null or line_number is null or line_text is null or raw_sha256 is null"),
        "dup_keys": n("select count(*) from (select 1 from zus_yearbook_lines group by entity, page_number, line_number having count(*) > 1)"),
        "versions": n("select string_agg(distinct entity || '@' || source_version, ' ') from zus_yearbook_lines"),
        "title_pages": n(f"select string_agg(entity || ':p' || page_number, ' ' order by entity, page_number) from (select distinct entity, page_number from zus_yearbook_lines where page_number > 16 and regexp_matches(line_text, '{TITLES}'))"),
    })


def dump2(con, client, held, land) -> None:
    """Log the target table pages of every edition in full (layout mode), and the first pension page in plain mode."""
    import io

    import pypdf

    from ingestion.common import bulk_bronze
    from ingestion.to_landing.patterns import bulk_file

    for entity in held:
        pages = [r[0] for r in con.sql(
            f"select distinct page_number from zus_yearbook_lines where entity = '{entity}' and page_number > 16 "
            f"and regexp_matches(line_text, '{TITLES}') order by 1").fetchall()]
        print(f"\n##### TARGET {entity}: {pages}", flush=True)
        for page in pages[:8]:
            print(f"\n======== {entity} PAGE {page}")
            for (line,) in con.sql(
                f"select line_text from zus_yearbook_lines where entity = '{entity}' and page_number = {page} order by line_number"
            ).fetchall():
                print(line[:420])
        pension = [r[0] for r in con.sql(
            f"select distinct page_number from zus_yearbook_lines where entity = '{entity}' and page_number > 16 "
            "and regexp_matches(line_text, '(?i)emerytury i renty wypłacone przez (zus|zakład)') order by 1").fetchall()]
        if pension:
            receipt = bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix(SOURCE, entity, land))[-1]
            body = bulk_bronze.read_raw(client, req("STORAGE_LANDING_BUCKET_NAME"), SOURCE, receipt)
            page = pypdf.PdfReader(io.BytesIO(body)).pages[pension[0] - 1]
            print(f"\n======== PLAIN {entity} PAGE {pension[0]}")
            for line in (page.extract_text() or "").splitlines()[:70]:
                print(line[:300])


def silver_gold(con, silver) -> None:
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    con.sql("create or replace table zus_yearbook_tables as " + render(silver, {"zus_yearbook_lines": "zus_yearbook_lines"}))
    s = "zus_yearbook_tables"
    stats = {
        "silver_rows": n(f"select count(*) from {s}"),
        "dup_grain": n(f"select count(*) from (select 1 from {s} group by measure_code, area_name, period_year having count(*) > 1)"),
        "null_keys": n(f"select count(*) from {s} where edition is null or table_code is null or measure_code is null or unit_code is null or area_name is null or period_year is null or cell_status is null or raw_sha256 is null"),
        "per_measure": n(f"select string_agg(x, '; ' order by x) from (select measure_code || ' ' || min(period_year) || '-' || max(period_year) || ' n=' || count(*) || ' areas=' || count(distinct area_name) x from {s} group by measure_code)"),
        "per_edition": n(f"select string_agg(x, '; ' order by x) from (select edition || ':' || measure_code || ' p' || min(page_number) || ' n=' || count(*) x from {s} group by edition, measure_code)"),
        "cell_status": n(f"select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from {s} group by 1)"),
        "areas_not_accepted": n(f"select count(*) from {s} where area_name not in ({', '.join(repr(a) for a in TERC)})"),
    }
    annotate("notice", "zus_yearbook trial silver", stats)
    show(con, "silver all", f"select edition, measure_code, area_name, period_year, value_text, value, cell_status, page_number from {s} order by measure_code, area_name, period_year", limit=700)
    gold = ROOT / "warehouse/models/gold/poc_build_zus_yearbook.sql"
    con.sql("create or replace table geo as select 'zus_yearbook' as source_system, k as source_code, v as geography_key from (select unnest(map_keys(m)) k, unnest(map_values(m)) v from (select MAP " + "{" + ", ".join(f"'{k}': '{v}'" for k, v in TERC.items()) + "} m))")
    con.sql("create or replace table g as " + render(gold, {"zus_yearbook_tables": s, "poc_map_geography": "geo"}))
    annotate("notice", "zus_yearbook trial gold", {
        "gold_rows": n("select count(*) from g"),
        "dup_series_date": n("select count(*) from (select 1 from g group by measure_key || '|' || geography_key || '|' || breakdown_json, date_key having count(*) > 1)"),
        "unmatched": n("select count(*) from g where geography_key = '_X'"),
        "null_values": n("select count(*) from g where value is null"),
        "per_measure": n("select string_agg(x, '; ' order by x) from (select measure_key || ' ' || min(period_label) || '-' || max(period_label) || ' n=' || count(*) || ' geo=' || count(distinct geography_key) x from g group by measure_key)"),
        "per_area_period_max": n("select max(c) from (select count(*) c from g group by measure_key, geography_key, date_key)"),
    })
    annotate("notice", "zus_yearbook trial gold Poland spot values", {"values": n(
        "select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes, period_label) from g "
        "where geography_key = 'PL' and period_label in ('2009', '2011', '2014', '2017', '2020', '2023')")})
    for test in sorted((ROOT / "warehouse/tests").glob("zus_yearbook_*.sql")):
        failing = con.sql(render(test, {"zus_yearbook_tables": s, "poc_build_zus_yearbook": "g"})).fetchall()
        annotate("notice" if not failing else "error", f"zus_yearbook test {test.stem}", {"failing_rows": len(failing), "first": [str(r) for r in failing[:10]]})


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
    robots()
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
        if "dump" in argv:
            dump(con, held)
        for entity in held if "files" in argv else []:
            rows = con.sql(f"select page_number, line_text from zus_yearbook_lines where entity = '{entity}' order by page_number, line_number").fetchall()
            with open(out / f"{entity}.txt", "w", encoding="utf-8") as f:
                last = None
                for page_number, text in rows:
                    if page_number != last:
                        f.write(f"\n=================== PAGE {page_number}\n")
                        last = page_number
                    f.write(text + "\n")
        profile(con)
        if "dump2" in argv:
            dump2(con, client, held, land)
        silver = ROOT / "warehouse/models/silver/zus_yearbook/zus_yearbook_tables.sql"
        if silver.exists():
            silver_gold(con, silver)
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
