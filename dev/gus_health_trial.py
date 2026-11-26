"""Dev trial of gus_health (#2451) on the final code: Landing and Bronze into zz_dev_gus_health/ (both buckets) and
namespace zz_dev_gus_health, each twice; then the branch's Silver SQL, its tests (unique, not_null and accepted_values
from _gus_health.yml, the completeness test), the Gold build stage, the KPI rows and value bounds applied in memory
(Jinja-rendered with warehouse/macros), plus the GUS terms pages read for the licence quote. Reads silver.teryt_terc
(read-only) for the voivodeship names. Activity annotations go to the log only; the trial writes at most eight.
Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

import csv
import html
import json
from pathlib import Path
import re
import sys

import duckdb
import jinja2
import requests
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze, report
from ingestion.common import gus_publications as gp
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze import gus_health as health_bronze
from ingestion.to_landing.gus_health import SERIES
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "gus_health"
SCRATCH = "zz_dev_gus_health"
LAND = f"{SCRATCH}/01_landing"
MACROS = "".join((ROOT / f"warehouse/macros/{m}.sql").read_text() for m in ("xlsx_cells", "gus_tables"))
ANNOTATE = report.annotate


def quiet(level, title, payload=None, *args, **kwargs):
    print(f"ANNOTATION {level} | {title} | {json.dumps(payload, default=str, ensure_ascii=False)[:3000]}", flush=True)


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def landed_state(client, bucket) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def rows(name: str) -> list[dict]:
    path = ROOT / f"warehouse/seed_rows/{name}/{SOURCE}.csv"
    return list(csv.DictReader(open(path, encoding="utf-8")))


def terms(url: str, anchors: list[str]) -> str:
    """The text of the terms page around its first anchor word (tags stripped), for the licence quote."""
    r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 60))
    text = html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"(?s)<(script|style).*?</\1>", " ", r.text))))
    for anchor in anchors:
        at = text.find(anchor)
        if at >= 0:
            return f"{r.status_code} {r.url} :: " + text[max(0, at - 200): at + 1100]
    return f"{r.status_code} {r.url} :: (anchor not found) " + text[:600]


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = quiet
    rc = 0
    try:
        # Landing twice: the second run must write nothing
        states = []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            code = gp.extract(SERIES, 60, landing_root=LAND)
            states.append(landed_state(client, land_bucket))
            print("exit", code, "objects", len(states[-1]), flush=True)
        entities = sorted(gp.held(SERIES, LAND))
        verify_rc = bulk_file.main(SOURCE, "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
        data = [(k, s) for k, s in states[0] if not k.endswith(".json")]
        landing = {
            "files": len(data),
            "bytes": sum(s for _, s in data),
            "entities": entities,
            "second_run_unchanged": states[0] == states[1],
            "verify_exit": verify_rc,
        }
        ANNOTATE("notice" if landing["second_run_unchanged"] and not verify_rc else "error", f"{SOURCE} trial landing", landing)

        # Bronze twice: the second run must add nothing
        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            health_bronze.bronze_load(entities, landing_root=LAND, namespace=SCRATCH)
            snap = cat.load_table((SCRATCH, f"{SOURCE}_cells")).current_snapshot()
            snapshots.append(snap.snapshot_id if snap else None)
        cells = cat.load_table((SCRATCH, f"{SOURCE}_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # the table exists only after a rejection
            rejected = []
        con.register(f"{SOURCE}_cells", cells)
        bronze = {
            "rows": cells.num_rows,
            "per_entity": n(f"select string_agg(entity || ':' || c, ' ' order by entity) from (select entity, count(*) c from {SOURCE}_cells group by 1)"),
            "files": n(f"select count(distinct raw_sha256) from {SOURCE}_cells"),
            "members_sheets": n(f"select count(distinct member) || ' members, ' || count(distinct entity || member || sheet_name) || ' sheets' from {SOURCE}_cells"),
            "null_values": n(f"select count(*) from {SOURCE}_cells where value_text is null"),
            "rejected": [(r.get("entity"), (r.get("error") or r.get("reason") or "")[:100]) for r in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
        }
        ANNOTATE("notice" if bronze["second_run_unchanged"] and not rejected else "error", f"{SOURCE} trial bronze", bronze)

        # Silver and its tests
        silver_sql = render(ROOT / f"warehouse/models/silver/{SOURCE}/{SOURCE}_tables.sql", {f"{SOURCE}_cells": f"{SOURCE}_cells"})
        con.sql("create table s as " + silver_sql)
        con.sql("create table s2 as " + silver_sql)
        spec = yaml.safe_load((ROOT / f"warehouse/models/silver/{SOURCE}/_{SOURCE}.yml").read_text())["models"][0]
        failures = {"unique": n("select count(*) from (select 1 from s group by table_code, measure_code, area_name, group_code, period_year having count(*) > 1)")}
        for column in spec["columns"]:
            for test in column["tests"]:
                if test == "not_null":
                    failures[f"not_null {column['name']}"] = n(f"select count(*) from s where {column['name']} is null")
                elif isinstance(test, dict) and "accepted_values" in test:
                    con.execute("create or replace temp table allowed as select unnest(?) as v", [test["accepted_values"]["arguments"]["values"]])
                    failures[f"accepted_values {column['name']}"] = n(f"select count(*) from s where {column['name']} not in (select v from allowed)")
        complete = render(ROOT / f"warehouse/tests/{SOURCE}_tables_complete.sql", {f"{SOURCE}_tables": "s"})
        failures["complete"] = n(f"select count(*) from ({complete})")
        silver = {
            "rows": n("select count(*) from s"),
            "rebuild_same": n("select count(*) from (select * from s except select * from s2)") == 0 and n("select count(*) from s2") == n("select count(*) from s"),
            "tests_run": len(failures),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
            "years": n("select min(period_year) || '-' || max(period_year) from s"),
            "per_measure": n("select string_agg(x, '; ' order by x) from (select measure_code || ' ' || min(period_year) || '-' || max(period_year) || ' n=' || count(*) x from s group by measure_code)"),
            "status": n("select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from s group by 1)"),
            "complete_failing": n(f"select string_agg(measure_code || ' ' || period_year || ' ' || found, '; ') from ({complete})"),
        }
        ANNOTATE("notice" if not any(failures.values()) and silver["rebuild_same"] else "error", f"{SOURCE} trial silver", silver)

        # markers in the modelled columns, label repairs, sheets and notes
        con.sql("""create table picked as select c.* from gus_health_cells c join (select distinct raw_sha256, member, sheet_name, column_number from s) p
            using (raw_sha256, member, sheet_name, column_number) where c.row_number > 3""")
        markers = {
            "non_numeric_in_value_rows": n("select string_agg(distinct value_text, ' | ') from s where try_cast(value_text as double) is null"),
            "symbol_cells_in_modelled_columns": n("""select string_agg(t || '=' || c, ' ') from (select trim(value_text) t, count(*) c from picked
                where trim(value_text) in ('-', '–', '—', '.', '#', 'x', 'X', ':') or regexp_matches(trim(value_text), '[0-9][*a-z)]+$') group by 1)"""),
            "zero_values": n("select count(*) from s where value = 0"),
            "split_names_repaired": n("select string_agg(distinct entity, ',') from s where area_name = 'warmińsko-mazurskie' and row_number > 0 and entity in ('zdrowie_2012','zdrowie_2013','zdrowie_2014','zdrowie_2015')"),
            "sheets": n("select string_agg(distinct entity || ' ' || table_code || '=' || sheet_name, ', ' order by entity || ' ' || table_code || '=' || sheet_name) from s"),
            "sources_named": n("select string_agg(distinct regexp_extract(table_notes, '(Ź ?r ?ó ?d ?ł ?o:?[^.]*)', 1), ' || ') from s"),
            "third_party_context": n("""select string_agg(distinct entity || ' ' || table_code || ': ' || x, ' || ') from (select entity, table_code,
                unnest(regexp_extract_all(table_notes, '[^.]{0,140}(NFZ|Narodow[a-ząęł]* Fundusz[a-z]*|Centrum e-Zdrowia|CSIOZ)[^.]{0,80}')) x from s)
                where entity in ('zdrowie_2012', 'zdrowie_2020', 'zdrowie_2024')"""),
            "owner_files_left_out": n("select count(*) from gus_health_cells where member like '%~$%'"),
        }
        ANNOTATE("notice", f"{SOURCE} trial markers", markers)

        # Gold build stage, with the TERYT names as poc_map_geography (geo of the source's geography macro)
        terc = cat.load_table(("silver", "teryt_terc")).scan(selected_fields=("terc_code", "name_pl", "unit_level")).to_arrow()
        con.register("terc", terc)
        con.sql(f"""create table geo as select '{SOURCE}' as source_system, lower(name_pl) as source_code, 'TERC:' || terc_code as geography_key
            from terc where unit_level = 'wojewodztwo' union all select '{SOURCE}', 'polska', 'PL'""")
        con.sql("create table g as " + render(ROOT / f"warehouse/models/gold/poc_build_{SOURCE}.sql", {f"{SOURCE}_tables": "s", "poc_map_geography": "geo"}))
        series = "measure_key || '|' || geography_key || '|' || breakdown_json"
        kpi = rows("kpi_measure")
        bounds = {b[0]: b for b in re.findall(r"\['(\w+)', '([^']+)', '([^']+)', '([^']+)'\]", (ROOT / f"warehouse/macros/value_range/{SOURCE}.sql").read_text())}
        kpi_stats = {}
        for k in kpi:
            pattern, (_, _, lo, hi) = k["measure_key_pattern"], bounds[k["kpi_key"]]
            kpi_stats[k["kpi_key"]] = {
                "rows": n(f"select count(*) from g where measure_key like '{pattern}'"),
                "max_per_area_period": n(f"select coalesce(max(c), 0) from (select count(*) c from g where measure_key like '{pattern}' group by geography_key, date_key)"),
                "outside_bound": n(f"select count(*) from g where measure_key like '{pattern}' and (value < {lo} or value > {hi})"),
                "max": n(f"select max(value) from g where measure_key like '{pattern}'"),
            }
        unit_codes = {r["unit_code"] for r in rows("unit_map")}
        gold = {
            "rows": n("select count(*) from g"),
            "measures": n("select count(distinct measure_key) from g"),
            "dup_series_date": n(f"select count(*) from (select 1 from g group by {series}, date_key having count(*) > 1)"),
            "unmatched_geo": n("select count(*) from g where geography_key = '_X'"),
            "geographies": n("select count(distinct geography_key) from g"),
            "null_values": n("select count(*) from g where value is null"),
            "dates": n("select min(date_key) || '..' || max(date_key) from g"),
            "frequencies": n("select string_agg(distinct frequency, ',') from g"),
            "unmapped_units": sorted(set(r[0] for r in con.sql("select distinct unit_code from g").fetchall()) - unit_codes),
            "kpi": kpi_stats,
            "licence": {"source_row": rows("source")[0]["licence_key"], "licence_row": rows("licence")[0]["licence_key"], "commercial_ok": rows("licence")[0]["commercial_ok"]},
        }
        ok = not gold["dup_series_date"] and not gold["unmatched_geo"] and not gold["unmapped_units"]
        ANNOTATE("notice" if ok else "error", f"{SOURCE} trial gold", gold)
        spot = n("""select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes, period_label)
            from g where geography_key = 'PL' and period_label in ('2012', '2016', '2020', '2021', '2023', '2024')""")
        voiv = n("""select string_agg(geography_source_code || ' ' || measure_codes || ' ' || period_label || '=' || value, '; ')
            from g where geography_source_code in ('mazowieckie', 'warmińsko-mazurskie') and period_label in ('2012', '2024') and measure_codes in ('beds', 'doctors', 'doctors_pre2021')""")
        ANNOTATE("notice", f"{SOURCE} trial spot values", {"poland": spot, "voivodeships": voiv})
        rc = 0 if ok and not any(failures.values()) and landing["second_run_unchanged"] and bronze["second_run_unchanged"] else 1

        # the GUS terms (licence quote)
        quotes = {}
        for url, anchors in (("https://stat.gov.pl/copyright", ["kopiowani", "Kopiowani", "źródła", "źródło"]),
                             ("https://stat.gov.pl/en/copyright/", ["copying", "Copying", "source"])):
            try:
                quotes[url] = terms(url, anchors)
            except requests.RequestException as exc:
                quotes[url] = f"error {type(exc).__name__}: {str(exc)[:120]}"
        ANNOTATE("notice", f"{SOURCE} trial licence terms", quotes)
        print("STATS", json.dumps({"landing": landing, "bronze": bronze, "silver": silver, "markers": markers, "gold": gold}, default=str, ensure_ascii=False), flush=True)
    finally:
        report.annotate = bulk_bronze.annotate = bulk_file.annotate = ANNOTATE
        removed = 0
        for bucket in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
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
        except Exception as exc:  # report, do not hide
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        ANNOTATE("notice" if not left else "error", f"{SOURCE} trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
