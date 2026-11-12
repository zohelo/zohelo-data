"""Dev trial of Crossref (CD-029, #553): Landing and Bronze into zz_dev_crossref/ (both buckets) and namespace
zz_dev_crossref, each twice; Bronze profiled; then the branch's Silver and Gold SQL and the source's tests applied in
memory (Jinja-rendered), checks and Poland spot values printed and annotated. Everything written is removed at the
end (finally). Deleted before merge."""

from __future__ import annotations

import csv
import json
from pathlib import Path
import re
import sys

import duckdb
import jinja2

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.crossref import load as bronze
from ingestion.to_landing import crossref as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_crossref"
W = ROOT / "warehouse"


def render(path: Path, tables: dict[str, str], macros: str = "") -> str:
    text = macros + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def one(con, sql):
    return con.sql(sql).fetchall()[0][0]


def show(con, title, sql, limit=80):
    rel = con.sql(sql)
    rows = rel.fetchall()
    print(f"\n=== {title}\n" + "\t".join(d[0] for d in rel.description), flush=True)
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:90] for v in r))
    return rows


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    stats: dict = {}
    try:
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            status = bulk_file.main("crossref", "load", landing_root=land, specs=landing.specs(),
                                    pause_seconds=landing.PAUSE_SECONDS)  # fmt: skip
            held = {s["entity"]: bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"),
                                                  bulk_file.prefix("crossref", s["entity"], land))
                    for s in landing.specs()}  # fmt: skip
            stats[f"landing_{attempt}"] = {
                "status": status,
                "entities": len(held),
                "copies": sum(len(v) for v in held.values()),
                "bytes": sum(v[-1]["size_bytes"] for v in held.values() if v),
                "entities_with_2_copies": [k for k, v in held.items() if len(v) > 1],
            }
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            bronze.load(10, landing_root=land, namespace=SCRATCH)
            stats[f"bronze_{attempt}_files"] = {
                t: len(bf.loaded_hashes(cat.load_table((SCRATCH, t))))
                for t in ("crossref_facet_counts", "crossref_funders")
            }
        counts = cat.load_table((SCRATCH, "crossref_facet_counts")).scan().to_arrow()
        funders = cat.load_table((SCRATCH, "crossref_funders")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "crossref_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        con.register("b_counts", counts)
        con.register("b_funders", funders)
        stats["bronze"] = {
            "count_rows": counts.num_rows,
            "funder_rows": funders.num_rows,
            "rejected": [(r.get("entity"), (r.get("error") or "")[:100]) for r in rejected],
            "entities": one(con, "select count(distinct entity) from b_counts"),
            "rows_per_facet": one(con, "select string_agg(facet || '=' || n, ' ' order by facet) from "
                                       "(select facet, count(*) n from b_counts group by 1)"),
            "null_value_rows": one(con, "select count(*) from b_counts where facet <> '_total' and facet_value is null"),
            "non_numeric_works": one(con, "select string_agg(distinct works, '|') from b_counts "
                                          "where try_cast(works as bigint) is null"),
            "published_years": one(con, "select min(facet_value) || '..' || max(facet_value) from b_counts "
                                        "where facet = 'published'"),
            "odd_years": one(con, "select string_agg(facet_value || ':' || works, ' ' order by facet_value) from "
                                  "b_counts where facet = 'published' and (try_cast(facet_value as int) < 1900 "
                                  "or try_cast(facet_value as int) > 2026 or try_cast(facet_value as int) is null)"),
            "facet_cut": one(con, "select string_agg(entity || ':' || facet || ' ' || n || '/' || vc, ' ') from "
                                  "(select entity, facet, count(*) n, max(cast(facet_value_count as int)) vc "
                                  "from b_counts where facet <> '_total' group by all having count(*) >= 1000 "
                                  "or count(*) < max(cast(facet_value_count as int)))"),
            "funder_locations": one(con, "select string_agg(distinct location, '|') from b_funders"),
            "funder_null_ids": one(con, "select count(*) from b_funders where funder_id is null or name is null"),
        }
        print("STATS bronze", json.dumps(stats, default=str), flush=True)
        annotate("notice", "crossref trial landing and bronze", stats)

        tables = {"crossref_facet_counts": "b_counts", "crossref_funders": "b_funders"}
        silver = W / "models/silver/crossref"
        con.sql("create table s_counts as " + render(silver / "crossref_works_pl_counts.sql", tables))
        con.sql("create table s_funders as " + render(silver / "crossref_funders.sql", tables))
        macros = (W / "macros/sources/crossref.sql").read_text()
        geo = jinja2.Environment().from_string(macros + "with {{ poc_geography__crossref() }} select * from crossref")
        con.sql("create table geo as " + geo.render())
        refs = {"crossref_works_pl_counts": "s_counts", "crossref_funders": "s_funders", "poc_map_geography": "geo"}
        con.sql("create table g as " + render(W / "models/gold/poc_build_crossref.sql", refs))
        tests = {}
        for name in ("crossref_works_pl_counts_complete", "crossref_funders_complete"):
            rows = show(con, f"test {name}", render(W / f"tests/{name}.sql", refs))
            tests[name] = len(rows)
        s = {
            "silver_rows": one(con, "select count(*) from s_counts"),
            "silver_dup": one(con, "select count(*) from (select 1 from s_counts group by entity, facet, "
                                   "coalesce(facet_value, '') having count(*) > 1)"),
            "silver_null_keys": one(con, "select count(*) from s_counts where entity is null or facet is null "
                                         "or works is null or raw_sha256 is null"),
            "silver_years": one(con, "select min(publication_year) || '..' || max(publication_year) from s_counts"),
            "truncated_facets": one(con, "select string_agg(distinct facet, ',') from s_counts where is_truncated"),
            "funders_rows": one(con, "select count(*) from s_funders"),
            "funders_dup": one(con, "select count(*) - count(distinct funder_id) from s_funders"),
            "tests_failing_rows": tests,
            "gold_rows": one(con, "select count(*) from g"),
            "gold_dup_series_date": one(con, "select count(*) from (select 1 from g group by measure_key, "
                                             "geography_key, breakdown_json, date_key having count(*) > 1)"),
            "gold_null_keys": one(con, "select count(*) from g where measure_key is null or geography_key is null "
                                       "or date_key is null or value is null"),
            "gold_unmatched": one(con, "select count(*) from g where geography_key = '_X'"),
            "gold_per_measure": one(con, "select string_agg(measure_key || '=' || n || ' ' || y, '; ') from "
                                         "(select measure_key, count(*) n, min(period_label) || '..' || "
                                         "max(period_label) y from g group by 1)"),
            "gold_status": one(con, "select string_agg(observation_status_key || '=' || n, ' ') from "
                                    "(select observation_status_key, count(*) n from g group by 1)"),
            "funder_labels": one(con, "select count(*) filter (where breakdown_label_json is not null) || '/' || "
                                      "count(*) from g where measure_codes = 'funder'"),
            "value_max_total": one(con, "select max(value) from g where measure_key = 'crossref:works_pl:works'"),
        }
        print("STATS silver gold", json.dumps(s, default=str), flush=True)
        annotate("notice", "crossref trial silver and gold", s)
        spot = {
            "total_by_year": one(con, "select string_agg(period_label || '=' || cast(value as bigint) || "
                                      "case when observation_status_key <> 'ok' then '(' || "
                                      "observation_status_key || ')' else '' end, ' ' order by period_label) "
                                      "from g where measure_key = 'crossref:works_pl:works' and period_label >= '2010'"),
            "types_2024": one(con, "select string_agg(breakdown_json || '=' || cast(value as bigint), ' ' order by "
                                   "value desc) from g where measure_codes = 'type' and period_label = '2024'"),
            "top_publishers_2024": one(con, "select string_agg(breakdown_json || '=' || cast(value as bigint), ' ' "
                                            "order by value desc) from (select * from g where measure_codes = "
                                            "'publisher' and period_label = '2024' order by value desc limit 8)"),
            "top_funders_2024": one(con, "select string_agg(coalesce(breakdown_label_json, breakdown_json) || '=' "
                                         "|| cast(value as bigint), ' ' order by value desc) from (select * from g "
                                         "where measure_codes = 'funder' and period_label = '2024' "
                                         "order by value desc limit 6)"),
        }
        print("SPOT", json.dumps(spot, ensure_ascii=False), flush=True)
        annotate("notice", "crossref trial Poland", spot)
        seeds = {n: list(csv.DictReader((W / f"seeds/{n}.csv").open(encoding="utf-8"))) for n in ("source", "licence")}
        src = next(r for r in seeds["source"] if r["source_key"] == "crossref")
        lic = next((r for r in seeds["licence"] if r["licence_key"] == src["licence_key"]), None)
        annotate("notice", "crossref trial licence row", {"source": src, "licence": lic})
        show(con, "silver funders sample", "select funder_id, name, replaces from s_funders order by funder_id limit 8")
        show(con, "gold sample", "select measure_key, breakdown_json, period_label, value, observation_status_key "
                                 "from g order by measure_key, period_label desc, value desc limit 40")
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
        except Exception as exc:
            print("drop failed", exc)
        annotate("notice", "crossref trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
