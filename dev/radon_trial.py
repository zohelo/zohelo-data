"""Dev trial of RAD-on / POL-on (PLS-023, #2990): Landing and Bronze into zz_dev_radon/ (both buckets) and namespace
zz_dev_radon, each twice; landing.inspect and landing.verify on the scratch copies; Bronze profiled; then the
branch's Silver and Gold SQL, the YAML tests (not_null, unique, accepted_values) and the singular tests applied in
memory (Jinja-rendered). gold.dim_geography is read from production (read only) for the voivodeship map. Checks and
Poland spot values printed and annotated. Everything written is removed at the end (finally). Deleted before merge.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
import re
import sys

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.radon import load as bronze
from ingestion.to_landing import radon as landing
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_landing.radon import extract

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_radon"
W = ROOT / "warehouse"
PERSON_HINTS = ("manager", "mail", "phone", "surname", "firstname", "employee", "person", "pesel", "orcid")


def render(path_or_text, tables: dict[str, str], macros: str = "") -> str:
    text = path_or_text.read_text() if isinstance(path_or_text, Path) else path_or_text
    text = macros + re.sub(r"\{\{ config\(.*?\) \}\}", "", text, flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def one(con, sql):
    return con.sql(sql).fetchall()[0][0]


def show(con, title, sql, limit=60):
    rel = con.sql(sql)
    rows = rel.fetchall()
    print(f"\n=== {title}\n" + "\t".join(d[0] for d in rel.description), flush=True)
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:90] for v in r))
    return rows


def yaml_tests(con, path: Path, tables: dict[str, str]) -> dict[str, int]:
    """not_null, unique and accepted_values of a dbt YAML file, run against the in-memory tables."""
    failures = {}
    for model in yaml.safe_load(path.read_text())["models"]:
        table = tables.get(model["name"])
        if table is None:
            continue
        for col in model.get("columns", []):
            for test in col.get("tests", []):
                name, c = (test if isinstance(test, str) else next(iter(test))), col["name"]
                if name == "not_null":
                    n = one(con, f'select count(*) from {table} where "{c}" is null')
                elif name == "unique":
                    n = one(con, f'select count(*) from (select 1 from {table} where "{c}" is not null '
                                 f'group by "{c}" having count(*) > 1)')  # fmt: skip
                elif name == "accepted_values":
                    vals = test[name]["arguments"]["values"]
                    lst = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                    n = one(con, f'select count(*) from {table} where "{c}" is not null and "{c}" not in ({lst})')
                    if n:
                        print(f"accepted_values {model['name']}.{c} outside:",
                              con.sql(f'select string_agg(distinct "{c}", \'|\') from {table} '
                                      f'where "{c}" not in ({lst})').fetchall()[0][0])  # fmt: skip
                else:
                    continue
                failures[f"{model['name']}.{c}.{name}"] = n
    return failures


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    lbucket = req("STORAGE_LANDING_BUCKET_NAME")
    stats: dict = {}
    try:
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            status = extract.extract(60, landing_root=land)
            held = {s["entity"]: bulk_file.landed(client, lbucket, bulk_file.prefix("radon", s["entity"], land))
                    for s in landing.specs()}  # fmt: skip
            stats[f"landing_{attempt}"] = {
                "status": status,
                "copies": {k: len(v) for k, v in held.items()},
                "bytes": {k: v[-1]["size_bytes"] for k, v in held.items() if v},
                "detail": {k: v[-1].get("detail") for k, v in held.items() if v},
            }
        stats["landing_inspect"] = extract.inspect(landing_root=land)
        stats["landing_verify"] = bulk_file.main("radon", "verify", specs=landing.specs(), landing_root=land)
        print("STATS landing", json.dumps(stats, default=str), flush=True)
        annotate("notice", "radon trial landing", stats)

        b: dict = {}
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            b[f"load_{attempt}_status"] = bronze.load(10, landing_root=land, namespace=SCRATCH)
            b[f"load_{attempt}_files"] = {
                t: len(bf.loaded_hashes(cat.load_table((SCRATCH, t)))) for t in bronze.TABLES.values()
            }
        inst = cat.load_table((SCRATCH, "radon_institutions")).scan().to_arrow()
        crs = cat.load_table((SCRATCH, "radon_courses")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, bronze.REJECTED)).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        con.register("b_inst", inst)
        con.register("b_crs", crs)
        cols = [c for c in inst.column_names + crs.column_names]
        b.update({
            "inst_rows": inst.num_rows,
            "inst_distinct_uuid": one(con, "select count(distinct institutionUuid) from b_inst"),
            "inst_null_uuid": one(con, "select count(*) from b_inst where institutionUuid is null"),
            "courses_rows": crs.num_rows,
            "courses_distinct_uuid": one(con, "select count(distinct courseUuid) from b_crs"),
            "courses_null_uuid": one(con, "select count(*) from b_crs where courseUuid is null"),
            "rejected": [(r.get("entity"), (r.get("error") or "")[:160]) for r in rejected],
            "person_like_columns": [c for c in cols if any(h in c.lower() for h in PERSON_HINTS)],
            "inst_start_range": one(con, "select min(iStartDT) || '..' || max(iStartDT) from b_inst"),
            "courses_creation_range": one(con, "select min(creationDate) || '..' || max(creationDate) from b_crs"),
            "inst_last_refresh": one(con, "select min(lastRefresh) || '..' || max(lastRefresh) from b_inst"),
            "courses_last_refresh": one(con, "select min(lastRefresh) || '..' || max(lastRefresh) from b_crs"),
        })
        print("STATS bronze", json.dumps(b, default=str), flush=True)
        annotate("notice", "radon trial bronze", b)
        prof = {
            "inst_status": one(con, "select string_agg(k || '=' || n, ' | ' order by n desc) from (select "
                                    "statusCode || ':' || coalesce(status, 'NULL') k, count(*) n from b_inst group by 1)"),
            "inst_kind": one(con, "select string_agg(k || '=' || n, ' | ' order by n desc) from (select "
                                  "coalesce(iKindCd, 'NULL') || ':' || coalesce(iKindName, 'NULL') k, count(*) n "
                                  "from b_inst group by 1)"),
            "inst_voiv": one(con, "select string_agg(k || '=' || n, ' | ' order by k) from (select "
                                  "coalesce(voivodeshipCode, 'NULL') || ':' || coalesce(voivodeship, 'NULL') k, "
                                  "count(*) n from b_inst group by 1)"),
            "inst_country": one(con, "select string_agg(k || '=' || n, ' | ' order by n desc) from (select "
                                     "coalesce(country, 'NULL') k, count(*) n from b_inst group by 1)"),
            "c_status": one(con, "select string_agg(k || '=' || n, ' | ' order by n desc) from (select "
                                 "coalesce(currentStatusCode, 'NULL') || ':' || coalesce(currentStatusName, 'NULL') k, "
                                 "count(*) n from b_crs group by 1)"),
            "c_level": one(con, "select string_agg(k || '=' || n, ' | ' order by n desc) from (select "
                                "coalesce(levelCode, 'NULL') || ':' || coalesce(levelName, 'NULL') k, count(*) n "
                                "from b_crs group by 1)"),
            "c_profile": one(con, "select string_agg(k || '=' || n, ' | ' order by n desc) from (select "
                                  "coalesce(profileCode, 'NULL') || ':' || coalesce(profileName, 'NULL') k, "
                                  "count(*) n from b_crs group by 1)"),
            "c_voiv": one(con, "select string_agg(k || '=' || n, ' | ' order by k) from (select "
                               "coalesce(leadingInstitutionVoivodeship, 'NULL') k, count(*) n from b_crs group by 1)"),
            "c_flags": one(con, "select string_agg(distinct coalesce(leadingInstitutionIsForeign,'NULL') || '/' || "
                                "coalesce(coLed,'NULL') || '/' || coalesce(teacherTraining,'NULL') || '/' || "
                                "coalesce(philological,'NULL'), ' ') from b_crs"),
            "c_inst_kind": one(con, "select string_agg(k || '=' || n, ' | ' order by n desc) from (select "
                                    "coalesce(mainInstitutionKindCode,'NULL') || ':' || coalesce(mainInstitutionKind,"
                                    "'NULL') k, count(*) n from b_crs group by 1)"),
        }
        print("PROFILE", json.dumps(prof, ensure_ascii=False), flush=True)
        annotate("notice", "radon trial bronze profile", prof)
        show(con, "launch statuses", "select statusCode, statusName, count(*) n from (select unnest(from_json("
                                     "courseInstances, '[{\"statusCode\":\"VARCHAR\",\"statusName\":\"VARCHAR\"}]'), "
                                     "recursive := true) from b_crs) group by all order by n desc")
        show(con, "course status x launches", "select currentStatusCode, json_array_length(courseInstances) = 0 as "
                                              "no_launch, count(*) from b_crs group by all order by all")

        tables = {"radon_institutions": "b_inst", "radon_courses": "b_crs"}
        silver = W / "models/silver/radon"
        con.sql("create table s_inst as " + render(silver / "radon_institutions.sql", tables))
        con.sql("create table s_crs as " + render(silver / "radon_courses.sql", tables))
        dimgeo = cat.load_table(("gold", "dim_geography")).scan().to_arrow()
        con.register("dimgeo", dimgeo)
        macros = (W / "macros/sources/radon.sql").read_text()
        con.sql("create table geomap as " + render("with geo as (select * from dimgeo), {{ poc_geography__radon() }} "
                                                   "select * from radon", {}, macros))  # fmt: skip
        refs = {"radon_institutions": "s_inst", "radon_courses": "s_crs", "poc_map_geography": "geomap"}
        con.sql("create table g as " + render(W / "models/gold/poc_build_radon.sql", refs))
        con.sql("create table e_inst as " + render(W / "models/gold/radon_institution.sql", refs))
        con.sql("create table e_crs as " + render(W / "models/gold/radon_study_programme.sql", refs))
        tests = yaml_tests(con, silver / "_radon.yml", {"radon_institutions": "s_inst", "radon_courses": "s_crs"})
        tests.update(yaml_tests(con, W / "models/gold/_radon.yml", {
            "radon_institution": "e_inst", "radon_study_programme": "e_crs", "poc_build_radon": "g"}))
        for name in ("radon_institutions_complete", "radon_courses_complete"):
            tests[name] = len(show(con, f"test {name}", render(W / f"tests/{name}.sql", refs)))
        s = {
            "silver_inst_rows": one(con, "select count(*) from s_inst"),
            "silver_courses_rows": one(con, "select count(*) from s_crs"),
            "silver_inst_dup": one(con, "select count(*) - count(distinct institution_uuid) from s_inst"),
            "silver_courses_dup": one(con, "select count(*) - count(distinct course_uuid) from s_crs"),
            "inst_start_unparsed": one(con, "select count(*) from b_inst where iStartDT is not null and "
                                            "try_cast(iStartDT as date) is null"),
            "tests_failing": {k: v for k, v in tests.items() if v},
            "tests_run": len(tests),
            "leading_discipline_set": one(con, "select count(leading_discipline_code) || '/' || count(*) from s_crs"),
            "leading_missing_in_inst": one(con, "select count(*) from s_crs c where c.leading_institution_uuid is not "
                                                "null and not exists (select 1 from s_inst i where i.institution_uuid "
                                                "= c.leading_institution_uuid)"),
            "geomap_rows": one(con, "select count(*) from geomap"),
            "inst_geo_unmatched": one(con, "select string_agg(distinct voivodeship, '|') from e_inst where "
                                           "voivodeship is not null and geography_key is null"),
            "crs_geo_unmatched": one(con, "select string_agg(distinct leading_voivodeship, '|') from e_crs where "
                                          "leading_voivodeship is not null and geography_key is null"),
            "entity_rows": [one(con, "select count(*) from e_inst"), one(con, "select count(*) from e_crs")],
            "gold_rows": one(con, "select count(*) from g"),
            "gold_dup_series_date": one(con, "select count(*) from (select 1 from g group by measure_key, "
                                             "geography_key, breakdown_json, date_key having count(*) > 1)"),
            "gold_null_keys": one(con, "select count(*) from g where measure_key is null or geography_key is null "
                                       "or date_key is null or value is null"),
            "gold_unmatched": one(con, "select count(*) from g where geography_key = '_X'"),
            "gold_dates": one(con, "select string_agg(distinct measure_key || '@' || frequency || ':' || "
                                   "period_label, ' ') from g"),
        }
        print("STATS silver gold", json.dumps(s, default=str), flush=True)
        annotate("notice", "radon trial silver and gold", s)
        spot = {
            "PL_totals": one(con, "select string_agg(measure_key || '=' || cast(value as bigint), ' ') from g "
                                  "where geography_key = 'PL' and is_total"),
            "PL_by_breakdown": one(con, "select string_agg(block_id || ' ' || breakdown_json || coalesce(' ' || "
                                        "breakdown_label_json, '') || '=' || cast(value as bigint), ' | ' order by "
                                        "block_id, value desc) from g where geography_key = 'PL' and not is_total"),
            "voiv_totals_sum": one(con, "select string_agg(block_id || ' ' || n || ' areas, sum ' || s, '; ') from "
                                        "(select block_id, count(*) n, cast(sum(value) as bigint) s from g where "
                                        "geography_key like 'TERC:%' and is_total group by 1)"),
            "mazowieckie": one(con, "select string_agg(block_id || '=' || cast(value as bigint), ' ') from g where "
                                    "geography_key = 'TERC:1400000' and is_total"),
        }
        print("SPOT", json.dumps(spot, ensure_ascii=False), flush=True)
        annotate("notice", "radon trial Poland", spot)
        seeds = {n: list(csv.DictReader((W / f"seeds/{n}.csv").open(encoding="utf-8"))) for n in ("source", "licence")}
        src = next(r for r in seeds["source"] if r["source_key"] == "radon")
        lic = next((r for r in seeds["licence"] if r["licence_key"] == src["licence_key"]), None)
        annotate("notice", "radon trial licence row", {"source": src, "licence": lic})
        show(con, "gold sample", "select block_id, geography_key, breakdown_json, period_label, value from g "
                                 "order by block_id, geography_key, breakdown_json limit 40")
        show(con, "silver inst sample", "select institution_uuid, kind_code, status_code, status_name, voivodeship, "
                                        "start_date, liquidation_date from s_inst limit 10")
    finally:
        removed = 0
        for bucket in (lbucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
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
        annotate("notice", "radon trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
