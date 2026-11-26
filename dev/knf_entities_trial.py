"""Dev trial of knf_entities (PLG-016, #2994) on the final code: Landing and Bronze into zz_dev_knf_entities/ (both
buckets) and namespace zz_dev_knf_entities, each twice; Bronze profile; then the branch's Silver SQL and its tests
(unique, not_null, accepted_values from the YAML, the custom test), the Gold entity table and build stage with their
YAML tests, duplicates by series_key x date_key and by entity key, spot records, the LEI join to production GLEIF
Silver (read only), applied in memory with DuckDB (Jinja-rendered). Everything written is removed at the end
(finally). Deleted before merge."""

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
from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.knf_entities import load as bronze
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "knf_entities"
SCRATCH = "zz_dev_knf_entities"
LAND = f"{SCRATCH}/01_landing"
ANNOTATE = report.annotate


def quiet(level, title, payload=None, *args, **kwargs):
    print(f"ANNOTATION {level} | {title} | {json.dumps(payload, default=str, ensure_ascii=False)[:3000]}", flush=True)


def render(path: Path, tables: dict[str, str]) -> str:
    text = re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def landed_state(client, bucket) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/{SOURCE}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def seed(name: str) -> list[dict]:
    return list(csv.DictReader(open(ROOT / f"warehouse/seed_rows/{name}/{SOURCE}.csv", encoding="utf-8")))


def yaml_tests(con, n, path: Path, tables: dict[str, str]) -> dict[str, int]:
    failures = {}
    for spec in yaml.safe_load(path.read_text())["models"]:
        table = tables[spec["name"]]
        for test in spec.get("tests", []):
            if "unique" in test:
                col = test["unique"]["column_name"]
                failures[f"{spec['name']} unique"] = n(
                    f"select count(*) from (select {col} k from {table} group by k having count(*) > 1)"
                )
        for column in spec.get("columns", []):
            for test in column.get("tests", []):
                key = f"{spec['name']}.{column['name']} {test if isinstance(test, str) else next(iter(test))}"
                if test == "not_null":
                    failures[key] = n(f"select count(*) from {table} where {column['name']} is null")
                elif test == "unique":
                    failures[key] = n(
                        f"select count(*) from (select {column['name']} from {table} group by 1 having count(*) > 1)"
                    )
                elif isinstance(test, dict) and "accepted_values" in test:
                    values = test["accepted_values"]["arguments"]["values"]
                    con.execute("create or replace temp table allowed as select unnest(?) as v", [[str(v) for v in values]])
                    failures[key] = n(
                        f"select count(*) from {table} where cast({column['name']} as varchar) not in (select v from allowed)"
                    )
    return failures


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = quiet
    ok = True
    try:
        # ---- Landing twice: the second run must write nothing
        states, codes = [], []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            codes.append(bulk_file.main(SOURCE, "load", landing_root=LAND))
            states.append(landed_state(client, land_bucket))
        inspect = bulk_file.main(SOURCE, "inspect", landing_root=LAND)
        verify = bulk_file.main(SOURCE, "verify", landing_root=LAND)
        data = [(k, s) for k, s in states[0] if not k.endswith(".json") or not k.endswith(".receipt.json")]
        data = [(k, s) for k, s in data if not k.endswith(".receipt.json")]
        landing = {"exit": codes, "files": len(data), "bytes": sum(s for _, s in data),
                   "keys": [k.rsplit("/", 2)[-2:] for k, _ in data], "second_run_unchanged": states[0] == states[1],
                   "inspect_exit": inspect, "verify_exit": verify}
        ok &= bool(landing["second_run_unchanged"] and not verify and not any(codes) and len(data) == 1)
        ANNOTATE("notice" if ok else "error", "knf_entities trial landing", landing)

        # ---- Bronze twice
        snaps = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            bronze.load(0, landing_root=LAND, namespace=SCRATCH)
            snap = cat.load_table((SCRATCH, "knf_entities_esma_entries")).current_snapshot()
            snaps.append(snap.snapshot_id if snap else None)
        rows = cat.load_table((SCRATCH, "knf_entities_esma_entries")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "knf_entities_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # the table exists only after a rejection
            rejected = []
        con.register("b_entries", rows)
        cols = [c for c, _ in bronze.COLUMNS if c not in ("entity", "raw_sha256", "source_version", "ingested_at",
                                                           "source_row_number")]
        empties = {c: n(f"select count(*) filter (where {c} is null) || '/' || count(*) filter (where trim({c}) = '') "
                        f"from b_entries") for c in cols}
        bronze_stats = {
            "rows": rows.num_rows,
            "files": n("select count(distinct raw_sha256) from b_entries"),
            "distinct_entry_id": n("select count(distinct entry_id) from b_entries"),
            "authorisation_range": n("select min(authorisation_date) || '..' || max(authorisation_date) from b_entries"),
            "last_update_range": n("select min(last_update) || '..' || max(last_update) from b_entries"),
            "null_or_empty_per_column": {c: v for c, v in empties.items() if v != "0/0"},
            "non_numeric_db_id": n("select count(*) from b_entries where not regexp_full_match(esma_db_id, '\\d+')"),
            "non_numeric_funds": n("select string_agg(distinct funds_count, '|') from b_entries "
                                   "where not regexp_full_match(funds_count, '\\d+')"),
            "rejected": [(r.get("entity"), (r.get("error") or r.get("reason") or "")[:100]) for r in rejected],
            "second_run_unchanged": snaps[0] == snaps[1],
        }
        ok &= bronze_stats["second_run_unchanged"] and not rejected and rows.num_rows > 0
        ANNOTATE("notice" if ok else "error", "knf_entities trial bronze", bronze_stats)

        # ---- Silver, tests, Gold
        tables = {
            "knf_entities_esma_entries": "s_entries",
            "knf_supervised_entity": "g_entity",
            "poc_build_knf_entities": "g_build",
            "poc_map_geography": "geo_map",
        }
        con.sql("create table geo_map as select 'knf_entities' as source_system, 'PL' as source_code, "
                "'PL' as geography_key")
        silver_sql = render(ROOT / "warehouse/models/silver/knf_entities/knf_entities_esma_entries.sql",
                            {**tables, "knf_entities_esma_entries": "b_entries"})
        con.sql("create table s_entries as " + silver_sql)
        con.sql("create table s_entries2 as " + silver_sql)
        con.sql("create table g_entity as " + render(ROOT / "warehouse/models/gold/knf_supervised_entity.sql", tables))
        con.sql("create table g_build as " + render(ROOT / "warehouse/models/gold/poc_build_knf_entities.sql", tables))
        failures = yaml_tests(con, n, ROOT / "warehouse/models/silver/knf_entities/_knf_entities.yml", tables)
        failures |= yaml_tests(con, n, ROOT / "warehouse/models/gold/_knf_entities.yml", tables)
        failures["knf_entities_esma_entries_complete"] = n(
            "select count(*) from (" + render(ROOT / "warehouse/tests/knf_entities_esma_entries_complete.sql", tables) + ")"
        )
        silver = {
            "silver_rows": n("select count(*) from s_entries"),
            "rebuild_same": n("select count(*) from (select * from s_entries except select * from s_entries2)") == 0,
            "tests_run": len(failures),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
            "by_type": n("select string_agg(entity_type_code || '=' || c, ' ' order by c desc) from "
                         "(select entity_type_code, count(*) c from s_entries group by 1)"),
            "by_status": n("select string_agg(status || '=' || c, ' ') from (select status, count(*) c from s_entries group by 1)"),
            "by_office": n("select string_agg(office_type || '=' || c, ' ') from (select office_type, count(*) c from s_entries group by 1)"),
            "lei_filled": n("select count(lei) from s_entries"),
            "head_office_lei_filled": n("select count(head_office_lei) from s_entries"),
            "distinct_lei": n("select count(distinct lei) from s_entries"),
            "authorised_range": n("select min(authorised_on) || '..' || max(authorised_on) from s_entries"),
            "withdrawn_range": n("select min(withdrawn_on) || '..' || max(withdrawn_on) from s_entries"),
            "inactive_without_withdrawal": n("select count(*) from s_entries where status = 'Inactive' and withdrawn_on is null"),
            "active_with_withdrawal": n("select count(*) from s_entries where status = 'Active' and withdrawn_on is not null"),
            "licence_key": seed("source")[0]["licence_key"],
            "licence_row": seed("licence")[0]["licence_key"] == seed("source")[0]["licence_key"],
        }
        ok &= not any(failures.values()) and silver["rebuild_same"] and silver["licence_row"]
        ANNOTATE("notice" if ok else "error", "knf_entities trial silver", silver)

        gold = {
            "entity_rows": n("select count(*) from g_entity"),
            "entity_dup_keys": n("select count(*) from (select entry_id from g_entity group by 1 having count(*) > 1)"),
            "build_rows": n("select count(*) from g_build"),
            "build_by_block": n("select string_agg(block_id || ' ' || frequency || ' ' || min_p || '..' || max_p || ' n=' || c, '; ') "
                                "from (select block_id, frequency, min(period_label) min_p, max(period_label) max_p, count(*) c "
                                "from g_build group by 1, 2)"),
            "dup_series_date": n("select count(*) from (select 1 from g_build group by measure_key || '|' || geography_key "
                                 "|| '|' || breakdown_json, date_key having count(*) > 1)"),
            "frequencies_per_measure": n("select max(c) from (select count(distinct frequency) c from g_build group by measure_key)"),
            "null_or_zero_values": n("select count(*) from g_build where value is null or value = 0"),
            "unmapped_units": sorted({r[0] for r in con.sql("select distinct unit_code from g_build").fetchall()}
                                     - {r["unit_code"] for r in seed("unit_map")}),
            "unmatched_geography": n("select count(*) from g_build where geography_key = '_X'"),
            "totals_by_status": n("select string_agg(measure_codes || '=' || value, ' ' order by measure_codes) from g_build "
                                  "where block_id = 'register_entries' and is_total"),
            "type_sum_equals_total": n(
                "select bool_and(t.value = s.v) from g_build t join (select measure_key, sum(value) v from g_build "
                "where block_id = 'register_entries' and breakdown_json like '%\"office_type\":\"_T\"%' "
                "and breakdown_json not like '%\"entity_type\":\"_T\"%' group by 1) s using (measure_key) where t.is_total"),
            "authorisations_by_year_last5": n("select string_agg(period_label || '=' || value || ' ' || observation_status_key, ' ' "
                                              "order by period_label) from (select * from g_build where block_id = "
                                              "'authorisations' and is_total order by period_label desc limit 5)"),
            "withdrawals_total": n("select sum(value) from g_build where block_id = 'withdrawals' and is_total"),
            "authorisations_total": n("select sum(value) from g_build where block_id = 'authorisations' and is_total"),
        }
        ok &= not gold["entity_dup_keys"] and not gold["dup_series_date"] and gold["frequencies_per_measure"] == 1
        ok &= not gold["null_or_zero_values"] and not gold["unmapped_units"] and not gold["unmatched_geography"]
        ANNOTATE("notice" if ok else "error", "knf_entities trial gold", gold)

        spot = {
            w: con.sql(
                "select entry_id, entity_type_code, office_type, status, lei, head_office_lei, valid_from, valid_to "
                f"from g_entity where upper(entity_name) like '%{w}%' order by entry_id limit 4"
            ).fetchall()
            for w in ("PKO TOWARZYSTWO", "XTB", "IPOPEMA", "GIEŁDA PAPIERÓW", "BONDSPOT")
        }
        ANNOTATE("notice", "knf_entities trial spot records", {k: [list(map(str, r)) for r in v] for k, v in spot.items()})

        # ---- LEI join to production GLEIF Silver (read only)
        try:
            from pyiceberg.expressions import In

            leis = sorted({r[0] for r in con.sql("select lei from s_entries where lei is not null union "
                                                 "select head_office_lei from s_entries where head_office_lei is not null").fetchall()})
            found = cat.load_table(("silver", "gleif_legal_entities")).scan(
                row_filter=In("lei", leis), selected_fields=("lei", "legal_name", "legal_jurisdiction")
            ).to_arrow()
            con.register("gl", found)
            join = {"distinct_leis": len(leis), "found_in_gleif": n("select count(distinct lei) from gl"),
                    "jurisdictions": n("select string_agg(legal_jurisdiction || '=' || c, ' ') from "
                                       "(select legal_jurisdiction, count(*) c from gl group by 1)")}
        except Exception as exc:  # report, do not hide
            join = {"error": str(exc)[:300]}
        ANNOTATE("notice", "knf_entities trial LEI join to GLEIF", join)
        print("STATS", json.dumps({"landing": landing, "bronze": bronze_stats, "silver": silver, "gold": gold,
                                   "join": join}, default=str, ensure_ascii=False), flush=True)
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
        ANNOTATE("notice" if not left else "error", "knf_entities trial cleanup",
                 {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
